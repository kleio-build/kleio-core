package kleio

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Pipeline orchestrates Ingest -> Correlate -> Synthesize -> Persist.
// Both kleio-cli and kleio-app are expected to instantiate Pipeline with
// the same set of Ingesters/Correlators/Synthesizers; the Store interface
// hides whether persistence happens against local SQLite or the cloud
// API. Concrete implementations of each stage live outside this package
// (e.g. kleio-cli/internal/ingest, /correlate, /synthesize).
// PostIngestFunc runs after all ingesters complete but before
// correlation. Used for entity normalization and persistence.
type PostIngestFunc func(ctx context.Context, signals []RawSignal) error

type Pipeline struct {
	Ingesters      []Ingester
	Correlators    []Correlator
	Synthesizers   []Synthesizer
	Store          Store
	PostIngestHooks []PostIngestFunc
}

// Run executes one full pipeline pass and returns a PipelineReport
// describing what was produced. Errors from any single ingester /
// correlator / synthesizer are recorded in the report rather than
// aborting the run, mirroring the CLI's "best-effort over per-source
// fragility" stance. A fatal error (e.g. Store write failure) is
// returned directly.
//
// Run intentionally takes no LLM provider: when a Synthesizer needs
// LLM enrichment it does so at construction time via a closure or the
// ai.AutoDetect helper. Keeping LLM integration out of the orchestrator
// preserves the "no friction without LLM" promise.
func (p *Pipeline) Run(ctx context.Context, scope IngestScope) (*PipelineReport, error) {
	start := time.Now()
	report := &PipelineReport{
		SignalsByIngester:    map[string]int{},
		ClustersByCorrelator: map[string]int{},
		EventsBySynthesizer:  map[string]int{},
	}

	var allSignals []RawSignal
	for _, ing := range p.Ingesters {
		signals, err := ing.Ingest(ctx, scope)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("ingest %s: %v", ing.Name(), err))
			continue
		}
		report.SignalsByIngester[ing.Name()] = len(signals)
		allSignals = append(allSignals, signals...)
	}

	for _, hook := range p.PostIngestHooks {
		if err := hook(ctx, allSignals); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("post-ingest hook: %v", err))
		}
	}

	var allClusters []Cluster
	for _, cor := range p.Correlators {
		clusters, err := cor.Correlate(ctx, allSignals)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("correlate %s: %v", cor.Name(), err))
			continue
		}
		report.ClustersByCorrelator[cor.Name()] = len(clusters)
		allClusters = append(allClusters, clusters...)
	}

	// A single RawSignal can land in multiple clusters (e.g. an
	// ID-reference cluster AND a time-window cluster). Each of those
	// clusters would be re-synthesized into the same Event, and the
	// store de-duplicates by ID via INSERT OR IGNORE. To make the
	// reported counts match what is actually persisted -- and to avoid
	// re-running CreateEvent unnecessarily -- we dedupe per pipeline
	// run by event ID before calling the store.
	seenEventID := map[string]bool{}

	for _, cluster := range allClusters {
		// Persist cluster membership as Link rows before synthesis so
		// downstream Synthesizers can reference the cluster graph
		// directly via Store.QueryLinks if they wish.
		links := persistCluster(ctx, p.Store, cluster)
		report.LinksCreated += links

		for _, syn := range p.Synthesizers {
			events, err := syn.Synthesize(ctx, cluster)
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("synthesize %s: %v", syn.Name(), err))
				continue
			}
			persisted := 0
			for i := range events {
				ev := &events[i]
				annotateEventWithCluster(ev, cluster, syn.Name())
				if ev.ID != "" && seenEventID[ev.ID] {
					continue
				}
				if err := p.Store.CreateEvent(ctx, ev); err != nil {
					report.Errors = append(report.Errors, fmt.Sprintf("create event from %s: %v", syn.Name(), err))
					continue
				}
				if ev.ID != "" {
					seenEventID[ev.ID] = true
				}
				persisted++
			}
			report.EventsBySynthesizer[syn.Name()] += persisted
		}
	}

	report.Duration = time.Since(start)
	return report, nil
}

// persistCluster writes one Link row per cluster member with link_type
// = LinkTypeClusterAnchor, plus one row per ClusterLink the correlators
// emitted. Returns the number of Link rows actually created.
func persistCluster(ctx context.Context, store Store, cluster Cluster) int {
	if cluster.AnchorID == "" {
		return 0
	}
	created := 0
	for _, m := range cluster.Members {
		if m.SourceID == cluster.AnchorID || m.SourceID == "" {
			continue
		}
		l := &Link{
			SourceID:   m.SourceID,
			TargetID:   cluster.AnchorID,
			LinkType:   LinkTypeClusterAnchor,
			Confidence: cluster.Confidence,
		}
		if err := store.CreateLink(ctx, l); err == nil {
			created++
		}
	}
	for _, edge := range cluster.Links {
		if edge.From == "" || edge.To == "" {
			continue
		}
		l := &Link{
			SourceID:   edge.From,
			TargetID:   edge.To,
			LinkType:   edge.LinkType,
			Confidence: edge.Confidence,
			Reason:     edge.Reason,
		}
		if err := store.CreateLink(ctx, l); err == nil {
			created++
		}
	}
	return created
}

// annotateEventWithCluster injects cluster_anchor_id, parent_signal_id,
// and provenance into ev.StructuredData so the Event remains traceable
// back to its originating Cluster. Existing keys in StructuredData are
// preserved (synthesizers may add their own metadata first).
func annotateEventWithCluster(ev *Event, cluster Cluster, synthName string) {
	sd := map[string]any{}
	if ev.StructuredData != "" {
		_ = json.Unmarshal([]byte(ev.StructuredData), &sd)
	}
	if cluster.AnchorID != "" {
		if _, ok := sd[StructuredKeyClusterAnchorID]; !ok {
			sd[StructuredKeyClusterAnchorID] = cluster.AnchorID
		}
	}
	if _, ok := sd[StructuredKeyProvenance]; !ok {
		sd[StructuredKeyProvenance] = synthName
	}
	if data, err := json.Marshal(sd); err == nil {
		ev.StructuredData = string(data)
	}
}
