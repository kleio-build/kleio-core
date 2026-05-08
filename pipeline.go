package kleio

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
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

// ProgressFunc receives stage-level progress messages during a pipeline
// run. Set Pipeline.OnProgress to a non-nil function to stream
// human-readable status updates (e.g. to stderr).
type ProgressFunc func(stage, detail string)

// BatchExecer is an optional interface implemented by stores that
// support wrapping multiple writes in a single transaction for
// throughput (e.g. localdb.Store.BatchExec).
type BatchExecer interface {
	BatchExec(ctx context.Context, fn func(ctx context.Context) error) error
}

type Pipeline struct {
	Ingesters       []Ingester
	Correlators     []Correlator
	Synthesizers    []Synthesizer
	Store           Store
	PostIngestHooks []PostIngestFunc
	OnProgress      ProgressFunc
	// WorkItemVocabulary supplies status normalization when deriving WorkItems from
	// events (plan todos, workspace aliases). Nil uses BuiltinWorkItemVocabulary().
	WorkItemVocabulary *WorkItemVocabulary
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
	progress := p.OnProgress
	if progress == nil {
		progress = func(string, string) {}
	}

	type ingestResult struct {
		name    string
		signals []RawSignal
		err     error
		elapsed time.Duration
	}

	progress("ingest", fmt.Sprintf("starting %d ingesters in parallel", len(p.Ingesters)))
	results := make([]ingestResult, len(p.Ingesters))
	var wg sync.WaitGroup
	for i, ing := range p.Ingesters {
		wg.Add(1)
		go func(idx int, ing Ingester) {
			defer wg.Done()
			t := time.Now()
			signals, err := ing.Ingest(ctx, scope)
			results[idx] = ingestResult{
				name:    ing.Name(),
				signals: signals,
				err:     err,
				elapsed: time.Since(t),
			}
		}(i, ing)
	}
	wg.Wait()

	var allSignals []RawSignal
	for _, r := range results {
		if r.err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("ingest %s: %v", r.name, r.err))
			progress("ingest", fmt.Sprintf("%s: error (%s)", r.name, r.elapsed.Truncate(time.Millisecond)))
			continue
		}
		report.SignalsByIngester[r.name] = len(r.signals)
		allSignals = append(allSignals, r.signals...)
		progress("ingest", fmt.Sprintf("%s: %d signals (%s)", r.name, len(r.signals), r.elapsed.Truncate(time.Millisecond)))
	}

	for i, hook := range p.PostIngestHooks {
		progress("hook", fmt.Sprintf("running post-ingest hook %d/%d", i+1, len(p.PostIngestHooks)))
		t := time.Now()
		if err := hook(ctx, allSignals); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("post-ingest hook: %v", err))
		}
		progress("hook", fmt.Sprintf("hook %d/%d done (%s)", i+1, len(p.PostIngestHooks), time.Since(t).Truncate(time.Millisecond)))
	}

	var allClusters []Cluster
	for _, cor := range p.Correlators {
		progress("correlate", fmt.Sprintf("starting %s over %d signals", cor.Name(), len(allSignals)))
		t := time.Now()
		clusters, err := cor.Correlate(ctx, allSignals)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("correlate %s: %v", cor.Name(), err))
			continue
		}
		report.ClustersByCorrelator[cor.Name()] = len(clusters)
		allClusters = append(allClusters, clusters...)
		progress("correlate", fmt.Sprintf("%s: %d clusters (%s)", cor.Name(), len(clusters), time.Since(t).Truncate(time.Millisecond)))
	}

	// A single RawSignal can land in multiple clusters (e.g. an
	// ID-reference cluster AND a time-window cluster). Each of those
	// clusters would be re-synthesized into the same Event, and the
	// store de-duplicates by ID via INSERT OR IGNORE. To make the
	// reported counts match what is actually persisted -- and to avoid
	// re-running CreateEvent unnecessarily -- we dedupe per pipeline
	// run by event ID before calling the store.
	seenEventID := map[string]bool{}
	seenWorkItemID := map[string]bool{}

	progress("synthesize", fmt.Sprintf("processing %d clusters", len(allClusters)))
	synthStart := time.Now()

	synthLoop := func(ctx context.Context) error {
		for ci, cluster := range allClusters {
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

					if ev.SignalType == SignalTypeWorkItem {
						vocab := p.resolveWorkItemVocab()
						deriveWorkItem(ctx, p.Store, ev, seenWorkItemID, report, vocab)
					} else if ev.SignalType == SignalTypeCheckpoint && isUmbrellaAnchor(ev) {
						vocab := p.resolveWorkItemVocab()
						deriveUmbrellaWorkItem(ctx, p.Store, ev, seenWorkItemID, report, vocab)
					}
				}
				report.EventsBySynthesizer[syn.Name()] += persisted
			}
			if (ci+1)%50 == 0 {
				progress("synthesize", fmt.Sprintf("  %d/%d clusters persisted", ci+1, len(allClusters)))
			}
		}
		return nil
	}

	if be, ok := p.Store.(BatchExecer); ok {
		if err := be.BatchExec(ctx, synthLoop); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("batch synth: %v", err))
		}
	} else {
		_ = synthLoop(ctx)
	}
	progress("synthesize", fmt.Sprintf("done (%s)", time.Since(synthStart).Truncate(time.Millisecond)))

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

// resolveWorkItemVocab returns workspace-configured normalization or builtins.
func (p *Pipeline) resolveWorkItemVocab() *WorkItemVocabulary {
	if p != nil && p.WorkItemVocabulary != nil {
		return p.WorkItemVocabulary
	}
	return BuiltinWorkItemVocabulary()
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

// deriveWorkItem creates a WorkItem from a work_item-typed event and links
// them via derived_from. The work item title is the first line of event
// content, capped at 120 chars.
func deriveWorkItem(ctx context.Context, store Store, ev *Event, seen map[string]bool, report *PipelineReport, vocab *WorkItemVocabulary) {
	wiID := "wi-" + ev.ID
	if seen[wiID] {
		return
	}

	title := ev.Content
	if idx := indexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}
	if len(title) > 120 {
		title = title[:120]
	}

	if vocab == nil {
		vocab = BuiltinWorkItemVocabulary()
	}
	status := deriveWorkItemStatus(ev, vocab)

	wi := &WorkItem{
		ID:          wiID,
		Title:       title,
		Body:        ev.Content,
		Status:      status,
		Category:    CategoryTask,
		Urgency:     UrgencyMedium,
		Importance:  ImportanceMedium,
		Granularity: deriveWorkItemGranularity(ev),
		RepoName:    ev.RepoName,
		CreatedAt:   ev.CreatedAt,
		UpdatedAt:   ev.CreatedAt,
	}
	attachIngestWorkItemAuthority(wi, ev)

	if err := store.CreateWorkItem(ctx, wi); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("derive work item: %v", err))
		return
	}
	seen[wiID] = true
	report.addWorkItemAffected(wiID)

	link := &Link{
		SourceID:   ev.ID,
		TargetID:   wiID,
		LinkType:   LinkTypeDerivedFrom,
		Confidence: 1.0,
		Reason:     "pipeline: work_item event -> WorkItem",
	}
	if err := store.CreateLink(ctx, link); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("link work item provenance: %v", err))
	}

	// parent_of: if this event has a parent_signal_id, link the parent's work
	// item to this one for hierarchy (only if the parent exists)
	if ev.StructuredData != "" {
		var sd map[string]any
		if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err == nil {
			if parentSignalID, ok := sd[StructuredKeyParentSignalID].(string); ok && parentSignalID != "" {
				parentWIID := "wi-" + parentSignalID
				if parentWI, _ := store.GetWorkItem(ctx, parentWIID); parentWI != nil {
					parentLink := &Link{
						SourceID:   parentWIID,
						TargetID:   wiID,
						LinkType:   LinkTypeParentOf,
						Confidence: 1.0,
						Reason:     "pipeline: umbrella -> child work item",
					}
					_ = store.CreateLink(ctx, parentLink)
				}
			}
		}
	}

	if err := RecomputeIntrinsicQuality(ctx, store, wiID); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("intrinsic quality %s: %v", wiID, err))
	}

	wiFresh, _ := store.GetWorkItem(ctx, wiID)
	if wiFresh != nil {
		wi = wiFresh
	}
	if err := EmitWorkItemObservationsFromDerive(ctx, store, wi, ev, vocab); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("emit work item observations %s: %v", wiID, err))
	}
	if err := ReconcileWorkItemFromObservations(ctx, store, wiID, vocab); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("reconcile work item observations %s: %v", wiID, err))
	}
}

// isUmbrellaAnchor returns true when the event's structured data marks
// it as a plan cluster anchor (umbrella checkpoint).
func isUmbrellaAnchor(ev *Event) bool {
	if ev.StructuredData == "" {
		return false
	}
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		return false
	}
	isAnchor, _ := sd["is_anchor"].(bool)
	return isAnchor
}

// deriveUmbrellaWorkItem creates a synthetic parent WorkItem for an
// umbrella checkpoint event so that child work items can reference it
// via parent_of links.
func deriveUmbrellaWorkItem(ctx context.Context, store Store, ev *Event, seen map[string]bool, report *PipelineReport, vocab *WorkItemVocabulary) {
	wiID := "wi-" + ev.ID
	if seen[wiID] {
		return
	}

	if vocab == nil {
		vocab = BuiltinWorkItemVocabulary()
	}

	title := ev.Content
	if idx := indexByte(title, '\n'); idx >= 0 {
		title = title[:idx]
	}
	if len(title) > 120 {
		title = title[:120]
	}

	wi := &WorkItem{
		ID:          wiID,
		Title:       title,
		Body:        ev.Content,
		Status:      WorkItemStatusOpen,
		Category:    "plan",
		Urgency:     "medium",
		Importance:  "medium",
		Granularity: WorkItemGranularityContainer,
		RepoName:    ev.RepoName,
		CreatedAt:   ev.CreatedAt,
		UpdatedAt:   ev.CreatedAt,
	}
	wi.StatusAuthority = WorkItemStatusAuthorityPlan
	wi.StatusSource = ev.SourceType
	wi.StatusSourceEventID = ev.ID
	wi.SourceStatusObservedAt = ev.CreatedAt

	if err := store.CreateWorkItem(ctx, wi); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("derive umbrella work item: %v", err))
		return
	}
	seen[wiID] = true
	report.addWorkItemAffected(wiID)

	link := &Link{
		SourceID:   ev.ID,
		TargetID:   wiID,
		LinkType:   LinkTypeDerivedFrom,
		Confidence: 1.0,
		Reason:     "pipeline: umbrella checkpoint -> plan WorkItem",
	}
	if err := store.CreateLink(ctx, link); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("link umbrella provenance: %v", err))
	}

	if err := RecomputeIntrinsicQuality(ctx, store, wiID); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("intrinsic quality %s: %v", wiID, err))
	}

	wiFresh, _ := store.GetWorkItem(ctx, wiID)
	if wiFresh != nil {
		wi = wiFresh
	}
	if err := EmitWorkItemObservationsFromDerive(ctx, store, wi, ev, vocab); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("emit umbrella observations %s: %v", wiID, err))
	}
	if err := ReconcileWorkItemFromObservations(ctx, store, wiID, vocab); err != nil {
		report.Errors = append(report.Errors, fmt.Sprintf("reconcile umbrella observations %s: %v", wiID, err))
	}
}

// isPlanFrontmatterTodoEvent is true for work items emitted from a plan file
// frontmatter todo (WI-PLN-001): cursor_plan source with todo_id or todo: source_offset.
func isPlanFrontmatterTodoEvent(ev *Event) bool {
	if ev == nil || ev.SignalType != SignalTypeWorkItem || ev.SourceType != "cursor_plan" {
		return false
	}
	if ev.StructuredData == "" {
		return false
	}
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		return false
	}
	if id, ok := sd[StructuredKeyTodoID].(string); ok && strings.TrimSpace(id) != "" {
		return true
	}
	off, ok := sd[StructuredKeySourceOffset].(string)
	return ok && strings.HasPrefix(off, "todo:")
}

func planTodoRawStatus(ev *Event) string {
	if ev == nil || ev.StructuredData == "" {
		return ""
	}
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		return ""
	}
	raw, _ := sd[StructuredKeyPlanStatus].(string)
	return raw
}

// deriveWorkItemGranularity applies WI-GRN defaults: umbrella is handled in
// deriveUmbrellaWorkItem; plan frontmatter todos are item, or subitem when
// parent_signal_id is set (nested under a plan umbrella).
func deriveWorkItemGranularity(ev *Event) string {
	if isPlanFrontmatterTodoEvent(ev) && eventParentSignalID(ev) != "" {
		return WorkItemGranularitySubitem
	}
	if isPlanFrontmatterTodoEvent(ev) {
		return WorkItemGranularityItem
	}
	return WorkItemGranularityItem
}

func eventParentSignalID(ev *Event) string {
	if ev == nil || ev.StructuredData == "" {
		return ""
	}
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		return ""
	}
	s, _ := sd[StructuredKeyParentSignalID].(string)
	return strings.TrimSpace(s)
}

// deriveWorkItemStatus applies plan todo normalization (WI-PLN-001/002) or defaults to open.
func deriveWorkItemStatus(ev *Event, vocab *WorkItemVocabulary) string {
	if !isPlanFrontmatterTodoEvent(ev) {
		return WorkItemStatusOpen
	}
	raw := strings.TrimSpace(planTodoRawStatus(ev))
	if raw == "" {
		return WorkItemStatusOpen
	}
	canonical, err := vocab.NormalizeStatus(raw)
	if err != nil {
		return WorkItemStatusOpen
	}
	return canonical
}

// attachIngestWorkItemAuthority stamps WI-UPS ingest provenance before CreateWorkItem upsert.
func attachIngestWorkItemAuthority(wi *WorkItem, ev *Event) {
	if wi == nil || ev == nil {
		return
	}
	switch ev.SourceType {
	case "cursor_plan":
		wi.StatusAuthority = WorkItemStatusAuthorityPlan
		wi.StatusSource = ev.SourceType
		wi.StatusSourceEventID = ev.ID
		if isPlanFrontmatterTodoEvent(ev) {
			wi.SourceStatus = strings.TrimSpace(planTodoRawStatus(ev))
		}
		wi.SourceStatusObservedAt = ev.CreatedAt
	default:
		wi.StatusAuthority = WorkItemStatusAuthorityInferred
		wi.StatusSource = ev.SourceType
		wi.StatusSourceEventID = ev.ID
		wi.SourceStatusObservedAt = ev.CreatedAt
	}
}

func (r *PipelineReport) addWorkItemAffected(id string) {
	if r == nil || id == "" {
		return
	}
	if r.wiAffectedSeen == nil {
		r.wiAffectedSeen = make(map[string]struct{})
	}
	if _, ok := r.wiAffectedSeen[id]; ok {
		return
	}
	r.wiAffectedSeen[id] = struct{}{}
	r.WorkItemIDsAffected = append(r.WorkItemIDsAffected, id)
}

// indexByte returns the index of the first instance of c in s, or -1.
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
