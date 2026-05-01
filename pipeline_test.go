package kleio

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeStore is a minimal in-memory Store implementation that records
// every CreateEvent / CreateLink call for assertion. It deliberately
// ignores everything not exercised by Pipeline.Run.
type fakeStore struct {
	mu     sync.Mutex
	events []*Event
	links  []*Link
}

func newFakeStore() *fakeStore { return &fakeStore{} }

func (s *fakeStore) Mode() StoreMode { return StoreModeLocal }
func (s *fakeStore) Close() error    { return nil }

func (s *fakeStore) CreateEvent(_ context.Context, e *Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.ID == "" {
		e.ID = "ev-" + e.SignalType + "-" + e.Content
	}
	s.events = append(s.events, e)
	return nil
}
func (s *fakeStore) ListEvents(context.Context, EventFilter) ([]Event, error) { return nil, nil }
func (s *fakeStore) GetEvent(context.Context, string) (*Event, error)         { return nil, nil }

func (s *fakeStore) CreateBacklogItem(context.Context, *BacklogItem) error { return nil }
func (s *fakeStore) ListBacklogItems(context.Context, BacklogFilter) ([]BacklogItem, error) {
	return nil, nil
}
func (s *fakeStore) GetBacklogItem(context.Context, string) (*BacklogItem, error)         { return nil, nil }
func (s *fakeStore) UpdateBacklogItem(context.Context, string, *BacklogItem) error        { return nil }
func (s *fakeStore) IndexCommits(context.Context, string, []Commit) error                  { return nil }
func (s *fakeStore) QueryCommits(context.Context, CommitFilter) ([]Commit, error)          { return nil, nil }

func (s *fakeStore) CreateLink(_ context.Context, l *Link) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.links = append(s.links, l)
	return nil
}
func (s *fakeStore) QueryLinks(context.Context, LinkFilter) ([]Link, error) { return nil, nil }

func (s *fakeStore) TrackFileChange(context.Context, *FileChange) error             { return nil }
func (s *fakeStore) FileHistory(context.Context, string) ([]FileChange, error)       { return nil, nil }
func (s *fakeStore) Search(context.Context, string, SearchOpts) ([]SearchResult, error) {
	return nil, nil
}

// fakeIngester emits the configured signals; if errFor is set, it errors instead.
type fakeIngester struct {
	name    string
	signals []RawSignal
	errFor  error
}

func (i *fakeIngester) Name() string { return i.name }
func (i *fakeIngester) Ingest(context.Context, IngestScope) ([]RawSignal, error) {
	if i.errFor != nil {
		return nil, i.errFor
	}
	return i.signals, nil
}

// fakeCorrelator groups all signals into one cluster anchored on the
// first signal. Useful for verifying Pipeline plumbing without committing
// to a particular correlation strategy.
type fakeCorrelator struct{ name string }

func (c *fakeCorrelator) Name() string { return c.name }
func (c *fakeCorrelator) Correlate(_ context.Context, signals []RawSignal) ([]Cluster, error) {
	if len(signals) == 0 {
		return nil, nil
	}
	return []Cluster{{
		AnchorID:   signals[0].SourceID,
		AnchorType: signals[0].SourceType,
		Members:    signals,
		Confidence: 0.9,
		Provenance: []string{c.name},
	}}, nil
}

// fakeSynthesizer emits one Event per cluster, copying anchor content.
type fakeSynthesizer struct{ name string }

func (s *fakeSynthesizer) Name() string { return s.name }
func (s *fakeSynthesizer) Synthesize(_ context.Context, cluster Cluster) ([]Event, error) {
	return []Event{{
		SignalType: SignalTypeCheckpoint,
		Content:    "from-" + cluster.AnchorID,
		SourceType: SourceTypeAgent,
	}}, nil
}

func TestPipeline_Run_HappyPath(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	signals := []RawSignal{
		{SourceType: "cursor_plan", SourceID: "p1", Content: "plan body", Timestamp: time.Now()},
		{SourceType: "cursor_transcript", SourceID: "t1", Content: "transcript line", Timestamp: time.Now()},
	}
	pipe := &Pipeline{
		Ingesters:    []Ingester{&fakeIngester{name: "fake-ing", signals: signals}},
		Correlators:  []Correlator{&fakeCorrelator{name: "fake-cor"}},
		Synthesizers: []Synthesizer{&fakeSynthesizer{name: "fake-syn"}},
		Store:        store,
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if got := report.SignalsByIngester["fake-ing"]; got != 2 {
		t.Errorf("SignalsByIngester[fake-ing] = %d, want 2", got)
	}
	if got := report.ClustersByCorrelator["fake-cor"]; got != 1 {
		t.Errorf("ClustersByCorrelator[fake-cor] = %d, want 1", got)
	}
	if got := report.EventsBySynthesizer["fake-syn"]; got != 1 {
		t.Errorf("EventsBySynthesizer[fake-syn] = %d, want 1", got)
	}
	if report.LinksCreated != 1 {
		t.Errorf("LinksCreated = %d, want 1 (one non-anchor member)", report.LinksCreated)
	}
	if len(report.Errors) != 0 {
		t.Errorf("expected no errors, got %v", report.Errors)
	}

	if len(store.events) != 1 {
		t.Fatalf("expected 1 persisted event, got %d", len(store.events))
	}
	ev := store.events[0]
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		t.Fatalf("event StructuredData not JSON: %v", err)
	}
	if sd[StructuredKeyClusterAnchorID] != "p1" {
		t.Errorf("cluster_anchor_id = %v, want p1", sd[StructuredKeyClusterAnchorID])
	}
	if sd[StructuredKeyProvenance] != "fake-syn" {
		t.Errorf("provenance = %v, want fake-syn", sd[StructuredKeyProvenance])
	}

	if len(store.links) != 1 {
		t.Fatalf("expected 1 cluster_anchor link, got %d", len(store.links))
	}
	if store.links[0].LinkType != LinkTypeClusterAnchor {
		t.Errorf("link type = %s, want %s", store.links[0].LinkType, LinkTypeClusterAnchor)
	}
	if store.links[0].SourceID != "t1" || store.links[0].TargetID != "p1" {
		t.Errorf("link edge = %s -> %s, want t1 -> p1",
			store.links[0].SourceID, store.links[0].TargetID)
	}
}

func TestPipeline_Run_RecordsIngesterErrorButContinues(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	pipe := &Pipeline{
		Ingesters: []Ingester{
			&fakeIngester{name: "broken", errFor: errors.New("disk full")},
			&fakeIngester{name: "ok", signals: []RawSignal{{SourceID: "s1", Content: "hi"}}},
		},
		Correlators:  []Correlator{&fakeCorrelator{name: "cor"}},
		Synthesizers: []Synthesizer{&fakeSynthesizer{name: "syn"}},
		Store:        store,
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("Run returned fatal error: %v", err)
	}
	if len(report.Errors) != 1 {
		t.Errorf("expected 1 recorded error, got %d: %v", len(report.Errors), report.Errors)
	}
	if report.SignalsByIngester["ok"] != 1 {
		t.Errorf("expected ok ingester to succeed, got %d signals", report.SignalsByIngester["ok"])
	}
	if report.EventsBySynthesizer["syn"] != 1 {
		t.Errorf("expected synthesis to fire on the ok signal, got %d events",
			report.EventsBySynthesizer["syn"])
	}
}

func TestPipeline_Run_NoSignalsProducesEmptyReport(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	pipe := &Pipeline{
		Ingesters:    []Ingester{&fakeIngester{name: "empty"}},
		Correlators:  []Correlator{&fakeCorrelator{name: "cor"}},
		Synthesizers: []Synthesizer{&fakeSynthesizer{name: "syn"}},
		Store:        store,
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if report.LinksCreated != 0 || len(store.events) != 0 {
		t.Errorf("expected zero links and events, got %d links, %d events",
			report.LinksCreated, len(store.events))
	}
}

// duplicatingCorrelator emits two clusters that contain the SAME anchor.
// This mirrors the production reality where ID-reference and time-window
// correlators both produce a cluster around the same plan signal.
type duplicatingCorrelator struct{ name string }

func (d *duplicatingCorrelator) Name() string { return d.name }
func (d *duplicatingCorrelator) Correlate(_ context.Context, signals []RawSignal) ([]Cluster, error) {
	if len(signals) == 0 {
		return nil, nil
	}
	c1 := Cluster{AnchorID: signals[0].SourceID, AnchorType: signals[0].SourceType, Members: signals}
	c2 := Cluster{AnchorID: signals[0].SourceID, AnchorType: signals[0].SourceType, Members: signals}
	return []Cluster{c1, c2}, nil
}

// fixedIDSynthesizer emits an Event whose ID is derived from the cluster
// anchor, mirroring how the real plan-cluster synthesizer derives stable
// IDs from the source signal.
type fixedIDSynthesizer struct{ name string }

func (s *fixedIDSynthesizer) Name() string { return s.name }
func (s *fixedIDSynthesizer) Synthesize(_ context.Context, c Cluster) ([]Event, error) {
	return []Event{{
		ID:         "ev-from-" + c.AnchorID,
		SignalType: SignalTypeCheckpoint,
		Content:    "from-" + c.AnchorID,
		SourceType: SourceTypeAgent,
	}}, nil
}

// TestPipeline_Run_DedupesEventsAcrossClusters protects against the
// regression where a single signal appearing in N clusters caused
// EventsBySynthesizer to over-count by N (and CreateEvent to be called
// N times even though INSERT OR IGNORE only persisted one).
func TestPipeline_Run_DedupesEventsAcrossClusters(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	pipe := &Pipeline{
		Ingesters: []Ingester{&fakeIngester{name: "ing", signals: []RawSignal{
			{SourceID: "anchor-1", SourceType: "cursor_plan"},
		}}},
		Correlators:  []Correlator{&duplicatingCorrelator{name: "dup"}},
		Synthesizers: []Synthesizer{&fixedIDSynthesizer{name: "syn"}},
		Store:        store,
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := report.EventsBySynthesizer["syn"]; got != 1 {
		t.Errorf("EventsBySynthesizer should dedupe by event ID, got %d, want 1", got)
	}
	if len(store.events) != 1 {
		t.Errorf("CreateEvent should fire once per unique ID, got %d calls", len(store.events))
	}
}

func TestNewLinkTypeConstantsArePresent(t *testing.T) {
	for _, lt := range []string{LinkTypeClusterAnchor, LinkTypeCorrelatedWith, LinkTypeDerivedFrom, LinkTypeParentSignal} {
		if lt == "" {
			t.Fatalf("expected non-empty LinkType constant")
		}
	}
}

func TestStructuredKeysExported(t *testing.T) {
	for _, k := range []string{StructuredKeyClusterAnchorID, StructuredKeyParentSignalID, StructuredKeyProvenance} {
		if k == "" {
			t.Fatalf("expected non-empty StructuredKey constant")
		}
	}
}
