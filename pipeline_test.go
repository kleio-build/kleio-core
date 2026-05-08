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
	mu        sync.Mutex
	events    []*Event
	links     []*Link
	workItems []*WorkItem
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

func (s *fakeStore) CreateWorkItem(_ context.Context, w *WorkItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workItems = append(s.workItems, w)
	return nil
}
func (s *fakeStore) ListWorkItems(context.Context, WorkItemFilter) ([]WorkItem, error) {
	return nil, nil
}
func (s *fakeStore) GetWorkItem(_ context.Context, id string) (*WorkItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workItems {
		if w.ID == id {
			return w, nil
		}
	}
	return nil, nil
}
func (s *fakeStore) UpdateWorkItem(context.Context, string, *WorkItem) error  { return nil }

func (s *fakeStore) CreateEntity(context.Context, *Entity) error                        { return nil }
func (s *fakeStore) FindEntity(context.Context, string, string) (*Entity, error)        { return nil, nil }
func (s *fakeStore) FindEntityByAlias(context.Context, string) (*Entity, error)         { return nil, nil }
func (s *fakeStore) ListEntities(context.Context, EntityFilter) ([]Entity, error)       { return nil, nil }
func (s *fakeStore) CreateEntityAlias(context.Context, *EntityAlias) error              { return nil }
func (s *fakeStore) CreateEntityMention(context.Context, *EntityMention) error          { return nil }
func (s *fakeStore) FindEntitiesByEvidence(context.Context, string) ([]Entity, error)   { return nil, nil }

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

// workItemSynthesizer emits work_item events for pipeline derivation testing.
type workItemSynthesizer struct{ name string }

func (s *workItemSynthesizer) Name() string { return s.name }
func (s *workItemSynthesizer) Synthesize(_ context.Context, cluster Cluster) ([]Event, error) {
	return []Event{{
		ID:         "evt-wi-" + cluster.AnchorID,
		SignalType: SignalTypeWorkItem,
		Content:    "Fix: " + cluster.AnchorID + "\nDetailed description here",
		SourceType: SourceTypeAgent,
		AuthorType: AuthorTypeAgent,
	}}, nil
}

// fakeStoreWithWorkItems tracks work items and their links
type fakeStoreWithWorkItems struct {
	fakeStore
	workItems []*WorkItem
}

func (s *fakeStoreWithWorkItems) CreateWorkItem(_ context.Context, w *WorkItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workItems = append(s.workItems, w)
	return nil
}

func (s *fakeStoreWithWorkItems) GetWorkItem(_ context.Context, id string) (*WorkItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workItems {
		if w.ID == id {
			return w, nil
		}
	}
	return nil, nil
}

func TestPipeline_DerivesWorkItemsFromWorkItemEvents(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}

	pipe := &Pipeline{
		Store: store,
		Ingesters: []Ingester{&fakeIngester{
			name: "test-ingest",
			signals: []RawSignal{
				{SourceType: "cursor_plan", SourceID: "plan-1", Content: "Fix the auth bug"},
			},
		}},
		Correlators:  []Correlator{&fakeCorrelator{name: "test-corr"}},
		Synthesizers: []Synthesizer{&workItemSynthesizer{name: "wi-synth"}},
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}
	if len(report.Errors) > 0 {
		t.Errorf("unexpected errors: %v", report.Errors)
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if len(store.workItems) != 1 {
		t.Fatalf("expected 1 work item, got %d", len(store.workItems))
	}

	wi := store.workItems[0]
	if wi.ID != "wi-evt-wi-plan-1" {
		t.Errorf("work item ID = %q, want %q", wi.ID, "wi-evt-wi-plan-1")
	}
	if wi.Title != "Fix: plan-1" {
		t.Errorf("work item title = %q, want %q", wi.Title, "Fix: plan-1")
	}
	if wi.Status != StatusOpen {
		t.Errorf("work item status = %q, want %q", wi.Status, StatusOpen)
	}

	// Check derived_from link was created
	hasDerivation := false
	for _, l := range store.links {
		if l.LinkType == LinkTypeDerivedFrom && l.TargetID == wi.ID {
			hasDerivation = true
		}
	}
	if !hasDerivation {
		t.Error("expected derived_from link from event to work item")
	}
}

func TestWorkItemLinkTypeConstants(t *testing.T) {
	if LinkTypeParentOf != "parent_of" {
		t.Errorf("LinkTypeParentOf = %q, want parent_of", LinkTypeParentOf)
	}
	if LinkTypeSupersedes != "supersedes" {
		t.Errorf("LinkTypeSupersedes = %q, want supersedes", LinkTypeSupersedes)
	}
}

func TestIsUmbrellaAnchor(t *testing.T) {
	tests := []struct {
		name string
		ev   *Event
		want bool
	}{
		{"anchor true", &Event{StructuredData: `{"is_anchor": true}`}, true},
		{"anchor false", &Event{StructuredData: `{"is_anchor": false}`}, false},
		{"no is_anchor", &Event{StructuredData: `{"foo": "bar"}`}, false},
		{"empty structured data", &Event{StructuredData: ""}, false},
		{"invalid json", &Event{StructuredData: "not json"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUmbrellaAnchor(tt.ev); got != tt.want {
				t.Errorf("isUmbrellaAnchor() = %v, want %v", got, tt.want)
			}
		})
	}
}

// umbrellaCheckpointSynthesizer emits a checkpoint event with is_anchor=true,
// plus a child work_item event referencing it.
type umbrellaCheckpointSynthesizer struct{ name string }

func (s *umbrellaCheckpointSynthesizer) Name() string { return s.name }
func (s *umbrellaCheckpointSynthesizer) Synthesize(_ context.Context, cluster Cluster) ([]Event, error) {
	umbrellaSD, _ := json.Marshal(map[string]any{
		StructuredKeyClusterAnchorID: cluster.AnchorID,
		StructuredKeyParentSignalID:  cluster.AnchorID,
		StructuredKeyProvenance:      "plan_cluster",
		"is_anchor":                  true,
	})
	childSD, _ := json.Marshal(map[string]any{
		StructuredKeyClusterAnchorID: cluster.AnchorID,
		StructuredKeyParentSignalID:  "evt-umbrella-" + cluster.AnchorID,
		StructuredKeyProvenance:      "plan_cluster",
	})
	return []Event{
		{
			ID:             "evt-umbrella-" + cluster.AnchorID,
			SignalType:     SignalTypeCheckpoint,
			Content:        "Umbrella: " + cluster.AnchorID,
			SourceType:     "cursor_plan",
			AuthorType:     AuthorTypeAgent,
			StructuredData: string(umbrellaSD),
		},
		{
			ID:             "evt-child-" + cluster.AnchorID,
			SignalType:     SignalTypeWorkItem,
			Content:        "Todo: child task for " + cluster.AnchorID,
			SourceType:     "cursor_plan",
			AuthorType:     AuthorTypeAgent,
			StructuredData: string(childSD),
		},
	}, nil
}

func TestPipeline_UmbrellaCheckpointCreatesWorkItem(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}

	pipe := &Pipeline{
		Store: store,
		Ingesters: []Ingester{&fakeIngester{
			name: "test-ingest",
			signals: []RawSignal{
				{SourceType: "cursor_plan", SourceID: "plan-umbrella", Content: "Plan with hierarchy"},
			},
		}},
		Correlators:  []Correlator{&fakeCorrelator{name: "test-corr"}},
		Synthesizers: []Synthesizer{&umbrellaCheckpointSynthesizer{name: "umbrella-synth"}},
	}

	report, err := pipe.Run(ctx, IngestScope{})
	if err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}
	if len(report.Errors) > 0 {
		t.Errorf("unexpected errors: %v", report.Errors)
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	// Should have 2 work items: one for umbrella, one for child
	if len(store.workItems) != 2 {
		t.Fatalf("expected 2 work items, got %d", len(store.workItems))
	}

	// Find the umbrella and child work items
	var umbrellaWI, childWI *WorkItem
	for _, wi := range store.workItems {
		if wi.Category == "plan" {
			umbrellaWI = wi
		} else {
			childWI = wi
		}
	}

	if umbrellaWI == nil {
		t.Fatal("expected umbrella work item with category 'plan'")
	}
	if childWI == nil {
		t.Fatal("expected child work item")
	}

	// Verify parent_of link: umbrella WI -> child WI
	hasParentLink := false
	for _, l := range store.links {
		if l.LinkType == LinkTypeParentOf &&
			l.SourceID == umbrellaWI.ID &&
			l.TargetID == childWI.ID {
			hasParentLink = true
		}
	}
	if !hasParentLink {
		t.Error("expected parent_of link from umbrella to child work item")
		t.Logf("umbrella WI ID: %s, child WI ID: %s", umbrellaWI.ID, childWI.ID)
		for _, l := range store.links {
			t.Logf("  link: %s -> %s [%s]", l.SourceID, l.TargetID, l.LinkType)
		}
	}
}
