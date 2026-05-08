package kleio

import (
	"context"
	"testing"
)

// reconstructFakeStore implements Store with a tiny in-memory link
// table sufficient for the reconstruct tests.
type reconstructFakeStore struct {
	links []Link
}

func (f *reconstructFakeStore) QueryLinks(_ context.Context, filter LinkFilter) ([]Link, error) {
	var out []Link
	for _, l := range f.links {
		if filter.SourceID != "" && l.SourceID != filter.SourceID {
			continue
		}
		if filter.TargetID != "" && l.TargetID != filter.TargetID {
			continue
		}
		if filter.LinkType != "" && l.LinkType != filter.LinkType {
			continue
		}
		out = append(out, l)
		if filter.Limit > 0 && len(out) >= filter.Limit {
			break
		}
	}
	return out, nil
}
func (f *reconstructFakeStore) CreateEvent(context.Context, *Event) error            { return nil }
func (f *reconstructFakeStore) ListEvents(context.Context, EventFilter) ([]Event, error) {
	return nil, nil
}
func (f *reconstructFakeStore) GetEvent(context.Context, string) (*Event, error)        { return nil, nil }
func (f *reconstructFakeStore) CreateBacklogItem(context.Context, *BacklogItem) error   { return nil }
func (f *reconstructFakeStore) ListBacklogItems(context.Context, BacklogFilter) ([]BacklogItem, error) {
	return nil, nil
}
func (f *reconstructFakeStore) GetBacklogItem(context.Context, string) (*BacklogItem, error) {
	return nil, nil
}
func (f *reconstructFakeStore) UpdateBacklogItem(context.Context, string, *BacklogItem) error {
	return nil
}
func (f *reconstructFakeStore) CreateWorkItem(context.Context, *WorkItem) error                { return nil }
func (f *reconstructFakeStore) ListWorkItems(context.Context, WorkItemFilter) ([]WorkItem, error) { return nil, nil }
func (f *reconstructFakeStore) GetWorkItem(context.Context, string) (*WorkItem, error)         { return nil, nil }
func (f *reconstructFakeStore) UpdateWorkItem(context.Context, string, *WorkItem) error        { return nil }
func (f *reconstructFakeStore) UpdateWorkItemQuality(context.Context, string, float64, string) error {
	return nil
}
func (f *reconstructFakeStore) UpsertWorkItemLabel(context.Context, *WorkItemLabel) error { return nil }
func (f *reconstructFakeStore) ListWorkItemLabels(context.Context, string) ([]WorkItemLabel, error) {
	return nil, nil
}
func (f *reconstructFakeStore) DeleteWorkItemLabel(context.Context, string, string) error { return nil }
func (f *reconstructFakeStore) IndexCommits(context.Context, string, []Commit) error { return nil }
func (f *reconstructFakeStore) QueryCommits(context.Context, CommitFilter) ([]Commit, error) {
	return nil, nil
}
func (f *reconstructFakeStore) CreateLink(_ context.Context, l *Link) error {
	f.links = append(f.links, *l)
	return nil
}
func (f *reconstructFakeStore) TrackFileChange(context.Context, *FileChange) error { return nil }
func (f *reconstructFakeStore) FileHistory(context.Context, string) ([]FileChange, error) {
	return nil, nil
}
func (f *reconstructFakeStore) Search(context.Context, string, SearchOpts) ([]SearchResult, error) {
	return nil, nil
}
func (f *reconstructFakeStore) CreateEntity(context.Context, *Entity) error                        { return nil }
func (f *reconstructFakeStore) FindEntity(context.Context, string, string) (*Entity, error)        { return nil, nil }
func (f *reconstructFakeStore) FindEntityByAlias(context.Context, string) (*Entity, error)         { return nil, nil }
func (f *reconstructFakeStore) ListEntities(context.Context, EntityFilter) ([]Entity, error)       { return nil, nil }
func (f *reconstructFakeStore) CreateEntityAlias(context.Context, *EntityAlias) error              { return nil }
func (f *reconstructFakeStore) CreateEntityMention(context.Context, *EntityMention) error          { return nil }
func (f *reconstructFakeStore) FindEntitiesByEvidence(context.Context, string) ([]Entity, error)   { return nil, nil }
func (f *reconstructFakeStore) Mode() StoreMode { return StoreModeLocal }
func (f *reconstructFakeStore) Close() error    { return nil }

func TestReconstructCluster_HappyPath(t *testing.T) {
	store := &reconstructFakeStore{links: []Link{
		{SourceID: "a", TargetID: "anchor", LinkType: LinkTypeClusterAnchor},
		{SourceID: "b", TargetID: "anchor", LinkType: LinkTypeClusterAnchor},
		{SourceID: "c", TargetID: "anchor", LinkType: LinkTypeClusterAnchor},
	}}
	m, err := ReconstructCluster(context.Background(), store, "anchor")
	if err != nil {
		t.Fatal(err)
	}
	if m.AnchorID != "anchor" {
		t.Errorf("anchor=%q want anchor", m.AnchorID)
	}
	want := []string{"a", "b", "c"}
	if len(m.MemberIDs) != 3 {
		t.Fatalf("members=%v want 3", m.MemberIDs)
	}
	for i, id := range want {
		if m.MemberIDs[i] != id {
			t.Errorf("members[%d]=%q want %q", i, m.MemberIDs[i], id)
		}
	}
}

func TestReconstructCluster_EmptyAnchor(t *testing.T) {
	store := &reconstructFakeStore{}
	m, err := ReconstructCluster(context.Background(), store, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.MemberIDs) != 0 {
		t.Errorf("want 0 members for empty anchor, got %d", len(m.MemberIDs))
	}
}

func TestReconstructCluster_IgnoresOtherLinkTypes(t *testing.T) {
	store := &reconstructFakeStore{links: []Link{
		{SourceID: "a", TargetID: "anchor", LinkType: LinkTypeClusterAnchor},
		{SourceID: "b", TargetID: "anchor", LinkType: LinkTypeReferences}, // NOT cluster anchor
	}}
	m, _ := ReconstructCluster(context.Background(), store, "anchor")
	if len(m.MemberIDs) != 1 {
		t.Errorf("want 1 member (cluster anchor only), got %d", len(m.MemberIDs))
	}
}

func TestListClusterAnchors_DistinctTargets(t *testing.T) {
	store := &reconstructFakeStore{links: []Link{
		{SourceID: "a", TargetID: "anchorA", LinkType: LinkTypeClusterAnchor},
		{SourceID: "b", TargetID: "anchorA", LinkType: LinkTypeClusterAnchor},
		{SourceID: "c", TargetID: "anchorB", LinkType: LinkTypeClusterAnchor},
	}}
	anchors, err := ListClusterAnchors(context.Background(), store, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(anchors) != 2 {
		t.Fatalf("want 2 anchors, got %d: %v", len(anchors), anchors)
	}
	if anchors[0] != "anchorA" || anchors[1] != "anchorB" {
		t.Errorf("anchors=%v want [anchorA anchorB]", anchors)
	}
}

func TestListClusterAnchors_RespectsLimit(t *testing.T) {
	store := &reconstructFakeStore{links: []Link{
		{SourceID: "a", TargetID: "anchorA", LinkType: LinkTypeClusterAnchor},
		{SourceID: "b", TargetID: "anchorB", LinkType: LinkTypeClusterAnchor},
		{SourceID: "c", TargetID: "anchorC", LinkType: LinkTypeClusterAnchor},
	}}
	anchors, _ := ListClusterAnchors(context.Background(), store, 1)
	if len(anchors) != 1 {
		t.Fatalf("want 1 anchor (limit), got %d", len(anchors))
	}
}
