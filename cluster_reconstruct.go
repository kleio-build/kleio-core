package kleio

import (
	"context"
	"sort"
)

// ClusterMembership is the round-tripped form of a Cluster: just the
// anchor ID plus the SourceIDs of every member. We deliberately avoid
// materialising RawSignals here because they live in source files, not
// the Store; the consumer is expected to look them up separately if
// they need full signal content.
type ClusterMembership struct {
	AnchorID  string   `json:"anchor_id"`
	MemberIDs []string `json:"member_ids"`
}

// ReconstructCluster returns the membership of the cluster anchored at
// anchorID by reading every link with target = anchorID and
// link_type = LinkTypeClusterAnchor. The returned MemberIDs list is
// sorted for determinism.
//
// This is the dual of Pipeline.persistCluster: it lets later commands
// (`kleio trace`, `kleio explain`, ...) walk the pipeline graph after
// the fact without re-running the entire ingest.
func ReconstructCluster(ctx context.Context, store Store, anchorID string) (*ClusterMembership, error) {
	if anchorID == "" {
		return &ClusterMembership{}, nil
	}
	links, err := store.QueryLinks(ctx, LinkFilter{
		TargetID: anchorID,
		LinkType: LinkTypeClusterAnchor,
	})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	out := &ClusterMembership{AnchorID: anchorID}
	for _, l := range links {
		if l.SourceID == "" || seen[l.SourceID] {
			continue
		}
		seen[l.SourceID] = true
		out.MemberIDs = append(out.MemberIDs, l.SourceID)
	}
	sort.Strings(out.MemberIDs)
	return out, nil
}

// ListClusterAnchors returns every distinct AnchorID in the link
// graph. Useful for surfaces like `kleio trace` that need to enumerate
// "every story Kleio knows about" without recomputing clusters.
//
// Implementation note: we don't have a dedicated "anchor" table, so we
// query the canonical LinkTypeClusterAnchor index and collect distinct
// targets. Limit caps the work; 0 means "all".
func ListClusterAnchors(ctx context.Context, store Store, limit int) ([]string, error) {
	links, err := store.QueryLinks(ctx, LinkFilter{
		LinkType: LinkTypeClusterAnchor,
		Limit:    limit,
	})
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, l := range links {
		if l.TargetID == "" || seen[l.TargetID] {
			continue
		}
		seen[l.TargetID] = true
		out = append(out, l.TargetID)
	}
	sort.Strings(out)
	return out, nil
}
