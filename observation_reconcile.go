package kleio

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// ReconcileWorkItemFromObservations applies deterministic policy from the
// observation stream to work_items.status (WI-UPS), skipping rows held at human
// authority (WI-HUM). Conflicting high-trust status_observed values set
// needs_review and emit conflict_flagged.
func ReconcileWorkItemFromObservations(ctx context.Context, store Store, workItemID string, vocab *WorkItemVocabulary) error {
	if store == nil || workItemID == "" {
		return nil
	}
	if vocab == nil {
		vocab = BuiltinWorkItemVocabulary()
	}
	wi, err := store.GetWorkItem(ctx, workItemID)
	if err != nil || wi == nil {
		return err
	}
	if wi.StatusAuthority >= WorkItemStatusAuthorityHuman {
		return nil
	}

	obs, err := store.ListObservations(ctx, ObservationFilter{WorkItemID: workItemID, Limit: 500})
	if err != nil {
		return err
	}

	if payload, conflict := highTrustStatusConflictPayload(obs, vocab); conflict {
		if wi.Status == WorkItemStatusNeedsReview {
			return nil
		}
		if err := store.CreateObservation(ctx, &WorkItemObservation{
			WorkItemID:      workItemID,
			ObservationType: ObservationTypeConflictFlagged,
			ObservedValue:   string(payload),
			SourceType:      StatusSourceKleioReconcile,
			SourceEventID:   ObservationConflictSourceEventKey,
			Confidence:      1,
			CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		}); err != nil {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339)
		return store.ApplyWorkItemStatusReconcile(ctx, workItemID, WorkItemStatusReconcileInput{
			Status:                 WorkItemStatusNeedsReview,
			StatusAuthority:        WorkItemStatusAuthorityCheckpoint,
			StatusSource:           StatusSourceKleioReconcile,
			StatusSourceEventID:    "",
			SourceStatus:           WorkItemStatusNeedsReview,
			SourceStatusObservedAt: now,
		})
	}

	win, winAuth := pickWinningStatusObservation(obs, vocab)
	if win == "" || win == wi.Status {
		return nil
	}
	if win == WorkItemStatusDone && !statusDoneAllowedFromObservations(obs) {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return store.ApplyWorkItemStatusReconcile(ctx, workItemID, WorkItemStatusReconcileInput{
		Status:                 win,
		StatusAuthority:        winAuth,
		StatusSource:           observationSourceForAuthority(winAuth),
		StatusSourceEventID:    "",
		SourceStatus:           win,
		SourceStatusObservedAt: now,
	})
}

// conflictStream is a single high-trust status_observed winner per source_type (for audit JSON).
type conflictStream struct {
	SourceType    string `json:"source_type"`
	Status        string `json:"normalized_status"`
	SourceEventID string `json:"source_event_id"`
}

// highTrustStatusConflictPayload returns a JSON payload for conflict_flagged when latest
// high-trust status_observed rows disagree across source_type values.
func highTrustStatusConflictPayload(obs []WorkItemObservation, vocab *WorkItemVocabulary) (payload []byte, conflict bool) {
	// One winning observation per source_type (newest created_at). Conflicts are
	// cross-stream disagreements (e.g. plan vs external), not plan YAML updates
	// that supersede an older status_observed row.
	bestBySource := make(map[string]WorkItemObservation)
	for _, o := range obs {
		if o.ObservationType != ObservationTypeStatusObserved {
			continue
		}
		if o.Confidence < ObservationHighTrustThreshold {
			continue
		}
		cur, ok := bestBySource[o.SourceType]
		if !ok || o.CreatedAt > cur.CreatedAt {
			oCopy := o
			bestBySource[o.SourceType] = oCopy
		}
	}
	statusSeen := make(map[string]struct{})
	var streams []conflictStream
	for st, o := range bestBySource {
		n, err := vocab.NormalizeStatus(strings.TrimSpace(o.ObservedValue))
		if err != nil {
			n = strings.TrimSpace(o.ObservedValue)
		}
		if n == "" {
			continue
		}
		statusSeen[n] = struct{}{}
		streams = append(streams, conflictStream{
			SourceType:    st,
			Status:        n,
			SourceEventID: o.SourceEventID,
		})
	}
	if len(statusSeen) < 2 {
		return nil, false
	}
	sort.Slice(streams, func(i, j int) bool {
		if streams[i].SourceType != streams[j].SourceType {
			return streams[i].SourceType < streams[j].SourceType
		}
		return streams[i].SourceEventID < streams[j].SourceEventID
	})
	body := map[string]any{
		"reason":  "conflicting_status_observed",
		"streams": streams,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, false
	}
	return b, true
}

func pickWinningStatusObservation(obs []WorkItemObservation, vocab *WorkItemVocabulary) (status string, authority int) {
	var candidates []WorkItemObservation
	for _, o := range obs {
		if o.ObservationType != ObservationTypeStatusObserved {
			continue
		}
		n, err := vocab.NormalizeStatus(strings.TrimSpace(o.ObservedValue))
		if err != nil || n == "" {
			continue
		}
		o2 := o
		o2.ObservedValue = n
		candidates = append(candidates, o2)
	}
	if len(candidates) == 0 {
		return "", 0
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		ti, tj := sourceTypeTrustRank(candidates[i].SourceType), sourceTypeTrustRank(candidates[j].SourceType)
		if ti != tj {
			return ti > tj
		}
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if candidates[i].CreatedAt != candidates[j].CreatedAt {
			return candidates[i].CreatedAt > candidates[j].CreatedAt
		}
		return candidates[i].SourceEventID > candidates[j].SourceEventID
	})
	win := candidates[0]
	auth := WorkItemStatusAuthorityInferred
	if win.SourceType == "cursor_plan" {
		auth = WorkItemStatusAuthorityPlan
	}
	return win.ObservedValue, auth
}

func sourceTypeTrustRank(sourceType string) int {
	switch sourceType {
	case "cursor_plan":
		return 30
	case SourceTypeAPI, "linear", "jira":
		return 25
	default:
		return 10
	}
}

// statusDoneAllowedFromObservations returns true only when a non–completion_claimed
// path supports done (plan/status_observed). Transcript completion_claimed never
// promotes to done in v1.
func statusDoneAllowedFromObservations(obs []WorkItemObservation) bool {
	for _, o := range obs {
		if o.ObservationType != ObservationTypeStatusObserved {
			continue
		}
		if strings.TrimSpace(o.ObservedValue) != WorkItemStatusDone {
			continue
		}
		if o.SourceType == "cursor_plan" && o.Confidence >= ObservationHighTrustThreshold {
			return true
		}
	}
	return false
}

func observationSourceForAuthority(auth int) string {
	switch auth {
	case WorkItemStatusAuthorityPlan:
		return "cursor_plan"
	case WorkItemStatusAuthorityCheckpoint:
		return StatusSourceKleioReconcile
	default:
		return "inferred"
	}
}
