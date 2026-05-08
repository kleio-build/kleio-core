package kleio

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestReconcileWorkItemFromObservations_HumanAuthorityNoOp(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}
	w := &WorkItem{
		ID:              "wi-1",
		Title:           "t",
		Status:          WorkItemStatusOpen,
		StatusAuthority: WorkItemStatusAuthorityHuman,
		StatusSource:    AuthorTypeHuman,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateWorkItem(ctx, w)
	_ = store.CreateObservation(ctx, &WorkItemObservation{
		WorkItemID:      w.ID,
		ObservationType: ObservationTypeStatusObserved,
		ObservedValue:   WorkItemStatusDone,
		SourceType:      "cursor_plan",
		SourceEventID:   "ev-1",
		Confidence:      ObservationConfidencePlanHigh,
		CreatedAt:       w.CreatedAt,
	})
	vocab := BuiltinWorkItemVocabulary()
	if err := ReconcileWorkItemFromObservations(ctx, store, w.ID, vocab); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _ := store.GetWorkItem(ctx, w.ID)
	if got.Status != WorkItemStatusOpen {
		t.Fatalf("human-held WI should not move: got %q", got.Status)
	}
}

func TestReconcileWorkItemFromObservations_ConflictNeedsReview(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}
	w := &WorkItem{
		ID:              "wi-2",
		Title:           "t",
		Status:          WorkItemStatusOpen,
		StatusAuthority: WorkItemStatusAuthorityPlan,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateWorkItem(ctx, w)
	ts := w.CreatedAt
	_ = store.CreateObservation(ctx, &WorkItemObservation{
		WorkItemID:      w.ID,
		ObservationType: ObservationTypeStatusObserved,
		ObservedValue:   WorkItemStatusOpen,
		SourceType:      "cursor_plan",
		SourceEventID:   "ev-a",
		Confidence:      ObservationConfidencePlanHigh,
		CreatedAt:       ts,
	})
	_ = store.CreateObservation(ctx, &WorkItemObservation{
		WorkItemID:      w.ID,
		ObservationType: ObservationTypeStatusObserved,
		ObservedValue:   WorkItemStatusDone,
		SourceType:      SourceTypeAPI,
		SourceEventID:   "ev-b",
		Confidence:      ObservationConfidencePlanHigh,
		CreatedAt:       ts,
	})
	vocab := BuiltinWorkItemVocabulary()
	if err := ReconcileWorkItemFromObservations(ctx, store, w.ID, vocab); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _ := store.GetWorkItem(ctx, w.ID)
	if got.Status != WorkItemStatusNeedsReview {
		t.Fatalf("want needs_review, got %q", got.Status)
	}
	flags, _ := store.ListObservations(ctx, ObservationFilter{WorkItemID: w.ID, ObservationType: ObservationTypeConflictFlagged})
	if len(flags) != 1 {
		t.Fatalf("want 1 conflict_flagged, got %d", len(flags))
	}
	var body struct {
		Reason  string `json:"reason"`
		Streams []struct {
			SourceType    string `json:"source_type"`
			Status        string `json:"normalized_status"`
			SourceEventID string `json:"source_event_id"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(flags[0].ObservedValue), &body); err != nil {
		t.Fatalf("conflict payload JSON: %v", err)
	}
	if body.Reason != "conflicting_status_observed" {
		t.Fatalf("reason: got %q", body.Reason)
	}
	if len(body.Streams) != 2 {
		t.Fatalf("streams: want 2, got %d", len(body.Streams))
	}
	byType := map[string]string{}
	for _, s := range body.Streams {
		byType[s.SourceType] = s.Status
	}
	if byType["cursor_plan"] != WorkItemStatusOpen || byType[SourceTypeAPI] != WorkItemStatusDone {
		t.Fatalf("unexpected streams: %+v", body.Streams)
	}
}

func TestReconcileWorkItemFromObservations_PromotesPlanDone(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}
	w := &WorkItem{
		ID:              "wi-3",
		Title:           "t",
		Status:          WorkItemStatusOpen,
		StatusAuthority: WorkItemStatusAuthorityInferred,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateWorkItem(ctx, w)
	_ = store.CreateObservation(ctx, &WorkItemObservation{
		WorkItemID:      w.ID,
		ObservationType: ObservationTypeStatusObserved,
		ObservedValue:   WorkItemStatusDone,
		SourceType:      "cursor_plan",
		SourceEventID:   "ev-done",
		Confidence:      ObservationConfidencePlanHigh,
		CreatedAt:       w.CreatedAt,
	})
	vocab := BuiltinWorkItemVocabulary()
	if err := ReconcileWorkItemFromObservations(ctx, store, w.ID, vocab); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _ := store.GetWorkItem(ctx, w.ID)
	if got.Status != WorkItemStatusDone {
		t.Fatalf("want done, got %q", got.Status)
	}
}

func TestReconcileWorkItemFromObservations_TranscriptCompletionClaimDoesNotDone(t *testing.T) {
	ctx := context.Background()
	store := &fakeStoreWithWorkItems{}
	w := &WorkItem{
		ID:              "wi-4",
		Title:           "t",
		Status:          WorkItemStatusOpen,
		StatusAuthority: WorkItemStatusAuthorityInferred,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateWorkItem(ctx, w)
	_ = store.CreateObservation(ctx, &WorkItemObservation{
		WorkItemID:      w.ID,
		ObservationType: ObservationTypeCompletionClaimed,
		ObservedValue:   WorkItemStatusDone,
		SourceType:      SourceTypeCursorTranscript,
		SourceEventID:   "ev-tr",
		Confidence:      ObservationConfidenceTranscript,
		CreatedAt:       w.CreatedAt,
	})
	vocab := BuiltinWorkItemVocabulary()
	if err := ReconcileWorkItemFromObservations(ctx, store, w.ID, vocab); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _ := store.GetWorkItem(ctx, w.ID)
	if got.Status != WorkItemStatusOpen {
		t.Fatalf("completion_claimed alone must not set done: got %q", got.Status)
	}
}
