package kleio

import (
	"context"
	"encoding/json"
	"strings"
)

// EmitWorkItemObservationsFromDerive records append-only observations after a
// work item row is created/linked from an event. Idempotent on
// (work_item_id, source_event_id, observation_type).
func EmitWorkItemObservationsFromDerive(ctx context.Context, store Store, wi *WorkItem, ev *Event, vocab *WorkItemVocabulary) error {
	if store == nil || wi == nil || ev == nil {
		return nil
	}
	if vocab == nil {
		vocab = BuiltinWorkItemVocabulary()
	}
	conf := observationConfidence(ev)
	obs := &WorkItemObservation{
		WorkItemID:      wi.ID,
		ObservationType: ObservationTypeStatusObserved,
		ObservedValue:   wi.Status,
		SourceType:      ev.SourceType,
		SourceEventID:   ev.ID,
		Confidence:      conf,
		CreatedAt:       ev.CreatedAt,
	}
	if err := store.CreateObservation(ctx, obs); err != nil {
		return err
	}
	if !isPlanFrontmatterTodoEvent(ev) && eventCompletionClaimed(ev) {
		claim := &WorkItemObservation{
			WorkItemID:      wi.ID,
			ObservationType: ObservationTypeCompletionClaimed,
			ObservedValue:   WorkItemStatusDone,
			SourceType:      ev.SourceType,
			SourceEventID:   ev.ID,
			Confidence:      ObservationConfidenceTranscript,
			ClaimKind:       "transcript",
			CreatedAt:       ev.CreatedAt,
		}
		if err := store.CreateObservation(ctx, claim); err != nil {
			return err
		}
	}
	return nil
}

func eventCompletionClaimed(ev *Event) bool {
	if ev == nil || ev.StructuredData == "" {
		return false
	}
	var sd map[string]any
	if err := json.Unmarshal([]byte(ev.StructuredData), &sd); err != nil {
		return false
	}
	v, ok := sd[StructuredKeyCompletionClaimed]
	if !ok {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return strings.EqualFold(strings.TrimSpace(x), "true")
	default:
		return false
	}
}

func observationConfidence(ev *Event) float64 {
	if ev == nil {
		return ObservationConfidenceTranscript
	}
	if ev.SourceType == "cursor_plan" {
		if isPlanFrontmatterTodoEvent(ev) {
			return ObservationConfidencePlanHigh
		}
		return ObservationConfidencePlanDefault
	}
	if ev.SourceType == SourceTypeCursorTranscript || ev.SourceType == SourceTypeCursorWatch {
		return ObservationConfidenceTranscript
	}
	return 0.5
}
