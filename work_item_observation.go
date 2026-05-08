package kleio

// Append-only observation about a work item (Layer 3 evidence), linked to a
// source event. Reconciliation policies read this stream to update
// work_items.status under WI-UPS / WI-HUM.
type WorkItemObservation struct {
	ID              string  `json:"id"`
	WorkItemID      string  `json:"work_item_id"`
	ObservationType string  `json:"observation_type"`
	ObservedValue   string  `json:"observed_value"`
	SourceType      string  `json:"source_type"`
	SourceEventID   string  `json:"source_event_id"`
	Confidence      float64 `json:"confidence"`
	ClaimKind       string  `json:"claim_kind,omitempty"`
	CreatedAt       string  `json:"created_at"`
}

// ObservationFilter constrains ListObservations.
type ObservationFilter struct {
	WorkItemID      string  `json:"work_item_id,omitempty"`
	ObservationType string  `json:"observation_type,omitempty"`
	SourceEventID   string  `json:"source_event_id,omitempty"`
	MinConfidence   float64 `json:"min_confidence,omitempty"`
	Limit           int     `json:"limit,omitempty"`
}

const (
	ObservationTypeStatusObserved    = "status_observed"
	ObservationTypeCompletionClaimed = "completion_claimed"
	ObservationTypeConflictFlagged   = "conflict_flagged"
)

// Observation confidence tiers for ingest (reconcile uses source_type + confidence).
const (
	ObservationConfidencePlanHigh     = 0.95
	ObservationConfidencePlanDefault  = 0.9
	ObservationConfidenceTranscript   = 0.2
	ObservationHighTrustThreshold     = 0.85
	ObservationConflictSourceEventKey = "kleio:synthetic:conflict"
)

// WorkItemStatusReconcileInput applies machine reconciliation updates (not
// human CLI edits). Implementations must respect WI-UPS authority comparison.
type WorkItemStatusReconcileInput struct {
	Status                 string
	StatusAuthority        int
	StatusSource           string
	StatusSourceEventID    string
	SourceStatus           string
	SourceStatusObservedAt string
}
