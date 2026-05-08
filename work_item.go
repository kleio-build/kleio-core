package kleio

// WorkItem is a tracked piece of actionable work: bugs, tasks, tech debt,
// feature gaps, or test gaps. It sits in Layer 3 of the signal model --
// downstream of events (Layer 2). Source metadata (source_type, source_id,
// file_path, etc.) lives on the source event(s), reachable via derived_from
// links in the links table.
//
// Authority fields steer ingest upserts: higher status_authority overwrites status
// and correlated metadata only when WI-UPS-003 permits.
type WorkItem struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Body       string `json:"body,omitempty"`
	Summary    string `json:"summary,omitempty"`
	Status     string `json:"status"`
	Category   string `json:"category"`
	Urgency    string `json:"urgency"`
	Importance string `json:"importance"`
	// Granularity is one of WorkItemGranularity* (container | item | subitem).
	Granularity            string `json:"granularity,omitempty"`
	StatusAuthority        int    `json:"status_authority,omitempty"`
	StatusSource           string `json:"status_source,omitempty"`
	StatusSourceEventID    string `json:"status_source_event_id,omitempty"`
	SourceStatus           string `json:"source_status,omitempty"`
	SourceStatusObservedAt string `json:"source_status_observed_at,omitempty"`
	RepoName               string `json:"repo_name,omitempty"`
	CreatedAt              string `json:"created_at"`
	UpdatedAt              string `json:"updated_at"`
	Synced                 *bool  `json:"synced,omitempty"`
	WorkspaceID            string `json:"workspace_id,omitempty"`
	// IntrinsicQualityScore is persisted 0–1 when the intrinsic-quality scorer has run (RFC-QLG).
	IntrinsicQualityScore *float64 `json:"intrinsic_quality_score,omitempty"`
	// QualityReasons is JSON (includes rule_version per RFC-QLG-002).
	QualityReasons string `json:"quality_reasons,omitempty"`
}

// WorkItemFilter constrains which work items are returned by
// Store.ListWorkItems.
type WorkItemFilter struct {
	Status string `json:"status,omitempty"`
	// Statuses filters by any of the given canonical status values in one query.
	// If non-empty, Status must be empty.
	Statuses   []string `json:"statuses,omitempty"`
	Category   string   `json:"category,omitempty"`
	Urgency    string   `json:"urgency,omitempty"`
	Importance string   `json:"importance,omitempty"`
	RepoName   string   `json:"repo_name,omitempty"`
	Search     string   `json:"search,omitempty"`
	// Granularity filters by exact canonical value when non-empty (container, item, subitem).
	Granularity string `json:"granularity,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	// MinIntrinsicQuality when > 0 excludes rows with NULL intrinsic score or score below threshold.
	MinIntrinsicQuality float64 `json:"min_intrinsic_quality,omitempty"`
}
