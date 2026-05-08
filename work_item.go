package kleio

// WorkItem is a tracked piece of actionable work: bugs, tasks, tech debt,
// feature gaps, or test gaps. It sits in Layer 3 of the signal model --
// downstream of events (Layer 2). Source metadata (source_type, source_id,
// file_path, etc.) lives on the source event(s), reachable via derived_from
// links in the links table.
type WorkItem struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Body        string `json:"body,omitempty"`
	Summary     string `json:"summary,omitempty"`
	Status      string `json:"status"`
	Category    string `json:"category"`
	Urgency     string `json:"urgency"`
	Importance  string `json:"importance"`
	RepoName    string `json:"repo_name,omitempty"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	Synced      *bool  `json:"synced,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
}

// WorkItemFilter constrains which work items are returned by
// Store.ListWorkItems.
type WorkItemFilter struct {
	Status      string `json:"status,omitempty"`
	Category    string `json:"category,omitempty"`
	Urgency     string `json:"urgency,omitempty"`
	Importance  string `json:"importance,omitempty"`
	RepoName    string `json:"repo_name,omitempty"`
	Search      string `json:"search,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}
