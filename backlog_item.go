package kleio

// BacklogItem represents a tracked piece of work: bugs, tasks, tech debt,
// feature gaps, or test gaps. It maps to the local SQLite backlog_items table
// and to the cloud backlog API.
type BacklogItem struct {
	ID         string `json:"id"`
	ShortID    int    `json:"short_id,omitempty"`
	Title      string `json:"title"`
	Summary    string `json:"summary,omitempty"`
	Body       string `json:"body,omitempty"`
	Status     string `json:"status"`
	Category   string `json:"category"`
	Urgency    string `json:"urgency"`
	Importance string `json:"importance"`
	RepoName   string `json:"repo_name,omitempty"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	Synced     bool   `json:"synced"`
}

// BacklogFilter constrains which backlog items are returned by
// Store.ListBacklogItems.
type BacklogFilter struct {
	Status      string `json:"status,omitempty"`
	Category    string `json:"category,omitempty"`
	Urgency     string `json:"urgency,omitempty"`
	Importance  string `json:"importance,omitempty"`
	RepoName    string `json:"repo_name,omitempty"`
	Search      string `json:"search,omitempty"`
	Assignee    string `json:"assignee,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Limit       int    `json:"limit,omitempty"`
}
