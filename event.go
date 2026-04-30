package kleio

// Event is the unified representation of a captured signal: work items,
// checkpoints, decisions, or git-derived events. It maps to the local SQLite
// events table and to the cloud captures API.
type Event struct {
	ID              string `json:"id"`
	SignalType      string `json:"signal_type"`
	Content         string `json:"content"`
	SourceType      string `json:"source_type"`
	CreatedAt       string `json:"created_at"`
	RepoName        string `json:"repo_name,omitempty"`
	BranchName      string `json:"branch_name,omitempty"`
	FilePath        string `json:"file_path,omitempty"`
	FreeformContext string `json:"freeform_context,omitempty"`
	StructuredData  string `json:"structured_data,omitempty"`
	AuthorType      string `json:"author_type"`
	AuthorLabel     string `json:"author_label,omitempty"`
	Synced          bool   `json:"synced"`
}

// EventFilter constrains which events are returned by Store.ListEvents.
type EventFilter struct {
	SignalType   string `json:"signal_type,omitempty"`
	SourceType   string `json:"source_type,omitempty"`
	RepoName     string `json:"repo_name,omitempty"`
	AuthorType   string `json:"author_type,omitempty"`
	CreatedAfter string `json:"created_after,omitempty"`
	CreatedBefore string `json:"created_before,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}
