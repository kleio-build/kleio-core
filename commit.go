package kleio

// Commit represents an indexed git commit. It maps to the local SQLite
// commits table.
type Commit struct {
	SHA          string `json:"sha"`
	RepoPath     string `json:"repo_path"`
	RepoName     string `json:"repo_name,omitempty"`
	Branch       string `json:"branch,omitempty"`
	AuthorName   string `json:"author_name,omitempty"`
	AuthorEmail  string `json:"author_email,omitempty"`
	Message      string `json:"message"`
	CommittedAt  string `json:"committed_at"`
	FilesChanged int    `json:"files_changed,omitempty"`
	Insertions   int    `json:"insertions,omitempty"`
	Deletions    int    `json:"deletions,omitempty"`
	IsMerge      bool   `json:"is_merge"`
	IndexedAt    string `json:"indexed_at"`
}

// CommitFilter constrains which commits are returned by Store.QueryCommits.
type CommitFilter struct {
	RepoPath      string `json:"repo_path,omitempty"`
	RepoName      string `json:"repo_name,omitempty"`
	Branch        string `json:"branch,omitempty"`
	AuthorName    string `json:"author_name,omitempty"`
	AuthorEmail   string `json:"author_email,omitempty"`
	MessageSearch string `json:"message_search,omitempty"`
	Since         string `json:"since,omitempty"`
	Until         string `json:"until,omitempty"`
	FilePath      string `json:"file_path,omitempty"`
	IsMerge       *bool  `json:"is_merge,omitempty"`
	Limit         int    `json:"limit,omitempty"`
}

// FileChange represents a single file modification within a commit. It maps
// to the local SQLite commit_files table.
type FileChange struct {
	CommitSHA  string `json:"commit_sha"`
	FilePath   string `json:"file_path"`
	ChangeType string `json:"change_type,omitempty"`
	OldPath    string `json:"old_path,omitempty"`
}
