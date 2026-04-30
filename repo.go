package kleio

// Repo represents a git repository tracked by the CLI for indexing. It maps to
// the local SQLite repos table and tracks incremental indexing state.
type Repo struct {
	Path           string `json:"path"`
	Name           string `json:"name"`
	LastIndexedSHA string `json:"last_indexed_sha,omitempty"`
	LastIndexedAt  string `json:"last_indexed_at,omitempty"`
	IsSquashHeavy  bool   `json:"is_squash_heavy"`
}
