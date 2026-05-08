package kleio

import "context"

// StoreMode indicates whether the store operates against a local SQLite
// database or the Kleio Cloud API.
type StoreMode int

const (
	StoreModeLocal StoreMode = iota
	StoreModeCloud
)

func (m StoreMode) String() string {
	switch m {
	case StoreModeLocal:
		return "local"
	case StoreModeCloud:
		return "cloud"
	default:
		return "unknown"
	}
}

// SearchOpts configures a Store.Search call.
type SearchOpts struct {
	RepoName   string `json:"repo_name,omitempty"`
	SignalType string `json:"signal_type,omitempty"`
	Since      string `json:"since,omitempty"`
	FilePath   string `json:"file_path,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// SearchResult is a single item returned by Store.Search, ranked by relevance.
type SearchResult struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"` // "event", "commit", "backlog_item", "identifier"
	Content    string  `json:"content"`
	Score      float64 `json:"score"`
	CreatedAt  string  `json:"created_at,omitempty"`
	RepoName   string  `json:"repo_name,omitempty"`
	FilePath   string  `json:"file_path,omitempty"`
	SignalType string  `json:"signal_type,omitempty"`
}

// Store is the core storage contract for Kleio. Both the local SQLite backend
// (localdb.Store) and the cloud HTTP backend (apistore.Store) implement this
// interface, ensuring commands and the analysis engine are written once.
type Store interface {
	// Events (captures, checkpoints, decisions)
	CreateEvent(ctx context.Context, event *Event) error
	ListEvents(ctx context.Context, filter EventFilter) ([]Event, error)
	GetEvent(ctx context.Context, id string) (*Event, error)

	// Backlog (deprecated -- use Work Items)
	CreateBacklogItem(ctx context.Context, item *BacklogItem) error
	ListBacklogItems(ctx context.Context, filter BacklogFilter) ([]BacklogItem, error)
	GetBacklogItem(ctx context.Context, id string) (*BacklogItem, error)
	UpdateBacklogItem(ctx context.Context, id string, update *BacklogItem) error

	// Work Items (Layer 3: actionable output derived from events)
	CreateWorkItem(ctx context.Context, item *WorkItem) error
	ListWorkItems(ctx context.Context, filter WorkItemFilter) ([]WorkItem, error)
	GetWorkItem(ctx context.Context, id string) (*WorkItem, error)
	UpdateWorkItem(ctx context.Context, id string, update *WorkItem) error
	// UpdateWorkItemQuality sets persisted intrinsic quality (does not touch status authority).
	UpdateWorkItemQuality(ctx context.Context, workItemID string, intrinsicScore float64, qualityReasonsJSON string) error
	UpsertWorkItemLabel(ctx context.Context, label *WorkItemLabel) error
	ListWorkItemLabels(ctx context.Context, workItemID string) ([]WorkItemLabel, error)
	DeleteWorkItemLabel(ctx context.Context, workItemID, labelText string) error

	// Work item observations (append-only evidence; local-first today).
	CreateObservation(ctx context.Context, o *WorkItemObservation) error
	ListObservations(ctx context.Context, filter ObservationFilter) ([]WorkItemObservation, error)
	// ApplyWorkItemStatusReconcile updates status + authority from observation
	// reconciliation (never stamps human tier — use UpdateWorkItem for that).
	ApplyWorkItemStatusReconcile(ctx context.Context, workItemID string, in WorkItemStatusReconcileInput) error

	// Git index
	IndexCommits(ctx context.Context, repoPath string, commits []Commit) error
	QueryCommits(ctx context.Context, filter CommitFilter) ([]Commit, error)

	// Links
	CreateLink(ctx context.Context, link *Link) error
	QueryLinks(ctx context.Context, filter LinkFilter) ([]Link, error)

	// File history
	TrackFileChange(ctx context.Context, change *FileChange) error
	FileHistory(ctx context.Context, path string) ([]FileChange, error)

	// Search (text-based for local, semantic for cloud)
	Search(ctx context.Context, query string, opts SearchOpts) ([]SearchResult, error)

	// Entities
	CreateEntity(ctx context.Context, entity *Entity) error
	FindEntity(ctx context.Context, kind, normalizedLabel string) (*Entity, error)
	FindEntityByAlias(ctx context.Context, alias string) (*Entity, error)
	ListEntities(ctx context.Context, filter EntityFilter) ([]Entity, error)
	CreateEntityAlias(ctx context.Context, alias *EntityAlias) error
	CreateEntityMention(ctx context.Context, mention *EntityMention) error
	FindEntitiesByEvidence(ctx context.Context, evidenceID string) ([]Entity, error)

	// Metadata
	Mode() StoreMode
	Close() error
}
