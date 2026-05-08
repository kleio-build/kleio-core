package kleio

import (
	"context"
	"time"
)

// RawSignal is the unit of input flowing through the ingestion pipeline.
// Every Ingester (plans, transcripts, git, future Slack/Linear) emits
// RawSignals, preserving enough provenance that any downstream Event can
// be traced back to its originating row, line, or file.
//
// RawSignal intentionally has no ID field: stable identity is the
// responsibility of the caller (typically a hash of SourceType + SourceID +
// SourceOffset). This keeps RawSignals cheap to construct and side-effect
// free during ingestion.
type RawSignal struct {
	// SourceType matches a SourceType* constant when the signal maps to an
	// existing source (e.g. SourceTypeCursorTranscript) or a custom string
	// for new pipeline-only sources (e.g. "cursor_plan", "slack_message").
	SourceType string `json:"source_type"`

	// SourceID uniquely identifies the originating artifact within its
	// source: a transcript UUID, a plan filename, a commit SHA, etc.
	SourceID string `json:"source_id"`

	// SourceOffset locates the signal inside the artifact: a line range,
	// a byte offset, a todo id, or any source-specific anchor.
	SourceOffset string `json:"source_offset,omitempty"`

	// Content is the literal text payload (already sanitized of credentials
	// by the time the signal reaches a correlator).
	Content string `json:"content"`

	// Kind hints at downstream signal_type (work_item, decision, checkpoint,
	// or "" for raw observations that the synthesizer will classify).
	Kind string `json:"kind,omitempty"`

	// Timestamp is the wall-clock moment the underlying artifact was
	// created or modified, used by TimeWindowCorrelator and ordering.
	Timestamp time.Time `json:"timestamp"`

	// Author is a free-form attribution (commit author email, transcript
	// "user", "assistant", "tool") used for filtering and display only.
	Author string `json:"author,omitempty"`

	// RepoName is the canonical kleio repo identifier when the signal can
	// be confidently attributed to one. Cluster synthesis trusts this
	// field when emitting Events into the captures table.
	RepoName string `json:"repo_name,omitempty"`

	// Metadata carries source-specific freeform data (file paths touched,
	// PR refs, plan section type). Correlators agree on string keys
	// per-source; no strict schema is enforced here.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// ClusterLink describes one edge inside a Cluster: a directional
// relationship between two RawSignals discovered by a Correlator. These
// edges are persisted as Link rows once the Cluster is committed.
type ClusterLink struct {
	From       string  `json:"from"` // RawSignal.SourceID
	To         string  `json:"to"`   // RawSignal.SourceID
	LinkType   string  `json:"link_type"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason,omitempty"`
}

// Cluster is a group of RawSignals deemed related by one or more
// correlators. It always has a single AnchorID identifying the canonical
// signal (typically a plan or umbrella signal); when no obvious anchor
// exists, the highest-confidence signal in the group serves as anchor.
//
// Persistence model: members are linked to AnchorID via Link rows whose
// LinkType is LinkTypeClusterAnchor (or a more specific type emitted by
// the contributing correlator). The cluster identity itself lives only
// in the link graph -- there is no separate "clusters" table -- which
// keeps Phase 1's schema delta to one new LinkType lookup row.
type Cluster struct {
	AnchorID   string        `json:"anchor_id"`
	AnchorType string        `json:"anchor_type"`
	Members    []RawSignal   `json:"members"`
	Links      []ClusterLink `json:"links,omitempty"`
	Confidence float64       `json:"confidence"`
	Provenance []string      `json:"provenance,omitempty"`
}

// IngestScope tells an Ingester what to load from its source. Ingesters
// are free to interpret fields they care about and ignore the rest; the
// shared shape lets Pipeline orchestrate scope across heterogeneous
// sources without per-source plumbing.
type IngestScope struct {
	// RepoName filters to a single canonical repo when set. Ignored when
	// AllRepos is true.
	RepoName string `json:"repo_name,omitempty"`

	// Workspace points at a multi-repo workspace identifier
	// (e.g. a .code-workspace file path) for ingesters that need it.
	Workspace string `json:"workspace,omitempty"`

	// Since is the incremental ingest watermark. Ingesters skip
	// artifacts older than this timestamp when set.
	Since time.Time `json:"since,omitempty"`

	// AllRepos overrides RepoName/Workspace and pulls from every
	// discoverable source on disk.
	AllRepos bool `json:"all_repos,omitempty"`

	// ExtraSelectors carries per-source overrides without bloating the
	// shared shape. Ingesters document the keys they accept.
	ExtraSelectors map[string]string `json:"extra_selectors,omitempty"`
}

// Ingester loads RawSignals from a single source (plans, transcripts,
// git commits, ...). Implementations are expected to be deterministic:
// re-running Ingest with the same scope must yield the same RawSignals
// in the same order so downstream correlation is reproducible.
type Ingester interface {
	Name() string
	Ingest(ctx context.Context, scope IngestScope) ([]RawSignal, error)
}

// Correlator inspects a slice of RawSignals and returns Clusters: groups
// of signals that should travel together through synthesis. Correlators
// run in the order registered on the Pipeline; later correlators see the
// flat signal list and may extend or override earlier clusters via
// ClusterLink edges.
type Correlator interface {
	Name() string
	Correlate(ctx context.Context, signals []RawSignal) ([]Cluster, error)
}

// Synthesizer reduces a single Cluster to zero or more Events suitable
// for persisting to the captures table. The returned Events should carry
// cluster_anchor_id and parent_signal_id in their StructuredData so the
// pipeline graph remains traversable after the fact (see plan
// "Schema posture" in report_quality_fixes_de202626.plan.md).
type Synthesizer interface {
	Name() string
	Synthesize(ctx context.Context, cluster Cluster) ([]Event, error)
}

// PipelineReport summarises a single Pipeline.Run: counts per stage, the
// number of Link rows created, total runtime, and any non-fatal errors.
// Returned to callers (CLI, app, smoke tests) for display and audit.
type PipelineReport struct {
	SignalsByIngester    map[string]int `json:"signals_by_ingester"`
	ClustersByCorrelator map[string]int `json:"clusters_by_correlator"`
	EventsBySynthesizer  map[string]int `json:"events_by_synthesizer"`
	LinksCreated         int            `json:"links_created"`
	Duration             time.Duration  `json:"duration"`
	Errors               []string       `json:"errors,omitempty"`
	// WorkItemIDsAffected lists work item IDs touched by deriveWorkItem / deriveUmbrellaWorkItem in this run.
	WorkItemIDsAffected []string `json:"work_item_ids_affected,omitempty"`

	wiAffectedSeen map[string]struct{} `json:"-"`
}

// Common StructuredData JSON keys agreed by all Synthesizers. Pipeline
// consumers (engine.Timeline, trace/explain/incident reports) look these
// up to reconstruct cluster membership and provenance after the fact.
const (
	StructuredKeyClusterAnchorID = "cluster_anchor_id"
	StructuredKeyParentSignalID  = "parent_signal_id"
	StructuredKeyProvenance      = "provenance"
	StructuredKeyPlanStatus      = "plan_status"
	StructuredKeySourceOffset    = "source_offset"
	StructuredKeyTodoID          = "todo_id"
	// StructuredKeyCompletionClaimed marks transcript (or other) structured
	// payloads that assert completion; stored as completion_claimed observations
	// with low trust — reconcile never promotes to done without an explicit gate.
	StructuredKeyCompletionClaimed = "completion_claimed"
)
