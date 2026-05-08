package kleio

// Signal types classify the kind of event captured.
const (
	SignalTypeWorkItem   = "work_item"
	SignalTypeCheckpoint = "checkpoint"
	SignalTypeDecision   = "decision"
	SignalTypeGitCommit  = "git_commit"
)

// Source types indicate how an event was ingested.
const (
	SourceTypeManual           = "manual"
	SourceTypeCLI              = "cli"
	SourceTypeAPI              = "api"
	SourceTypeAgent            = "agent"
	SourceTypeLocalGit         = "local_git"
	SourceTypeCursorTranscript = "cursor_transcript"
	SourceTypeCursorWatch      = "cursor_watch"
	// StatusSourceKleioReconcile tags machine reconciliation (e.g. needs_review).
	StatusSourceKleioReconcile = "kleio_reconcile"
)

// Link types describe the relationship between two entities.
const (
	LinkTypeRelatedTo      = "related_to"
	LinkTypeLedTo          = "led_to"
	LinkTypeImplements     = "implements"
	LinkTypeKeywordMatch   = "keyword_match"
	LinkTypeReferences     = "references"
	LinkTypeSquashContains = "squash_contains"
	LinkTypeTouches        = "touches"

	// Pipeline link types: introduced by the Ingest -> Correlate ->
	// Synthesize pipeline (see kleio.Cluster). LinkTypeClusterAnchor
	// connects every member of a cluster to its canonical anchor signal;
	// LinkTypeCorrelatedWith records weaker pairwise correlations
	// emitted by individual correlators (TimeWindow, IDReference, etc).
	// LinkTypeDerivedFrom flows from a synthesized Event back to its
	// source RawSignal IDs for round-trip provenance.
	// LinkTypeParentSignal is reserved for hierarchical Plan ingestion
	// (umbrella plan signal -> child todo/decision/risk signals).
	LinkTypeClusterAnchor  = "cluster_anchor"
	LinkTypeCorrelatedWith = "correlated_with"
	LinkTypeDerivedFrom    = "derived_from"
	LinkTypeParentSignal   = "parent_signal"

	// Work item hierarchy link types.
	LinkTypeParentOf   = "parent_of"  // work_item A is parent of work_item B
	LinkTypeSupersedes = "supersedes" // work_item A replaces work_item B (refinement/split/merge)
)

// Backlog item statuses.
const (
	StatusOpen       = "open"
	StatusInProgress = "in_progress"
	StatusDone       = "done"
	StatusIgnored    = "ignored"
)

// Backlog item categories.
const (
	CategoryTask       = "task"
	CategoryBug        = "bug"
	CategoryDebt       = "debt"
	CategoryFeatureGap = "feature_gap"
	CategoryTestGap    = "test_gap"
)

// Urgency levels.
const (
	UrgencyLow      = "low"
	UrgencyMedium   = "medium"
	UrgencyHigh     = "high"
	UrgencyCritical = "critical"
)

// Importance levels.
const (
	ImportanceLow      = "low"
	ImportanceMedium   = "medium"
	ImportanceHigh     = "high"
	ImportanceCritical = "critical"
)

// Identifier kinds classify extracted references.
const (
	IdentifierKindTicket    = "ticket"
	IdentifierKindPR        = "pr"
	IdentifierKindMilestone = "milestone"
	IdentifierKindTag       = "tag"
	IdentifierKindProject   = "project"
)

// Identifier providers indicate the external system.
const (
	ProviderJira    = "jira"
	ProviderGitHub  = "github"
	ProviderLinear  = "linear"
	ProviderGitTag  = "git_tag"
	ProviderKeyword = "keyword"
)

// Author types distinguish human-written from agent-generated content.
const (
	AuthorTypeHuman = "human"
	AuthorTypeAgent = "agent"
)

// Work-item status_authority tiers (relative integers; larger wins over smaller on ingest upsert).
// See openspec WI-UPS and design docs.
const (
	WorkItemStatusAuthorityInferred   = 0
	WorkItemStatusAuthorityPlan       = 10
	WorkItemStatusAuthorityCheckpoint = 20
	WorkItemStatusAuthorityExternal   = 30
	WorkItemStatusAuthorityHuman      = 40
)

// Capture modes distinguish explicit user/agent actions from system-inferred
// or system-generated signals.
const (
	CaptureModeExplicit    = "explicit"    // deliberate tool invocation
	CaptureModeDerived     = "derived"     // inferred from source material (transcript mining, plan parsing)
	CaptureModeSynthesized = "synthesized" // LLM/pipeline-generated (summaries, dedup merges)
)

// File change types.
const (
	ChangeTypeAdded    = "added"
	ChangeTypeModified = "modified"
	ChangeTypeDeleted  = "deleted"
	ChangeTypeRenamed  = "renamed"
)
