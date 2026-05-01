package kleio

// Signal types classify the kind of event captured.
const (
	SignalTypeWorkItem  = "work_item"
	SignalTypeCheckpoint = "checkpoint"
	SignalTypeDecision  = "decision"
	SignalTypeGitCommit = "git_commit"
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
	LinkTypeClusterAnchor   = "cluster_anchor"
	LinkTypeCorrelatedWith  = "correlated_with"
	LinkTypeDerivedFrom     = "derived_from"
	LinkTypeParentSignal    = "parent_signal"
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

// File change types.
const (
	ChangeTypeAdded    = "added"
	ChangeTypeModified = "modified"
	ChangeTypeDeleted  = "deleted"
	ChangeTypeRenamed  = "renamed"
)
