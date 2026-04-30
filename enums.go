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
