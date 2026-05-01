package kleio

// Entity represents a canonical extracted concept: a ticket, file path,
// code symbol, feature name, actor, or decision. Entities accumulate
// confidence through repeated observation across signals.
type Entity struct {
	ID              string  `json:"id"`
	Kind            string  `json:"kind"`
	Label           string  `json:"label"`
	NormalizedLabel string  `json:"normalized_label"`
	RepoName        string  `json:"repo_name,omitempty"`
	FirstSeenAt     string  `json:"first_seen_at"`
	LastSeenAt      string  `json:"last_seen_at"`
	MentionCount    int     `json:"mention_count"`
	Confidence      float64 `json:"confidence"`
}

// EntityAlias maps a surface form to a canonical entity. Aliases can come
// from branch names, commit messages, plans, transcripts, or co-occurrence
// learning.
type EntityAlias struct {
	EntityID   string  `json:"entity_id"`
	Alias      string  `json:"alias"`
	Source     string  `json:"source"`
	Confidence float64 `json:"confidence"`
	CreatedAt  string  `json:"created_at"`
}

// EntityMention records that an entity was observed in a specific piece
// of evidence (event, commit, signal). The mention_context preserves a
// short surrounding text snippet for provenance display.
type EntityMention struct {
	ID           string  `json:"id"`
	EntityID     string  `json:"entity_id"`
	EvidenceType string  `json:"evidence_type"`
	EvidenceID   string  `json:"evidence_id"`
	Context      string  `json:"mention_context,omitempty"`
	Confidence   float64 `json:"confidence"`
	CreatedAt    string  `json:"created_at"`
}

// EntityFilter constrains entity queries.
type EntityFilter struct {
	Kind            string `json:"kind,omitempty"`
	NormalizedLabel string `json:"normalized_label,omitempty"`
	RepoName        string `json:"repo_name,omitempty"`
	Limit           int    `json:"limit,omitempty"`
}

// Entity kinds.
const (
	EntityKindTicket       = "ticket"
	EntityKindFile         = "file"
	EntityKindSymbol       = "symbol"
	EntityKindFeature      = "feature"
	EntityKindActor        = "actor"
	EntityKindDecisionName = "decision_name"
	EntityKindPlanAnchor   = "plan_anchor"
)

// EntityAlias sources.
const (
	AliasSourceBranch        = "branch"
	AliasSourceCommitMessage = "commit_message"
	AliasSourcePlan          = "plan"
	AliasSourceTranscript    = "transcript"
	AliasSourceCoOccurrence  = "co_occurrence"
)

// EntityMention evidence types.
const (
	EvidenceTypeEvent  = "event"
	EvidenceTypeCommit = "commit"
	EvidenceTypeSignal = "signal"
)
