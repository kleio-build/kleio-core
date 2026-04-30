package kleio

// Identifier represents an extracted reference to an external entity: a ticket,
// pull request, milestone, tag, or project keyword. It maps to the local SQLite
// identifiers table.
type Identifier struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Value       string `json:"value"`
	Provider    string `json:"provider,omitempty"`
	URL         string `json:"url,omitempty"`
	FirstSeenAt string `json:"first_seen_at"`
}
