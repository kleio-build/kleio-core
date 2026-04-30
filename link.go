package kleio

// Link represents a directional relationship between two entities (events,
// commits, identifiers, or files). It maps to the local SQLite links table.
type Link struct {
	ID         string  `json:"id"`
	SourceID   string  `json:"source_id"`
	TargetID   string  `json:"target_id"`
	LinkType   string  `json:"link_type"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason,omitempty"`
	CreatedAt  string  `json:"created_at"`
}

// LinkFilter constrains which links are returned by Store.QueryLinks.
type LinkFilter struct {
	SourceID string `json:"source_id,omitempty"`
	TargetID string `json:"target_id,omitempty"`
	LinkType string `json:"link_type,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}
