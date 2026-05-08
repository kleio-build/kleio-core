package kleio

// WorkItemLabel is a namespaced key:value tag on a work item (RFC-QLG-003).
type WorkItemLabel struct {
	ID         string  `json:"id"`
	WorkItemID string  `json:"work_item_id"`
	LabelText  string  `json:"label_text"` // full "key:value"
	Source     string  `json:"source"`
	Confidence float64 `json:"confidence"`
	CreatedAt  string  `json:"created_at"`
}
