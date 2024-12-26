package model

type ElasticsearchError struct {
	Failures []Failure `json:"failures"` // Details of each failure
}

type Failure struct {
	ID     string `json:"id"`     // Document ID
	Index  string `json:"index"`  // Index name
	Reason string `json:"reason"` // Failure reason
	Type   string `json:"type"`   // Type of error (e.g., version_conflict_engine_exception)
	Status int    `json:"status"` // HTTP status code
}
