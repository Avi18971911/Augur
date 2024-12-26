package model

type DataType string

const (
	Log     DataType = "log"
	Span    DataType = "span"
	Unknown DataType = "unknown"
)
