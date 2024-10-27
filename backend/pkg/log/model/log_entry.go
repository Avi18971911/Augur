package model

import "time"

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Severity  Level     `json:"level"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
}

type Level string

const (
	InfoLevel  Level = "info"
	ErrorLevel Level = "error"
	DebugLevel Level = "debug"
	WarnLevel  Level = "warn"
)
