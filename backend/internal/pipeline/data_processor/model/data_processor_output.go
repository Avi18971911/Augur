package model

type DataProcessorOutput struct {
	SpanOrLogData []map[string]interface{}
	Error         error
}
