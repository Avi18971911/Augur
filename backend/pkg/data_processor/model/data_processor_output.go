package model

import "context"

type DataProcessorOutput struct {
	ctx           context.Context
	SpanOrLogData []map[string]interface{}
}
