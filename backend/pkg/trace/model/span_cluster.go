package model

type SpanClusterIdField map[string]interface{}

type SpanClusterResult struct {
	Ids                   []string
	NewClusterIdDocuments []SpanClusterIdField
}
