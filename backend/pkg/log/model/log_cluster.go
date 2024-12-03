package model

type LogClusterIdField map[string]interface{}

type LogClusterResult struct {
	Ids                   []string
	NewClusterIdDocuments []LogClusterIdField
}
