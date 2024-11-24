package model

type MetaMap map[string]interface{}
type DocumentMap map[string]interface{}

type GetCountAndUpdateOccurrencesQueryConstituentsResult struct {
	ClusterIds      []string
	MetaMapList     []MetaMap
	DocumentMapList []DocumentMap
}
