package model

import "github.com/Avi18971911/Augur/pkg/elasticsearch/client"

type IncreaseMissesInput struct {
	ClusterId             string
	CoClusterIdsToExclude []string
}

type GetCountAndUpdateOccurrencesQueryConstituentsResult struct {
	IncreaseIncrementForMissesInput IncreaseMissesInput
	MetaMapList                     []client.MetaMap
	DocumentMapList                 []client.DocumentMap
}

type GetMetaAndDocumentInfoForIncrementMissesQueryResult struct {
	MetaMapList     []client.MetaMap
	DocumentMapList []client.DocumentMap
}
