package model

import "github.com/Avi18971911/Augur/pkg/elasticsearch/client"

type GetCountAndUpdateOccurrencesQueryConstituentsResult struct {
	ClusterIds      []string
	MetaMapList     []client.MetaMap
	DocumentMapList []client.DocumentMap
}
