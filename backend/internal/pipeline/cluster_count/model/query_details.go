package model

import "github.com/Avi18971911/Augur/internal/db/elasticsearch/client"

type IncreaseMissesInput struct {
	ClusterId                 string
	CoClusterDetailsToExclude []string
}

type GetCountAndUpdateQueryDetails struct {
	IncreaseIncrementForMissesInput IncreaseMissesInput
	TotalCountMetaMapList           []client.MetaMap
	TotalCountDocumentMapList       []client.DocumentMap
	WindowCountMetaMapList          []client.MetaMap
	WindowCountDocumentMapList      []client.DocumentMap
}

type GetCountQueryDetails struct {
	TotalCountMetaMapList      []client.MetaMap
	TotalCountDocumentMapList  []client.DocumentMap
	WindowCountMetaMapList     []client.MetaMap
	WindowCountDocumentMapList []client.DocumentMap
}

type GetIncrementMissesQueryDetails struct {
	MetaMapList     []client.MetaMap
	DocumentMapList []client.DocumentMap
}
