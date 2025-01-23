package model

import "github.com/Avi18971911/Augur/internal/db/elasticsearch/client"

type BulkClusterQueryResult struct {
	MissesInput            []IncreaseMissesInput
	TotalCountMetaMap      []client.MetaMap
	TotalCountDocumentMap  []client.DocumentMap
	WindowCountMetaMap     []client.MetaMap
	WindowCountDocumentMap []client.DocumentMap
}
