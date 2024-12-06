package model

type ClusterDataType string

const (
	LogClusterInputType  ClusterDataType = "log"
	SpanClusterInputType ClusterDataType = "span"
)

type ClusterInput struct {
	DataType    ClusterDataType
	TextualData string
	ServiceName string
	Id          string
	ClusterId   string
}
