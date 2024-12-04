package model

type ClusterInputType string

const (
	LogClusterInputType  ClusterInputType = "log"
	SpanClusterInputType ClusterInputType = "span"
)

type ClusterInput interface {
	GetClusterId() string
	SetClusterId(clusterId string)
	GetType() ClusterInputType
	GetTextualData() string
	GetServiceName() string
	GetId() string
}
