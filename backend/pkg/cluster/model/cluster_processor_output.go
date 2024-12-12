package model

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
)

type CountProcessorInput struct {
	ctx           context.Context
	clusterOutput []model.ClusterOutput
}
