package service

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/inference/model"
)

type AnalyticsQueryService interface {
	GetChainOfEvents(
		ctx context.Context,
		input model.LogOrSpanData,
	) (mostLikelySequence map[string]*model.ClusterNode, err error)
	GetSpanOrLogData(ctx context.Context, id string) (model.LogOrSpanData, error)
}

type AnalyticsQueryServiceImpl struct {
}
