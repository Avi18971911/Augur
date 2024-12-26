package log_and_span

import (
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span/model"
	"go.uber.org/zap"
)

type ErrorSearchParams struct {
	Service   *string `json:"service,omitempty"`
	StartTime *string `json:"start_time,omitempty"`
	EndTime   *string `json:"end_time,omitempty"`
}

type LogAndSpanQueryService interface {
	GetAllErrors(errorSearchParams ErrorSearchParams) ([]model.LogAndSpan, error)
}

type LogAndSpanService struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewLogAndSpanService(ac client.AugurClient, logger *zap.Logger) *LogAndSpanService {
	return &LogAndSpanService{
		ac:     ac,
		logger: logger,
	}
}

func (lss *LogAndSpanService) GetAllErrors(errorSearchParams ErrorSearchParams) ([]model.LogAndSpan, error) {

}
