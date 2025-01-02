package log_and_span

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logHelper "github.com/Avi18971911/Augur/internal/otel_server/log/helper"
	spanHelper "github.com/Avi18971911/Augur/internal/otel_server/trace/helper"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span/model"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second
const querySize = 10000

type Type string

const (
	Error Type = "error"
	Info  Type = "info"
	Warn  Type = "warn"
	Ok    Type = "ok"
	Unset Type = "unset"
)

type SearchParams struct {
	Service   *string `json:"service,omitempty"`
	Operation *string `json:"operation,omitempty"`
	StartTime *string `json:"start_time,omitempty"`
	EndTime   *string `json:"end_time,omitempty"`
	Types     []Type  `json:"type,omitempty"`
}

type LogAndSpanQueryService interface {
	GetLogsAndSpans(ctx context.Context, errorSearchParams SearchParams) ([]model.LogAndSpan, error)
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

func (lss *LogAndSpanService) GetLogsAndSpans(
	ctx context.Context,
	searchParams SearchParams,
) ([]model.LogAndSpan, error) {
	query := getLogsAndSpansQuery(searchParams)
	queryJson, err := json.Marshal(query)
	if err != nil {
		lss.logger.Error("Error when marshalling query to JSON", zap.Error(err))
		return nil, err
	}
	localQuerySize := querySize
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := lss.ac.Search(
		queryCtx,
		string(queryJson),
		[]string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName},
		&localQuerySize,
	)
	if err != nil {
		lss.logger.Error("Error when searching for errors", zap.Error(err))
		return nil, err
	}
	finalRes, err := getLogAndSpansFromSearchResult(res)
	if err != nil {
		lss.logger.Error("Error when converting search result to LogAndSpan", zap.Error(err))
		return nil, err
	}
	return finalRes, nil
}

func getLogAndSpansFromSearchResult(res []map[string]interface{}) ([]model.LogAndSpan, error) {
	logsAndSpans := make([]model.LogAndSpan, len(res))
	for i, doc := range res {
		if _, ok := doc["timestamp"]; ok {
			log, err := logHelper.ConvertFromDocuments([]map[string]interface{}{doc})
			if err != nil {
				return nil, err
			}
			logAndSpan := model.LogAndSpan{
				LogDetails: &log[0],
			}
			logsAndSpans[i] = logAndSpan
		} else {
			span, err := spanHelper.ConvertFromDocuments([]map[string]interface{}{doc})
			if err != nil {
				return nil, err
			}
			logAndSpan := model.LogAndSpan{
				SpanDetails: &span[0],
			}
			logsAndSpans[i] = logAndSpan
		}
	}
	return logsAndSpans, nil
}
