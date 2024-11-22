package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/model"
	"io"
	"log"
	"strings"
	"time"
)

const timeOut = 1 * time.Second

func (a *AugurClientImpl) Search(
	ctx context.Context,
	query string,
	indices []string,
	queryResultSize *int,
) ([]map[string]interface{}, error) {
	res, err := a.es.Search(
		a.es.Search.WithContext(ctx),
		a.es.Search.WithIndex(indices...),
		a.es.Search.WithBody(strings.NewReader(query)),
		a.es.Search.WithPretty(),
		a.es.Search.WithSize(getQuerySize(queryResultSize)),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("failed to execute query: %s", res.String())
	}

	var esResponse model.EsResponse
	if err := json.NewDecoder(res.Body).Decode(&esResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response body: %w", err)
	}

	var results []map[string]interface{}
	for _, hit := range esResponse.Hits.HitArray {
		results = append(results, hit.Source)
		results[len(results)-1]["_id"] = hit.ID
	}

	return results, nil
}

func (a *AugurClientImpl) Count(
	ctx context.Context,
	query string,
	indices []string,
) (int64, error) {
	res, err := a.es.Count(
		a.es.Count.WithContext(ctx),
		a.es.Count.WithIndex(indices...),
		a.es.Count.WithBody(strings.NewReader(query)),
		a.es.Count.WithPretty(),
	)

	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, fmt.Errorf("failed to execute query: %s", res.String())
	}

	var countResponse model.CountResponse
	if err := json.NewDecoder(res.Body).Decode(&countResponse); err != nil {
		return 0, fmt.Errorf("failed to decode response body: %w", err)
	}

	return int64(countResponse.Count), nil
}

func (a *AugurClientImpl) UpdateByQuery(
	ctx context.Context,
	query string,
	indices []string,
) error {
	res, err := a.es.UpdateByQuery(
		indices,
		a.es.UpdateByQuery.WithBody(strings.NewReader(query)),
		a.es.UpdateByQuery.WithContext(ctx),
		a.es.UpdateByQuery.WithRefresh(a.refreshRate == string(Immediate)),
	)
	if err != nil {
		return fmt.Errorf("failed to update by query in Elasticsearch: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("update by query error: %s", res.String())
	}
	return nil
}

type SearchAfterParams struct {
	Id        string
	CreatedAt time.Time
}

func (a *AugurClientImpl) SearchAfter(
	ctx context.Context,
	query map[string]interface{},
	indices []string,
	searchAfterParams *SearchAfterParams,
	querySize *int,
) (string, bool, error) {
	keepAlive := "1m"
	pitId, err := a.openPointInTime(ctx, indices, keepAlive)
	if err != nil {
		return s, b, err2
	}

	defer a.closePointInTime(ctx, pitId, err)
	pitQuery := buildSearchWithPitQuery(query, pitId, searchAfterParams)
	pitQueryJson, err := json.Marshal(pitQuery)
	if err != nil {
		return "", false, fmt.Errorf("failed to marshal pit query: %w", err)
	}

	res, err := a.es.Search(
		a.es.Search.WithContext(ctx),
		a.es.Search.WithIndex(indices...),
		a.es.Search.WithBody(strings.NewReader(string(pitQueryJson))),
		a.es.Search.WithSize(getQuerySize(querySize)),
	)
	if err != nil {
		return "", false, fmt.Errorf("failed to execute query: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return "", false, fmt.Errorf("failed to execute query: %s", res.String())
	}
	return pitId, true, nil
}

func (a *AugurClientImpl) openPointInTime(
	ctx context.Context,
	indices []string,
	keepAlive string,
) (string, error) {
	openPitCtx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()
	pitRes, err := a.es.OpenPointInTime(
		indices,
		keepAlive,
		a.es.OpenPointInTime.WithContext(openPitCtx),
	)
	if err != nil {
		return "", fmt.Errorf("failed to open point in time: %w", err)
	}
	defer pitRes.Body.Close()
	if pitRes.IsError() {
		return "", fmt.Errorf("failed to open point in time: %s", pitRes.String())
	}
	pitId, err := readPitBody(pitRes.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read pit id: %w", err)
	}
	return pitId, err
}

func (a *AugurClientImpl) closePointInTime(ctx context.Context, pitID string, err error) error {
	var buf bytes.Buffer
	body := map[string]interface{}{"pit_id": pitID}
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		log.Fatalf("Error encoding close PIT request body: %v", err)
	}

	closePitCtx, cancel := context.WithTimeout(ctx, timeOut)
	defer cancel()
	res, err := a.es.ClosePointInTime(
		a.es.ClosePointInTime.WithBody(&buf),
		a.es.ClosePointInTime.WithContext(closePitCtx),
	)
	if err != nil {
		return fmt.Errorf("failed to close PIT: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("failed to close PIT: %s", res.String())
	}
	return nil
}

func readPitBody(body io.ReadCloser) (string, error) {
	var pitResponse map[string]any
	if err := json.NewDecoder(body).Decode(&pitResponse); err != nil {
		return "", fmt.Errorf("failed to decode pit response: %w", err)
	}
	pitId, ok := pitResponse["id"].(string)
	if !ok {
		return "", fmt.Errorf("failed to read pit id")
	}
	return pitId, nil
}

func getQuerySize(querySize *int) int {
	if querySize == nil {
		return SearchResultSize
	} else {
		return *querySize
	}
}

func buildSearchWithPitQuery(
	query map[string]interface{},
	pitId string,
	searchAfterParams *SearchAfterParams,
) map[string]interface{} {
	query["pit"] = map[string]string{
		"id": pitId,
	}
	query["sort"] = []map[string]interface{}{
		{
			"_id": "asc",
		},
		{
			"created_at": "asc",
		},
	}
	if searchAfterParams != nil {
		query["search_after"] = []interface{}{searchAfterParams.Id, searchAfterParams.CreatedAt}
	}
	return query
}
