package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/model"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"strings"
	"time"
)

const SearchResultSize = 10

type AugurClient interface {
	BulkIndex(data []interface{}, metaInfo []map[string]interface{}, index string) error
	Index(data interface{}, metaInfo map[string]interface{}, index string) error
	Search(query string, index string, ctx context.Context) ([]interface{}, error)
	Update(id string, fieldList []map[string]interface{}) error
}

type AugurClientImpl struct {
	es *elasticsearch.Client
}

func NewAugurClientImpl(es *elasticsearch.Client) *AugurClientImpl {
	return &AugurClientImpl{es: es}
}

func (a *AugurClientImpl) Update(
	id string,
	fieldList []map[string]interface{},
) error {
	var buf bytes.Buffer
	for _, fields := range fieldList {
		update := map[string]interface{}{
			"update": map[string]interface{}{
				"_id": id,
			},
		}
		metaJSON, err := json.Marshal(update)
		if err != nil {
			return fmt.Errorf("error marshaling update: %w", err)
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		fieldJSON, err := json.Marshal(map[string]interface{}{"doc": fields})
		if err != nil {
			return fmt.Errorf("error marshaling field to update: %w", err)
		}
		buf.Write(fieldJSON)
		buf.WriteByte('\n')
	}

	res, err := a.es.Bulk(bytes.NewReader(buf.Bytes()), a.es.Bulk.WithIndex(LogIndexName))
	if err != nil {
		return fmt.Errorf("failed to update in Elasticsearch: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("bulk update error: %s", res.String())
	}
	return nil
}

func (a *AugurClientImpl) BulkIndex(
	data []interface{},
	metaInfo []map[string]interface{},
	index string,
) error {
	var buf bytes.Buffer
	for i, d := range data {
		var meta map[string]interface{}
		if metaInfo != nil && i < len(metaInfo) {
			meta = metaInfo[i]
		} else {
			// empty meta for bulk index
			meta = map[string]interface{}{"index": map[string]interface{}{}}
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("error marshaling meta to bulk index: %w", err)
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		dataJSON, err := json.Marshal(d)
		if err != nil {
			return fmt.Errorf("error marshaling data to bulk index: %w", err)
		}
		buf.Write(dataJSON)
		buf.WriteByte('\n')
	}
	var res *esapi.Response
	var err error
	if len(index) > 0 {
		res, err = a.es.Bulk(bytes.NewReader(buf.Bytes()), a.es.Bulk.WithIndex(index))
	} else {
		res, err = a.es.Bulk(bytes.NewReader(buf.Bytes()))
	}
	if err != nil {
		return fmt.Errorf("error bulk indexing: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("bulk index error: %s", res.String())
	}
	return nil
}

func (a *AugurClientImpl) Index(data interface{}, metaInfo map[string]interface{}, index string) error {
	return a.BulkIndex([]interface{}{data}, []map[string]interface{}{metaInfo}, index)
}

func (a *AugurClientImpl) Search(query string, index string, ctx context.Context) ([]interface{}, error) {
	esContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := a.es.Search(
		a.es.Search.WithContext(esContext),
		a.es.Search.WithIndex(index),
		a.es.Search.WithBody(strings.NewReader(query)),
		a.es.Search.WithPretty(),
		a.es.Search.WithSize(SearchResultSize),
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

	// convert esResponse to source
	var results []interface{}
	for _, hit := range esResponse.Hits.HitArray {
		results = append(results, hit.Source)
	}

	return results, nil
}

func ToInterfaceSlice[T any](values []T) []interface{} {
	interfaces := make([]interface{}, len(values))
	for i, v := range values {
		interfaces[i] = v
	}
	return interfaces
}

func ToTypedSlice[T any](values []interface{}) []T {
	typed := make([]T, len(values))
	for i, v := range values {
		typed[i] = v.(T)
	}
	return typed
}
