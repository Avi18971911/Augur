package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type AugurClient interface {
	BulkIndex(data []interface{}, metaInfo []map[string]interface{}, index string) error
	Index(data interface{}, index string) error
	Search(query string, index string, ctx context.Context) ([]interface{}, error)
}

type AugurClientImpl struct {
	es *elasticsearch.Client
}

func NewAugurClientImpl(es *elasticsearch.Client) *AugurClientImpl {
	return &AugurClientImpl{es: es}
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

func (a *AugurClientImpl) Index(data interface{}, index string) error {
	return nil
}

func (a *AugurClientImpl) Search(query string, index string, ctx context.Context) ([]interface{}, error) {
	return nil, nil
}

func ToInterfaceSlice[T any](values []T) []interface{} {
	interfaces := make([]interface{}, len(values))
	for i, v := range values {
		interfaces[i] = v
	}
	return interfaces
}
