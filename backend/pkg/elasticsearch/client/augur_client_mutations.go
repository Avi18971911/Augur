package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func (a *AugurClientImpl) BulkUpdate(
	ctx context.Context,
	ids []string,
	fieldList []map[string]interface{},
	index string,
) error {
	var buf bytes.Buffer
	for i, fields := range fieldList {
		update := map[string]interface{}{
			"update": map[string]interface{}{
				"_id": ids[i],
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

	res, err := a.es.Bulk(
		bytes.NewReader(buf.Bytes()),
		a.es.Bulk.WithIndex(index),
		a.es.Bulk.WithContext(ctx),
		a.es.Bulk.WithRefresh(a.refreshRate),
	)
	if err != nil {
		return fmt.Errorf("failed to update in Elasticsearch: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("bulk update error: %s", res.String())
	}
	return nil
}

func (a *AugurClientImpl) Upsert(
	ctx context.Context,
	upsertScript map[string]interface{},
	index string,
	id string,
) error {
	var buf bytes.Buffer
	upsertJSON, err := json.Marshal(upsertScript)
	if err != nil {
		return fmt.Errorf("error marshaling upsert: %w", err)
	}
	buf.Write(upsertJSON)

	res, err := a.es.Update(
		index, id,
		bytes.NewReader(buf.Bytes()),
		a.es.Update.WithContext(ctx),
		a.es.Update.WithRefresh(a.refreshRate),
	)
	if err != nil {
		return fmt.Errorf("failed to upsert in Elasticsearch: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("upsert error: %s", res.String())
	}
	return nil
}

func (a *AugurClientImpl) BulkIndex(
	ctx context.Context,
	data []map[string]interface{},
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
		res, err = a.es.Bulk(
			bytes.NewReader(buf.Bytes()),
			a.es.Bulk.WithIndex(index),
			a.es.Bulk.WithContext(ctx),
			a.es.Bulk.WithRefresh(a.refreshRate),
		)
	} else {
		res, err = a.es.Bulk(
			bytes.NewReader(buf.Bytes()),
			a.es.Bulk.WithContext(ctx),
			a.es.Bulk.WithRefresh(a.refreshRate),
		)
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

func (a *AugurClientImpl) Index(
	ctx context.Context,
	data map[string]interface{},
	metaInfo map[string]interface{},
	index string,
) error {
	if metaInfo == nil {
		return a.BulkIndex(ctx, []map[string]interface{}{data}, nil, index)
	}
	return a.BulkIndex(ctx, []map[string]interface{}{data}, []map[string]interface{}{metaInfo}, index)
}
