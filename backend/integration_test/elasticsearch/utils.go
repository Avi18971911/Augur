package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/elastic/go-elasticsearch/v8"
	"os"
	"time"
)

func loadTestDataFromFile(es *elasticsearch.Client, indexName, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read dump file: %w", err)
	}

	var documents []map[string]interface{}
	if err := json.Unmarshal(data, &documents); err != nil {
		return fmt.Errorf("failed to unmarshal JSON data: %w", err)
	}

	for _, doc := range documents {
		delete(doc, "_id")
		delete(doc, "_index")
		delete(doc, "_score")
		delete(doc, "_ignored")
		// elastic search doesn't like the _source field, need to move it to top-level
		if source, ok := doc["_source"].(map[string]interface{}); ok {
			for key, value := range source {
				doc[key] = value
			}
			delete(doc, "_source")
		}
		docJSON, _ := json.Marshal(doc)
		res, err := es.Index(indexName, bytes.NewReader(docJSON), es.Index.WithRefresh("true"))
		if err != nil {
			return fmt.Errorf("failed to index document: %w", err)
		}
		defer res.Body.Close()
		if res.IsError() {
			return fmt.Errorf("failed to index document: %s", res.String())
		}
	}

	return nil
}

func deleteAllDocuments(es *elasticsearch.Client) error {
	indexes := []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName, bootstrapper.CountIndexName}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	queryJSON, _ := json.Marshal(query)
	res, err := es.DeleteByQuery(indexes, bytes.NewReader(queryJSON), es.DeleteByQuery.WithRefresh(true))
	if err != nil {
		return fmt.Errorf("failed to delete documents by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete documents in index %s", res.String())
	}
	return nil
}

func loadDataIntoElasticsearch[Data any](ac client.AugurClient, data []Data, index string) error {
	metaMap, dataMap, err := client.ToMetaAndDataMap(data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ac.BulkIndex(ctx, metaMap, dataMap, &index)
	if err != nil {
		return err
	}
	return nil
}

func getAllQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}

func convertToLogDocuments(data []map[string]interface{}) ([]model.LogEntry, error) {
	var docs []model.LogEntry

	for _, item := range data {
		doc := model.LogEntry{}

		timestamp, ok := item["timestamp"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert timestamp to string %v", item["timestamp"])
		}

		timestampParsed, err := client.NormalizeTimestampToNanoseconds(timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to convert timestamp '%s' to time.Time: %v", timestamp, err)
		}

		doc.Timestamp = timestampParsed

		createdAt, ok := item["created_at"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert created_at to string")
		}
		createdAtParsed, err := client.NormalizeTimestampToNanoseconds(createdAt)
		if err != nil {
			return nil, fmt.Errorf("failed to convert created_at '%s' to time.Time: %v", createdAt, err)
		}
		doc.CreatedAt = createdAtParsed

		severity, ok := item["severity"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert severity to string")
		}

		doc.Severity = model.Level(severity)

		message, ok := item["message"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert message to string")
		}
		doc.Message = message

		service, ok := item["service"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert service to string")
		}
		doc.Service = service

		clusterId, ok := item["cluster_id"].(string)
		if ok {
			doc.ClusterId = clusterId
		}

		doc.Id = item["_id"].(string)
		docs = append(docs, doc)
	}

	return docs, nil
}
