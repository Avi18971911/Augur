package elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"os"
	"strings"
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

func deleteAllDocuments(es *elasticsearch.Client, indexName string) error {
	query := `{
		"query": {
			"match_all": {}
		}
	}`

	res, err := es.DeleteByQuery([]string{indexName}, strings.NewReader(query))
	if err != nil {
		return fmt.Errorf("failed to delete documents by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete documents in index %s: %s", indexName, res.String())
	}
	return nil
}
