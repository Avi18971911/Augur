package client

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8"
)

const SearchResultSize = 10

type RefreshRate string

const (
	// Wait for the changes made by the request to be made visible by a refresh before replying.
	Wait RefreshRate = "wait_for"
	// Immediate Refresh the relevant primary and replica shards (not the whole index) immediately after the operation occurs.
	Immediate RefreshRate = "true"
	// Async Take no refresh related actions. The changes made by this request will be made visible at some point after the request returns.
	Async RefreshRate = "false"
)

type AugurClient interface {
	// BulkIndex indexes (inserts) multiple documents in the same index
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
	BulkIndex(ctx context.Context, metaInfo []MetaMap, documentInfo []DocumentMap, index string) error
	// Index indexes (inserts) a single document in the index
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-index_.html
	Index(ctx context.Context, metaInfo MetaMap, documentInfo DocumentMap, index string) error
	// Search searches for documents in the index
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
	// queryResultSize is the number of results to return, -1 for default
	Search(ctx context.Context, query string, indices []string, queryResultSize *int) ([]map[string]interface{}, error)
	// SearchAfter searches for documents in the index using the search_after parameter. This allows for Pagination.
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/paginate-search-results.html
	SearchAfter(
		ctx context.Context,
		query map[string]interface{},
		indices []string,
		searchAfterParams *SearchAfterParams,
		queryResultSize *int,
	) <-chan SearchAfterResult
	// BulkUpdate updates multiple documents in the same index
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
	BulkUpdate(ctx context.Context, ids []string, fieldList []map[string]interface{}, index string) error
	// Upsert updates or inserts a document in the index using a script or upsert annotation
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-update.html#upserts
	Upsert(ctx context.Context, upsertScript map[string]interface{}, index string, id string) error
	// UpdateByQuery updates documents in the index matching the query. Useful for updating multiple documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
	UpdateByQuery(ctx context.Context, query string, indices []string) error
	// Count counts the number of documents in the index matching the query
	// https://www.elastic.co/guide/en/elasticsearch/reference/master/search-count.html
	Count(ctx context.Context, query string, indices []string) (int64, error)
}

type AugurClientImpl struct {
	es          *elasticsearch.Client
	refreshRate string
}

func NewAugurClientImpl(es *elasticsearch.Client, refreshRate RefreshRate) *AugurClientImpl {
	return &AugurClientImpl{es: es, refreshRate: string(refreshRate)}
}
