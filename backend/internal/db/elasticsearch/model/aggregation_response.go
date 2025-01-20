package model

type EsAggregationResponse struct {
	Took         int         `json:"took"`
	TimedOut     bool        `json:"timed_out"`
	Aggregations Aggregation `json:"aggregations"`
}

type Aggregation struct {
	TopTagsAggregation TopTags `json:"top_tags"`
}

type TopTags struct {
	Buckets []Bucket `json:"buckets"`
}

type Bucket struct {
	Key      string        `json:"key"`
	DocCount int           `json:"doc_count"`
	TopHits  HitsContainer `json:"top_tagged_hits"`
}

type HitsContainer struct {
	Hits AggregationHits `json:"hits"`
}

type AggregationHits struct {
	Total    AggregationTotal `json:"total"`
	MaxScore *float64         `json:"max_score"`
	Hits     []Hit            `json:"hits"`
}

type AggregationTotal struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Hit struct {
	Index  string                 `json:"_index"`
	ID     string                 `json:"_id"`
	Score  *float64               `json:"_score"`
	Source map[string]interface{} `json:"_source"`
	Sort   []interface{}          `json:"sort"`
}
