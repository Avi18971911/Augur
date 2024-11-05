package model

// Structs for parsing the Elasticsearch response
type EsResponse struct {
	Took     int       `json:"took"`
	TimedOut bool      `json:"timed_out"`
	Shards   ShardInfo `json:"_shards"`
	Hits     Hits      `json:"hits"`
}

type ShardInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

type Hits struct {
	Total    Total       `json:"total"`
	MaxScore float64     `json:"max_score"`
	HitArray []HitSource `json:"hits"`
}

type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type HitSource struct {
	Index  string        `json:"_index"`
	ID     string        `json:"_id"`
	Score  float64       `json:"_score"`
	Source []interface{} `json:"_source"`
}
