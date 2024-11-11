package model

type CountResponse struct {
	Count  int    `json:"count"`
	Shards Shards `json:"_shards"`
}

type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}
