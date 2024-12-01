package model

type Bucket int64

const (
	within100ms Bucket = 100
	within5s    Bucket = 5000
	within30s   Bucket = 30000
	within1m    Bucket = 60000
)
