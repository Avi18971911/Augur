package service

type bucket int64

const (
	within100ms bucket = 100
	within5s    bucket = 5000
	within30s   bucket = 30000
	within1m    bucket = 60000
)

type CountInfo struct {
	coOccurrences int64
	occurrences   int64
}

type CorrelationService struct {
	// coefficient for coOccurrences of co-occurrences
	alpha float64
	// coefficient for bucket size
	beta float64
}

func NewCorrelationService() *CorrelationService {
	return &CorrelationService{
		alpha: 0.5,
		beta:  0.5,
	}
}

func (cs *CorrelationService) getConfidenceScore(countInfo CountInfo, bucket bucket) float64 {
	return cs.alpha*float64(countInfo.coOccurrences)/float64(countInfo.occurrences) + (cs.beta / float64(bucket))
}

//func (cs *CorrelationService) getConfidenceScores(serviceActionScores map[string]map[bucket]countInfo) {
//	for serviceAction, buckets := range serviceActionScores {
//		for bucket, countInfo := range buckets {
//			serviceActionScores[serviceAction][bucket] = cs.getConfidenceScore(countInfo, bucket)
//		}
//	}
//}
