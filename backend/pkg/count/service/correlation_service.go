package service

type CorrelationService struct {
	// coefficient for CoOccurrences of co-Occurrences
	alpha float64
	// coefficient for Bucket size
	beta float64
}

func NewCorrelationService() *CorrelationService {
	return &CorrelationService{
		alpha: 0.5,
		beta:  0.5,
	}
}

//func (cs *CorrelationService) getConfidenceScore(countInfo model.CountInfo, bucket model.Bucket) float64 {
//	return cs.alpha*float64(countInfo.Occurrences)/float64(countInfo.Occurrences) + (cs.beta / float64(bucket))
//}

//func (cs *CorrelationService) getConfidenceScores(serviceActionScores map[string]map[Bucket]countInfo) {
//	for serviceAction, buckets := range serviceActionScores {
//		for Bucket, countInfo := range buckets {
//			serviceActionScores[serviceAction][Bucket] = cs.getConfidenceScore(countInfo, Bucket)
//		}
//	}
//}
