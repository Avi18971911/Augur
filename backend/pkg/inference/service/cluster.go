package service

type CountCluster struct {
	ClusterId     string
	CoOccurrences int64
	Occurrences   int64
	MeanTDOA      float64
	VarianceTDOA  float64
}
