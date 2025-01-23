package model

type ClusterWindowCount struct {
	ClusterId    string
	CoClusterId  string
	Start        float64
	End          float64
	Occurrences  int64
	MeanTDOA     float64
	VarianceTDOA float64
}
