package service

import (
	"math"
)

func fitLogNormal(mean, variance float64) (mu, sigmaSquared float64) {
	const epsilon = 1e-6
	if mean <= 0 {
		mean = epsilon
	}
	if variance <= 0 {
		variance = epsilon
	}
	sigmaSquared = math.Log(1 + variance/(mean*mean))
	mu = math.Log(mean) - 0.5*sigmaSquared
	return mu, sigmaSquared
}

func logNormalPDF(t, mu, sigma float64) float64 {
	if t <= 0 {
		return 0.0
	}
	return (1 / (t * sigma * math.Sqrt(2*math.Pi))) * math.Exp(-math.Pow(math.Log(t)-mu, 2)/(2*sigma*sigma))
}

func uniformPDF(t, a, b float64) float64 {
	if t >= a && t <= b {
		return 1 / (b - a)
	}
	return 0.0
}

func bayesianInference(t, mu, sigma, a, b, priorH1, priorH0 float64) (bayesResult float64, probability float64) {
	// Likelihoods
	likelihoodH1 := logNormalPDF(t, mu, sigma)
	likelihoodH0 := uniformPDF(t, a, b)

	// Posterior probabilities
	numeratorH1 := likelihoodH1 * priorH1
	numeratorH0 := likelihoodH0 * priorH0
	posteriorH1 := numeratorH1 / (numeratorH1 + numeratorH0)

	return posteriorH1, likelihoodH1
}

func DetermineCausality(
	sampleTDOA,
	meanTDOA,
	varianceTDOA float64,
	coOccurrences,
	occurrences int64,
) (isCausal bool, withProbability float64) {
	if meanTDOA < 0 {
		meanTDOA = math.Abs(meanTDOA)
	}
	if sampleTDOA < 0 {
		sampleTDOA = math.Abs(sampleTDOA)
	}
	mu, sigmaSquared := fitLogNormal(meanTDOA, varianceTDOA)
	sigma := math.Sqrt(sigmaSquared)
	a := 0.0
	b := math.Max(sampleTDOA, meanTDOA) * 2
	priorH1 := float64(coOccurrences) / float64(occurrences)
	priorH0 := 1 - priorH1

	bayesResult, probability := bayesianInference(sampleTDOA, mu, sigma, a, b, priorH1, priorH0)
	fulfillsHypothesis := bayesResult > 0.5
	return fulfillsHypothesis, probability
}
