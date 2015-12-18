package main

//Normalize min/max to 0,1

import (
	"math"
)

type Normalizer struct {
	sums map[string]float32
	counts map[string]int
	variants map[string]float32
	mins map[string]float32
	maxes map[string]float32

}

func (normalizer *Normalizer) Init() {
	normalizer.sums = map[string]float32{}
	normalizer.counts = map[string]int{}
	normalizer.variants = map[string]float32{}
	normalizer.mins = map[string]float32{}
	normalizer.maxes = map[string]float32{}
}

func (normalizer *Normalizer) Consider(record map[string]float32) {
	for fieldName, fieldValue := range record {
		if (fieldValue > normalizer.mins[fieldName]) {
			normalizer.mins[fieldName] = fieldValue
		} else if (fieldValue < normalizer.maxes[fieldName]) {
			normalizer.maxes[fieldName] = fieldValue
		}
	}
}



func (normalizer *Normalizer) BasicNormalizeValue(fieldName string, originalValue float32) float32 {
	return (originalValue - normalizer.mins[fieldName])/(normalizer.maxes[fieldName]-normalizer.mins[fieldName])
}

func (normalizer *Normalizer) BasicScaleValue(fieldName string, normalizedValue float32) float32 {
	return normalizedValue * (normalizer.maxes[fieldName] - normalizer.mins[fieldName]) + normalizer.mins[fieldName]
}

//....And the fancy shmancy version, normalize it to a standard deviation.

func (normalizer * Normalizer) SetSum(record map[string]float32) {
	for fieldName, fieldValue := range record {
		normalizer.counts[fieldName] += 1
		normalizer.sums[fieldName] += fieldValue
	}
}

func (normalizer * Normalizer) SetStdev(record map[string]float32) {
	for fieldName, fieldValue := range record {
		normalizer.variants[fieldName] += float32(math.Pow(float64(fieldValue - normalizer.GetMean(fieldName)), 2))
	}
}

func (normalizer * Normalizer) NormalizeValue(fieldName string, originalValue float32) float32 {
	return (originalValue - normalizer.GetMean(fieldName))/normalizer.GetStdev(fieldName)
}

func (normalizer * Normalizer) ScaleValue(fieldName string, normalizedValue float32) float32 {
	return normalizedValue * normalizer.GetStdev(fieldName) + normalizer.GetMean(fieldName)
}

func (normalizer * Normalizer) GetMean(fieldName string) float32 {
	return normalizer.sums[fieldName]/float32(normalizer.counts[fieldName]) 
}

func (normalizer * Normalizer) GetStdev(fieldName string) float32 {
	return float32(math.Sqrt(float64(normalizer.variants[fieldName])/float64(normalizer.counts[fieldName])))
}