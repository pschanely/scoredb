package main

import (
	"container/heap"
	"math"
)

type DocScore struct {
	DocId int64
	Score float32
}

type ResultSet []DocScore

func (h ResultSet) Len() int           { return len(h) }
func (h ResultSet) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h ResultSet) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *ResultSet) Push(x interface{}) {
	*h = append(*h, x.(DocScore))
}
func (h *ResultSet) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func BridgeQuery(numResults int, weights map[string]float32, itr DocItr) []int64 {
	results := &ResultSet{}
	heap.Init(results)
	minScore, maxScore := float32(math.Inf(-1)), float32(math.Inf(1))
	for itr.Next() {
		score := itr.Score()
		if score < minScore {
			continue
		}
		heap.Push(results, DocScore{DocId: itr.DocId(), Score: score})
		if results.Len() > numResults {
			minScore = heap.Pop(results).(DocScore).Score
			itr.SetBounds(minScore, maxScore)
		}
	}
	numResults = results.Len()
	var resultIds = make([]int64, numResults)
	for idx, docScore := range *results {
		resultIds[numResults-(idx+1)] = docScore.DocId
	}
	return resultIds
}
