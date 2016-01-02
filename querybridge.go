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

func BridgeQuery(query Query, itr DocItr) []int64 {
	offset, limit := query.Offset, query.Limit
	numResults := offset + limit
	resultData := make(ResultSet, 0, numResults+1)
	results := &resultData
	heap.Init(results)
	minScore, maxScore := float32(math.Inf(-1)), float32(math.Inf(1))
	docId := int64(-1)
	for itr.Next(docId + 1) {
		score := itr.Score()
		docId = itr.DocId()
		if score > minScore {
			heap.Push(results, DocScore{DocId: docId, Score: score})
			if results.Len() > numResults {
				minScore = heap.Pop(results).(DocScore).Score
				itr.SetBounds(minScore, maxScore)
			}
		}
	}
	itr.Close()

	for offset > 0 && len(resultData) > 0 {
		heap.Pop(results)
		offset -= 1
	}

	numResults = results.Len()
	var resultIds = make([]int64, numResults)
	for idx, _ := range resultIds {
		resultIds[numResults-(idx+1)] = heap.Pop(results).(DocScore).DocId
	}
	return resultIds
}
