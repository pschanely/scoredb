package main

import ()

type Db interface {
	BulkIndex(records []map[string]float32) []int64
	Index(record map[string]float32) int64
	Query(numResults int, weights map[string]float32) []int64
}
