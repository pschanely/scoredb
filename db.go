package main

import ()

type Db interface {
	BulkIndex(records []map[string]float32) ([]int64, error)
	Index(record map[string]float32) (int64, error)
	Query(numResults int, weights map[string]float32) ([]int64, error)
}
