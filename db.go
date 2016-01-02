package main

import ()

type Query struct {
	Offset int
	Limit  int

	// mixed, nested arrays of strings and numbers describing a function; for example: ["+", ["field", "age"], ["field", "height"]]
	Scorer []interface{}
}

type QueryResult struct {
	Ids []int64
}

type Db interface {
	BulkIndex(records []map[string]float32) ([]int64, error)
	Index(record map[string]float32) (int64, error)
	Query(query Query) (QueryResult, error)
}
