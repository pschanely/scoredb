package main

import ()

type Db interface {
	Index(record map[string]float32) int64
	Query(numResults int, weights map[string]float32) []int64
}
