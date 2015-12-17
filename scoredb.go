package main

import (
	"fmt"
	"log"
)

type StubDb struct {
}

func (sdb *StubDb) Index(record map[string]float32) int64 {
	return 7
}

func (sdb *StubDb) Query(numResults int, weights map[string]float32) []int64 {
	return []int64{7, 42}
}

func main() {
	fmt.Println("starting scoredb")
	l1 := NewMemoryDocItr(
		[]float32{1.0, 1.0, 0.5, 1.0, 0.5},
		[]int64{1, 5, 7, 8, 9},
	)
	fmt.Printf("Next = %s\n", l1.Next())

	// TODO pick implementation based on command line flags?
	var scoreDb Db = &StubDb{}

	addr := ":8080"
	fmt.Printf("Serving on %s\n", addr)
	log.Fatal(ServeHttp(addr, scoreDb))
}
