package main

import (
	"flag"
	"fmt"
	"log"
)

type StubDb struct {
	idx int64
}

func (sdb *StubDb) Index(record map[string]float32) int64 {
	sdb.idx += 1
	return sdb.idx
}

func (sdb *StubDb) Query(numResults int, weights map[string]float32) []int64 {
	return []int64{7, 42}
}

func main() {
	var dataDir = flag.String("datadir", "scoredb-data", "data directory")
	flag.Parse()

	// TODO pick implementation based on command line flags?
	var scoreDb Db = &FsScoreDb{dataDir: *dataDir, nextId: 1}

	fmt.Println("starting scoredb in %s", *dataDir)
	addr := ":8080"
	fmt.Printf("Serving on %s\n", addr)
	log.Fatal(ServeHttp(addr, scoreDb))
}
