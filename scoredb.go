package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {

	var (
		mode        = flag.String("mode", "http", "mode to run, http or bench")
		dbType      = flag.String("db", "scoredb", "db implementation: scoredb, brute, es, or stub")
		csvFilename = flag.String("csv", "", "csv filename, required for bench mode")
		dataDir     = flag.String("datadir", "data", "data dir scoredb uses, defaults to 'data'")
	)

	flag.Parse()

	var db Db

	if *dbType == "scoredb" {
		db = &FsScoreDb{dataDir: *dataDir}
	} else if *dbType == "brute" {
		panic("brute not implemented")
	} else if *dbType == "es" {
		log.Fatal("es not implemented")
	} else if *dbType == "stub" {
		db = &StubDb{}
	} else {
		log.Fatalf("Unknown db: %s\n", *dbType)
	}

	if *mode == "http" {
		addr := ":8080"
		fmt.Printf("Serving on %s\n", addr)
		log.Fatal(ServeHttp(addr, db))
	} else if *mode == "bench" {
		if *csvFilename == "" {
			log.Fatal("missing csv filename")
		}
		err := RunBenchmark(db, *csvFilename)
		if err != nil {
			panic(err)
		}
	} else {
		log.Fatalf("Unknown mode: %s\n", *mode)
	}
}
