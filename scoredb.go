package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
)

func main() {

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	servePort := serveCommand.Int("port", 11625, "listening port in http mode, defaults to 11625")
	serveIntf := serveCommand.String("interface", "", "network interface to listen on in http mode, defaults to empty string (any interface)")
	serveDbType := serveCommand.String("dbtype", "fsscoredb", "Type of database to run")
	serveDataDir := serveCommand.String("datadir", "./data", "Storage directory for database")

	benchCommand := flag.NewFlagSet("benchmark", flag.ExitOnError)
	benchCsvFilename := benchCommand.String("csv", "", "csv filename of census data")
	benchEsUrl := benchCommand.String("esurl", "http://localhost:9200/", "URL of elasticsearch instance")
	benchEsIndex := benchCommand.String("esindex", "benchmark_scoredb", "Index name to use for elasticsearch")
	benchFsDataDir := benchCommand.String("fsdatadir", "./benchmark_data", "Storage directory for native scoredb database")

	/*
		for cmd := range([]*flag.FlagSet{serveCommand, benchCommand}) {
			// common args here
		}
	*/

	if len(os.Args) <= 1 {
		fmt.Println("usage: scoredb <command> [<args>]")
		fmt.Println("Commands:")
		fmt.Println(" serve      Run a scoredb server")
		fmt.Println(" benchmark  Run performance benchmarks")
		fmt.Println("For more help, run scoredb <command> -h")
		os.Exit(0)
	}
	switch os.Args[1] {
	case "serve":
		serveCommand.Parse(os.Args[2:])
		var db Db
		if *serveDbType == "scoredb" {
			db = BaseDb{BaseStreamingDb{NewFsScoreDb(*serveDataDir)}}
		}
		addr := fmt.Sprintf("%s:%d", *serveIntf, *servePort)
		fmt.Printf("Serving on %s\n", addr)
		log.Fatal(ServeHttp(addr, db.(Db)))
	case "benchmark":
		runtime.GOMAXPROCS(runtime.NumCPU())
		benchCommand.Parse(os.Args[2:])
		esDb := &EsScoreDb{BaseURL: *benchEsUrl, Index: *benchEsIndex, NextId: 1}
		//fsDb := BaseDb{BaseStreamingDb{NewFsScoreDb(path.Join(*benchFsDataDir, "shard1"))}}
		fsDb := BaseDb{ShardedDb{
			Shards: []StreamingDb{ // 4 shards to match elasticsearch defaults
				BaseStreamingDb{NewFlatFsScoreDb(path.Join(*benchFsDataDir, "shard1"))},
				BaseStreamingDb{NewFlatFsScoreDb(path.Join(*benchFsDataDir, "shard2"))},
				BaseStreamingDb{NewFlatFsScoreDb(path.Join(*benchFsDataDir, "shard3"))},
				BaseStreamingDb{NewFlatFsScoreDb(path.Join(*benchFsDataDir, "shard4"))},
			},
		}}
		batchSize := 10000
		if !Exists(*benchCsvFilename) {
			log.Fatal(fmt.Sprintf("Cannot find source csv data file at '%s'", *benchCsvFilename))
		}

		fmt.Printf("Running es benchmarks\n")
		esDb.DeleteIndex()
		esDb.CreateIndex()
		esIndexTimes, esQueryTimes, err := RunBenchmark(esDb, *benchCsvFilename, batchSize)
		//esDb.DeleteIndex()
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to run es benchmark: %v\n", err))
		}

		fmt.Printf("Running native benchmarks\n")
		fsIndexTimes, fsQueryTimes, err := RunBenchmark(fsDb, *benchCsvFilename, batchSize)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to run native benchmark: %v\n", err))
		}

		fmt.Printf("records,es_index,native_index,es_query_1,native_query_1,es_query_2,native_query_2\n")
		for idx := 0; idx < len(esIndexTimes); idx++ {
			fmt.Printf("%v,%v,%v", idx*batchSize, esIndexTimes[idx], fsIndexTimes[idx])
			for idx2 := 0; idx2 < len(esQueryTimes[idx]); idx2++ {
				fmt.Printf(",%v,%v", esQueryTimes[idx][idx2], fsQueryTimes[idx][idx2])
			}		
			fmt.Printf("\n")
		}
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}
}
