package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pschanely/scoredb"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"time"
)

func MakeStandardDb(dataDir string, numShards int) (*scoredb.BaseDb, error) {
	shards := make([]scoredb.StreamingDb, numShards)
	for i := range shards {
		shardDir := path.Join(dataDir, fmt.Sprintf("shard.%d", i))
		shards[i] = scoredb.BaseStreamingDb{scoredb.NewFsScoreDb(shardDir)}
	}
	idDb, err := scoredb.NewBoltIdDb(path.Join(dataDir, "iddb"))
	if err != nil {
		return nil, err
	}
	return &scoredb.BaseDb{
		StreamingDb: scoredb.ShardedDb{
			Shards: shards,
		},
		IdDb: idDb,
	}, nil
}

func watchDir(dbChannel chan scoredb.Db, baseDir string, namePrefix string) {
	log.Printf("Watching for databases at %s%s*\n", baseDir, namePrefix)
	var lastName = ""
	for {
		dir, err := os.Open(baseDir)
		var fileInfos []os.FileInfo
		if err == nil {
			fileInfos, err = dir.Readdir(0)
		}
		if err != nil {
			log.Printf("Unable to read %v: %v\n", dir, err)
			time.Sleep(55 * time.Second)
		} else {
			var newDbName = ""
			for _, fileInfo := range fileInfos {
				name := fileInfo.Name()
				if strings.HasPrefix(name, namePrefix) {
					if name > newDbName {
						newDbName = name
					}
				}
			}
			if newDbName > lastName {
				fmt.Printf("Detected database at %s%s\n", baseDir, newDbName)
				newDb, err := MakeStandardDb(newDbName, 1)
				if err != nil {
					log.Printf("Unable to load database at %s%s (%v); ignoring\n", dir, newDbName, err)
				} else {
					fmt.Printf("The database at %s%s is live\n", baseDir, newDbName)
					dbChannel <- newDb
					lastName = newDbName
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func SetupDirLoading(databaseDir string) *scoredb.MigratableDb {
	dbChannel := make(chan scoredb.Db)
	migratable := scoredb.MigratableDb{Current: nil, NextDbs: dbChannel}
	baseDir, namePrefix := path.Split(databaseDir)
	fmt.Printf("Watching for new databases named %s* in %s\n", namePrefix, baseDir)
	go watchDir(dbChannel, baseDir, namePrefix)
	return &migratable
}

func main() {

	serveCommand := flag.NewFlagSet("serve", flag.ExitOnError)
	servePort := serveCommand.Int("port", 11625, "listening port in http mode, defaults to 11625")
	serveIntf := serveCommand.String("interface", "", "network interface to listen on in http mode, defaults to empty string (any interface)")
	serveDataDir := serveCommand.String("datadir", "./data", "Storage directory for database")
	serveNumShards := serveCommand.Int("numshards", 4, "Number of shards")
	serveReadOnly := serveCommand.Bool("readonly", false, "Only allow GET requests")
	serveAutoMigrate := serveCommand.Bool("automigrate", false, "When new directories appear matching <datadir>*, atomically swap in the database at that directory. (lexigraphically last)")

	loadCommand := flag.NewFlagSet("load", flag.ExitOnError)
	loadDataDir := loadCommand.String("datadir", "./data", "Storage directory for database")
	loadNumShards := loadCommand.Int("numshards", 4, "Number of shards (ignored if db already exists)")

	benchCommand := flag.NewFlagSet("benchmark", flag.ExitOnError)
	benchCsvFilename := benchCommand.String("csv", "", "csv filename of census data")
	benchMaxRecords := benchCommand.Int64("maxrecords", 1000*1000, "Maximum size of database to benchmark (in # of records)")
	benchCsvOutput := benchCommand.String("out", "output.csv", "csv of performance data to output")
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
		fmt.Println(" load       Load json lines from stdin")
		fmt.Println(" benchmark  Run performance benchmarks")
		fmt.Println("For more help, run scoredb <command> -h")
		os.Exit(1)
	}
	var db scoredb.Db
	var err error
	switch os.Args[1] {
	case "serve":
		serveCommand.Parse(os.Args[2:])
		if *serveAutoMigrate {
			db = SetupDirLoading(*serveDataDir)
		} else {
			db, err = MakeStandardDb(*serveDataDir, *serveNumShards)
			if err != nil {
				log.Fatalf("Failed to initialize database at %v: %v\n", *serveDataDir, err)
			}
		}
		addr := fmt.Sprintf("%s:%d", *serveIntf, *servePort)
		fmt.Printf("Serving on %s\n", addr)
		log.Fatal(scoredb.ServeHttp(addr, db, *serveReadOnly))
	case "load":
		loadCommand.Parse(os.Args[2:])
		db, err := MakeStandardDb(*loadDataDir, *loadNumShards)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to initialize database at %v: %v\n", *loadDataDir, err))
		}
		scanner := bufio.NewScanner(os.Stdin)
		batchSize := 200
		batchIndex := 0
		var batch = make([]scoredb.Record, batchSize)
		for scanner.Scan() {
			record := scoredb.Record{}
			line := scanner.Bytes()
			json.Unmarshal(line, &record)
			batch[batchIndex] = record
			batchIndex += 1
			if batchIndex >= batchSize {
				db.BulkIndex(batch)
				batchIndex = 0
				batch = make([]scoredb.Record, batchSize)
			}
		}
		if batchIndex > 0 {
			db.BulkIndex(batch)
		}
	case "benchmark":
		outputFd, err := os.Create(*benchCsvOutput)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to output output csv file at %v: %v\n", *benchCsvOutput, err))
		}

		runtime.GOMAXPROCS(runtime.NumCPU())
		benchCommand.Parse(os.Args[2:])
		esDb := &scoredb.EsScoreDb{BaseURL: *benchEsUrl, Index: *benchEsIndex}
		fsDb, err := MakeStandardDb(*benchFsDataDir, 4)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to initialize database at %v: %v\n", *benchFsDataDir, err))
		}
		if !scoredb.Exists(*benchCsvFilename) {
			log.Fatal(fmt.Sprintf("Cannot find source csv data file at '%s'", *benchCsvFilename))
		}

		fmt.Printf("Running es benchmarks\n")
		esDb.DeleteIndex()
		esDb.CreateIndex()
		counts, esIndexTimes, esQueryTimes, err := scoredb.RunBenchmark(esDb, *benchCsvFilename, *benchMaxRecords)
		//esDb.DeleteIndex()
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to run es benchmark: %v\n", err))
		}

		fmt.Printf("Running native benchmarks\n")
		_, fsIndexTimes, fsQueryTimes, err := scoredb.RunBenchmark(fsDb, *benchCsvFilename, *benchMaxRecords)
		if err != nil {
			log.Fatal(fmt.Sprintf("Failed to run native benchmark: %v\n", err))
		}

		fmt.Fprintf(outputFd, "records,es_index,native_index,es_query_1,native_query_1,es_query_2,native_query_2\n")
		for idx := 0; idx < len(esIndexTimes); idx++ {
			fmt.Fprintf(outputFd, "%v,%v,%v", counts[idx], esIndexTimes[idx], fsIndexTimes[idx])
			for idx2 := 0; idx2 < len(esQueryTimes[idx]); idx2++ {
				fmt.Fprintf(outputFd, ",%v,%v", esQueryTimes[idx][idx2], fsQueryTimes[idx][idx2])
			}
			fmt.Fprintf(outputFd, "\n")
		}
		outputFd.Close()
	default:
		fmt.Printf("%q is not valid command.\n", os.Args[1])
		os.Exit(2)
	}
}
