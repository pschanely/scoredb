package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var (
	csvfile = flag.String("csvfile", "", "csv file with all records to search")
	query   = flag.String("query", "", "query function to maximize")
	limit   = flag.Int("limit", 10, "maximum number of results")
)

func main() {
	flag.Parse()

	if len(*csvfile) == 0 || len(*query) == 0 {
		fmt.Printf("ERROR: --csvfile and --query must be specified\n")
		flag.PrintDefaults()
		os.Exit(2)
	}
	fmt.Printf("csvfile=%s query=%s limit=%d\n", *csvfile, *query, *limit)

	csvReader, err := os.Open(*csvfile)
	if err != nil {
		log.Fatalf("ERROR: failed to open csvfile: %s\n", err)
	}

	csvScanner := bufio.NewScanner(csvReader)
	csvScanner.Split(bufio.ScanLines)

	if !csvScanner.Scan() {
		log.Fatalf("ERROR: no header line in csvfile\n")
	}
	header := csvScanner.Text()
	fields := strings.Split(header, ",")

	invFields := make(map[string]int)
	for i, f := range fields {
		invFields[f] = i
	}

	fmt.Printf("fields=%v invFields=%v\n", fields, invFields)
}
