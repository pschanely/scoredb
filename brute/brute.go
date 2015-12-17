package main

import (
	"flag"
	"fmt"
)

var (
	csvfile = flag.String("csvfile", "", "csv file with all records to search")
	query   = flag.String("query", "", "query function to maximize")
	limit   = flag.Int("limit", 10, "maximum number of results")
)

func main() {
	flag.Parse()
	fmt.Printf("csvfile=%s query=%s limit=%d\n", *csvfile, *query, *limit)
}
