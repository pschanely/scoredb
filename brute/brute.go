package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

var (
	csvfile = flag.String("csvfile", "", "csv file with all records to search")
	query   = flag.String("query", "", "query function to maximize")
	limit   = flag.Int("limit", 10, "maximum number of results")
)

type QTerm struct {
	idx    int
	weight float32
}

func (t QTerm) String() string { return fmt.Sprintf("%d=%f", t.idx, t.weight) }

type byIdx []QTerm

func (q byIdx) Len() int           { return len(q) }
func (q byIdx) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q byIdx) Less(i, j int) bool { return q[i].idx < q[j].idx }

func parseQuery(header, query string) []QTerm {
	fields := strings.Split(header, ",")
	fmt.Printf("fields=%v\n", fields)

	invFields := make(map[string]int)
	for i, f := range fields {
		invFields[f] = i
	}

	rawTerms := strings.Split(query, ",")
	terms := make([]QTerm, 0, len(rawTerms))

	for _, t := range rawTerms {
		pair := strings.Split(t, "=")
		if len(pair) != 2 {
			log.Fatalf("ERROR: malformed query\n")
		}
		idx, ok := invFields[pair[0]]
		if !ok {
			log.Fatalf("ERROR: malformed query: unknown field '%s'\n", pair[0])
		}
		val64, err := strconv.ParseFloat(pair[1], 32)
		if err != nil {
			log.Fatalf("ERROR: malformed query: %s\n", err)
		}
		terms = append(terms, QTerm{idx, float32(val64)})
	}

	sort.Sort(byIdx(terms))
	return terms
}

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

	terms := parseQuery(header, *query)
	fmt.Printf("terms=%v\n", terms)

	topScore := float32(math.Inf(-1))
	topIdx := int64(-1)
	lineIdx := int64(0)

	for csvScanner.Scan() {
		var score float32
		row := csvScanner.Text()
		vals := strings.Split(row, ",")
		for _, t := range terms {
			// no error checking :)
			fv, _ := strconv.ParseFloat(vals[t.idx], 32)
			score += float32(fv) * t.weight
		}
		if score > topScore {
			topIdx = lineIdx
			topScore = score
		}
		lineIdx += 1
	}
	if serr := csvScanner.Err(); serr != nil {
		log.Fatalf("Error: failed reading csv: %s\n", serr)
	}
	fmt.Printf("Top score=%f idx=%d\n", topScore, topIdx)
}
