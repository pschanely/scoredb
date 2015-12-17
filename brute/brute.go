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

type Result struct {
	id    string
	score float32
}

func parseQuery(header, query string) []QTerm {
	fields := strings.Split(header, ",")
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
		if idx == 0 {
			log.Fatalf("ERROR: malformed query: cannot use id (first) field\n")
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
		os.Exit(1)
	}
	if *limit <= 0 {
		log.Fatalf("ERROR: --limit must be greater than zero")
	}
	fmt.Printf(" > csvfile=%s query=%s limit=%d\n", *csvfile, *query, *limit)

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
	fmt.Printf(" > column=weight : %v\n", terms)

	topResults := make([]Result, *limit)
	for i, _ := range topResults {
		topResults[i].score = float32(math.Inf(-1))
	}

	for csvScanner.Scan() {
		var score float32
		row := csvScanner.Text()
		vals := strings.Split(row, ",")
		for _, t := range terms {
			// no error checking :)
			fv, _ := strconv.ParseFloat(vals[t.idx], 32)
			score += float32(fv) * t.weight
		}

		ridx := *limit - 1
		for ridx >= 0 {
			if score <= topResults[ridx].score {
				break
			}
			if ridx < *limit-1 {
				topResults[ridx+1] = topResults[ridx]
			}
			ridx -= 1
		}
		loc := ridx + 1
		if loc < *limit {
			topResults[loc] = Result{vals[0], score}
		}
	}
	if serr := csvScanner.Err(); serr != nil {
		log.Fatalf("Error: failed reading csv: %s\n", serr)
	}

	fmt.Printf("Top results:\n")
	for _, r := range topResults {
		fmt.Printf(" %-10s %f\n", r.id, r.score)
	}
}
