package main

import (
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const baseURL = "http://localhost:9200/"
const batchSize = 100000
const dbname = "scoredb"

var (
	deleteflag = flag.Bool("delete", false, "delete data from elasticsearch")
	loadflag   = flag.String("load", "", "csv file with all records to search")
	queryflag  = flag.String("query", "", "column_name=weighting_factor,...")
)

func deleteElasticData(index string) {
	req, _ := http.NewRequest("DELETE", baseURL+index, nil)
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func parseQuery(query string) map[string]float32 {
	fields := strings.Split(query, ",")
	coefs := make(map[string]float32)
	for _, f := range fields {
		fieldparts := strings.Split(f, "=")
		if len(fieldparts) != 2 {
			log.Fatalf("ERROR: malformed query\n")
		}
		val, _ := strconv.ParseFloat(fieldparts[1], 32)
		coefs[fieldparts[0]] = float32(val)
	}
	return coefs
}

func executeQuery(index string, numResults int, coefs map[string]float32) {
	var scorefactors bytes.Buffer
	first := true
	for key, val := range coefs {
		if !first {
			scorefactors.WriteString(",")
		} else {
			first = false
		}
		scorefactors.WriteString(fmt.Sprintf(`{"field_value_factor":{"field":"%s","factor":%f}}`, key, val))
	}
	data := fmt.Sprintf(`{
    "size":%d,
    "query":{
      "function_score":{
        "functions":[%s],
        "score_mode": "sum"
      }
    }
  }`, numResults, scorefactors.String())
	resp, err := http.Post(baseURL+index+"/external/_search?pretty", "application/json", strings.NewReader(data))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func loadElasticData(index string, data io.Reader) {
	resp, err := http.Post(baseURL+index+"/external/_bulk", "application/json", data)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}

func load(csvfile string) {

	csvReader, err := os.Open(csvfile)
	if err != nil {
		log.Fatalf("ERROR: failed to open csvfile: %s\n", err)
	}
	csvScanner := csv.NewReader(csvReader)
	cols, _ := csvScanner.Read()
	var data []string
	data, err = csvScanner.Read()
	var jsonbuf bytes.Buffer
	batchRows := 0
	rows := 0
	for ; err == nil; data, err = csvScanner.Read() {
		for i, col := range cols {
			if i == 0 {
				jsonbuf.WriteString(`{"index":{"_id":"` + data[i] + "\"}}\n{")
			} else {
				if i != 1 {
					jsonbuf.WriteString(",")
				}
				val, _ := strconv.Atoi(data[i])
				jsonbuf.WriteString("\"" + col + "\":" + strconv.Itoa(val))
			}
		}
		jsonbuf.WriteString("}\n")
		batchRows++
		if batchRows >= batchSize {
			fmt.Printf("loading rows %d to %d\n", rows, rows+batchRows)
			rows += batchRows
			loadElasticData(dbname, strings.NewReader(jsonbuf.String()))
			jsonbuf.Truncate(0)
			batchRows = 0
		}
	}
	if batchRows > 0 {
		fmt.Printf("loading rows %d to %d\n", rows, rows+batchRows)
		loadElasticData(dbname, strings.NewReader(jsonbuf.String()))
	}
}

// // BulkIndex from Db interface
// func BulkIndex(records []map[string]float32) []int64 {
// 	return []int64{}
// }
//
// // Index from Db interface
// func Index(record map[string]float32) int64 {
// 	return 0
// }

// Query from Db interface
func Query(numResults int, weights map[string]float32) []int64 {
	executeQuery(dbname, numResults, weights)
	return []int64{}
}

func main() {
	flag.Parse()
	if *deleteflag {
		deleteElasticData(dbname)
	} else if len(*loadflag) > 0 {
		load(*loadflag)
	} else if len(*queryflag) > 0 {
		coefs := parseQuery(*queryflag)
		executeQuery("scoredb", 10, coefs)
	} else {
		fmt.Println("need to use --load filename, --query querystring, or --delete")
	}
}
