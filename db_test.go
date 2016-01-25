package scoredb

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
)

func CallAndCheck(db Db, t *testing.T, r1 []string, limit int, scorer []interface{}) {
	r2, err := db.Query(Query{Limit:limit, Scorer:scorer})
	if (err != nil) {
		t.Fatal()
	}
	if len(r1) != len(r2.Ids) {
		t.Fatal()
	}
	for idx, v1 := range r1 {
		if v1 != r2.Ids[idx] {
			t.Fatal()
		}
	}
}

func DbBasicsTest(db Db, t *testing.T) {
	err := db.Index("r1", map[string]float32{"age": 32, "height": 2.0})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	err = db.Index("r2", map[string]float32{"age": 25, "height": 1.5})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	err = db.Index("r3", map[string]float32{"age": 16, "height": 2.5})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	CallAndCheck(db, t, []string{"r3", "r1"}, 2, []interface{}{"field", "height"})		
	CallAndCheck(db, t, []string{"r1", "r2"}, 2, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []string{"r1"}, 1, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []string{"r3", "r1"}, 2, []interface{}{"sum", 
		[]interface{}{"scale", 0.1, []interface{}{"field", "age"}},
		[]interface{}{"scale", 10.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []string{"r3", "r2"}, 2, []interface{}{"sum", 
		[]interface{}{"scale", -1.0, []interface{}{"field", "age"}}, 
		[]interface{}{"scale", -1.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []string{"r2", "r1", "r3"}, 3, []interface{}{"sum", 
		[]interface{}{"scale", 1.0, []interface{}{"field", "age"}}, 
		[]interface{}{"scale", -100.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []string{}, 0, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []string{"r1", "r3", "r2"}, 3, []interface{}{"product", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []string{"r3", "r1", "r2"}, 3, []interface{}{"min", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
}

func RmAllTestData() (func(name string) string) {
	tmpDir := os.TempDir()
	dirfd, err := os.Open(tmpDir)
	if err == nil {
		names, err := dirfd.Readdirnames(0)
		if err == nil {
			for _, name := range names {
				if strings.HasPrefix(name, "scoredbtest.") {
					os.RemoveAll(path.Join(tmpDir, name))
				}
			}
		}
	}
	return func(name string) string {
		fullname := path.Join(tmpDir, "scoredbtest." + name)
		return fullname
	}
}
