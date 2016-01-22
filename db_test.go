package scoredb

import (
	"fmt"
	"testing"
)

func CallAndCheck(db Db, t *testing.T, r1 []int64, limit int, scorer []interface{}) {
	r2, err := db.Query(Query{Limit:limit, Scorer:scorer})
	fmt.Printf("Is? %v %v (err:%v)\n", r1, r2, err)
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
	fmt.Print(" =================== \n")
	_, err := db.Index(map[string]float32{"age": 32, "height": 2.0})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	_, err = db.Index(map[string]float32{"age": 25, "height": 1.5})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	_, err = db.Index(map[string]float32{"age": 16, "height": 2.5})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	fmt.Print(" =================== \n")
	CallAndCheck(db, t, []int64{3, 1}, 2, []interface{}{"field", "height"})		
	CallAndCheck(db, t, []int64{1, 2}, 2, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []int64{1}, 1, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []int64{3, 1}, 2, []interface{}{"sum", 
		[]interface{}{"scale", 0.1, []interface{}{"field", "age"}},
		[]interface{}{"scale", 10.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []int64{3, 2}, 2, []interface{}{"sum", 
		[]interface{}{"scale", -1.0, []interface{}{"field", "age"}}, 
		[]interface{}{"scale", -1.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []int64{2, 1, 3}, 3, []interface{}{"sum", 
		[]interface{}{"scale", 1.0, []interface{}{"field", "age"}}, 
		[]interface{}{"scale", -100.0, []interface{}{"field", "height"}}})
	CallAndCheck(db, t, []int64{}, 0, []interface{}{"sum", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})

	CallAndCheck(db, t, []int64{1, 3, 2}, 3, []interface{}{"product", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
	CallAndCheck(db, t, []int64{3, 1, 2}, 3, []interface{}{"min", 
		[]interface{}{"field", "age"}, 
		[]interface{}{"field", "height"}})
}

