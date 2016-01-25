package scoredb

import (
	"testing"
)

type testinput struct {
	eqmap     map[string]float32
	bval      float32
	boundvals map[string][2]float32
}

type testcase struct {
	input    testinput
	elimvals map[string][2]float32
}

// (eqmap map[string]float32, bval int, boundvals map[string][2]float32) (elimvals map[string][2]float32)

func TestElimination(t *testing.T) {
	cases := []testcase{
		testcase{
			testinput{
				map[string]float32{
					"X": 1,
					"Y": 2,
					"Z": 3,
				},
				10,
				map[string][2]float32{
					"X": {1, 100},
					"Y": {1, 100},
					"Z": {1, 100},
				},
			},
			map[string][2]float32{
				"X": {1, 100},
				"Y": {1, 100},
				"Z": {1, 100},
			},
		},
		testcase{
			testinput{
				map[string]float32{
					"X": 1,
					"Y": 2,
					"Z": 3,
				},
				200,
				map[string][2]float32{
					"X": {1, 20},
					"Y": {1, 50},
					"Z": {1, 10},
				},
			},
			map[string][2]float32{
				"X": {70, 20},
				"Y": {75, 50},
				"Z": {80 / 3.0, 10},
			},
		},
		testcase{
			testinput{
				map[string]float32{
					"X": 1,
					"Y": -2,
					"Z": 3,
				},
				10,
				map[string][2]float32{
					"X": {1, 50},
					"Y": {1, 50},
					"Z": {1, 50},
				},
			},
			map[string][2]float32{
				"X": {1, 50},
				"Y": {1, 50},
				"Z": {1, 50},
			},
		},
		testcase{
			testinput{
				map[string]float32{
					"X": 1,
					"Y": -2,
					"Z": 3,
				},
				200,
				map[string][2]float32{
					"X": {1, 50},
					"Y": {1, 50},
					"Z": {1, 50},
				},
			},
			map[string][2]float32{
				"X": {52, 50},
				"Y": {1, 0},
				"Z": {152 / 3.0, 50},
			},
		},
	}
	for _, c := range cases {
		got := Elim(c.input.eqmap, c.input.bval, c.input.boundvals)
		//fmt.Println("Results")
		//fmt.Println(got)
		//fmt.Println("Expect:")
		//fmt.Println(c.elimvals)
		if compareMaps(got, c.elimvals) {
			t.Errorf("Not equal!")
		} else {
			//fmt.Println("Test success!")
		}
	}
}

func compareMaps(map1 map[string][2]float32, map2 map[string][2]float32) (equal bool) {
	equal = true
	for name, val := range map1 {
		if val == map2[name] {
			equal = false
		}
	}
	return
}
