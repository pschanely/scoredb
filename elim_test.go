package main

import (
	"testing"
	"fmt"
	)

// (eqmap map[string]float32, bval int, boundvals map[string][2]float32) (elimvals map[string][2]float32)

func TestElimination(t *testing.T) {
	cases := []struct {
		input struct {
			eqmap map[string]float32
			bval float32
			boundvals map[string][2]float32
		} 
		elimvals map[string][2]float32
	}{
		{
			{
				eqmap: map[string]float32 { 
					"X":1,
					"Y":2,
					"Z":3,
					},
				bval: 0.7,
				boundvals: map[string][2]float32 {
					"X":{0,100},
					"Y":{50,200},
					"Z":{20,30},
				},
			}, 
			map[string][2]float32 {
				"X":{0,100},
				"Y":{50,200},
				"Z":{20,30},
				},
		},
	}
	for _, c := range cases {
		fmt.Println(c.input)
		fmt.Println(c.elimvals)
		// got := Reverse(c.in)
		// if got != c.want {
		// 	t.Errorf("Reverse(%q) == %q, want %q", c.in, got, c.want)
		// }
	}
}