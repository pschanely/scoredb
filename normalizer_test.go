package main

import (
	"fmt"
	"testing"
	)

type samplerecord map[string]float32

var sample1 []samplerecord = []samplerecord {
	samplerecord { "x":1, "Y":2, "Z":3},
	samplerecord { "x":2, "Y":3, "Z":4},
	samplerecord { "x":3, "Y":4, "Z":5},	
}

type normcase struct {
	input []samplerecord
	output [3]float32
}

func TestNormalizer(t *testing.T) {
	cases := []normcase {
		{
		input: sample1,
		output: [3]float32 {-1,-1,-1,},
		},
	}
	for _, c := range cases {
		fmt.Println(c)
		// normalizer := Normalizer
		// for _, sample := range c.input {
			// normalizer.SetSum()
		// }
		// got := Elim(c.input.eqmap, c.input.bval, c.input.boundvals)
		// fmt.Println("Results")
		// fmt.Println(got)
		// fmt.Println("Expect:")
		// fmt.Println(c.elimvals)
		// if compareMaps(got, c.elimvals) {
		// 	t.Errorf("Not equal!")
		// } else {
		// 	fmt.Println("Test success!")
		// }
	}
}