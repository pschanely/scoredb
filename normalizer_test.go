package main

import (
	"fmt"
	"testing"
)

type samplerecord map[string]float32

var sample1 []samplerecord = []samplerecord{
	samplerecord{"X": 1, "Y": 2, "Z": 3},
	samplerecord{"X": 2, "Y": 3, "Z": 4},
	samplerecord{"X": 3, "Y": 4, "Z": 5},
}

type normcase struct {
	input  []samplerecord
	output map[string]float32
}

func TestNormalizer(t *testing.T) {
	cases := []normcase{
		{
			input:  sample1,
			output: map[string]float32{"X": -1.2247448, "Y": -1.2247448, "Z": -1.2247448},
		},
	}
	fmt.Println("Testing normalizer\n")
	for _, c := range cases {
		var normalizer Normalizer
		normalizer.Init()
		for _, sample := range c.input {
			normalizer.SetMean(sample)
		}
		for _, sample := range c.input {
			normalizer.SetStdev(sample)
		}
		for fieldName, val := range c.output {
			if result := normalizer.NormalizeValue(fieldName, c.input[0][fieldName]); result == val {
				if rescaled := normalizer.ScaleValue(fieldName, result); rescaled == c.input[0][fieldName] {
					fmt.Println("Test success!\n")
				} else {
					t.Errorf("Rescaler gave unexpected value: %v\n", result)
				}
			} else {
				t.Errorf("Normalizer gave unexpected value: %v\n", result)
			}
		}
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
