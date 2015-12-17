package main

import (
	"fmt"
)

func main() {
	fmt.Println("starting scoredb")
	l1 := NewMemoryDocItr(
		[]float32{1.0, 1.0, 0.5, 1.0, 0.5},
		[]int64{1, 5, 7, 8, 9},
	)
	fmt.Printf("Next = %s\n", l1.Next())
}
