package scoredb

import (
	"math"
)

var PositiveInfinity = float32(math.Inf(1))
var NegativeInfinity = float32(math.Inf(-1))

type DocItr interface {
	// An iterator over (document id, score) values.

	Name() string

	// return false if the iterator is now known to not produce any more values
	SetBounds(min, max float32) bool

	GetBounds() (min, max float32)

	// Next() skips the iterator ahead to at least as far as the given id.
	// It always advances the iterator at least one position.
	// It Returns false if there are no remaining values.
	// Iterators need a call to Next(0) to intialize them to a real value; they all initially have a docId of -1
	Next(minId int64) bool

	Close() // release resources held by this iterator (if any)

	Cur() (int64, float32) // doc id and score of current result, or (-1, 0.0) if the iterator has not been initialized

}
