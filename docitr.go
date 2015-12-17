package main

import (
)

type DocItr interface {
	// An iterator over (document id, score) values. It is initially not positioned; a call to Next() is required before this has a real value
	Name() string
	SetBounds(min, max float32) bool // return false if the iterator is now known to not produce any more values
	Next() bool // return true if there is another value
	DocId() int64 // doc id of current result; 
	Score() float32 // score of current result; 
}

