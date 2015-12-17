package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
)

type FsScoreDb struct {
	dataDir string
	nextId  int64
}

func Exists(path string) bool {
	_, err := os.Stat("/path/to/whatever")
	return !os.IsNotExist(err)
}

func OpenPostingList(dataDir string, key string, value float32) (io.Writer, error) {
	scoreBits := math.Float32bits(value)
	filename := dataDir + "/" + key + "/" + fmt.Sprintf("%#x", scoreBits>>16)[2:]
	//var mode int
	var fd *os.File
	var err error
	if Exists(filename) {
		fd, err = os.OpenFile(filename, os.O_APPEND, 0666)
	} else {
		fd, err = os.Create(filename)
	}
	if err != nil {
		return nil, err
	}
	return fd, err
}

func OpenPostingListRange(dataDir string, key string) ([]DocItr, error) {
	fieldDir := dataDir + "/" + key
	files, err := ioutil.ReadDir(fieldDir)
	if err != nil {
		return nil, err
	}

	results := make([]DocItr, len(files))
	for idx, fileInfo := range files {
		name := fileInfo.Name()
		fd, err := os.Open(fieldDir + "/" + name)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReader(fd)
		bytes, err := hex.DecodeString(name)
		if err != nil {
			return nil, err
		}
		var rangePrefix uint32
		rangePrefix = uint32(bytes[0])<<32 | uint32(bytes[1])<<24
		results[idx] = NewPostingListDocItr(reader, rangePrefix)
	}
	return results, nil
}

func (db *FsScoreDb) Index(record map[string]float32) int64 {
	dataDir := db.dataDir

	docid := db.nextId
	db.nextId += 1
	for key, value := range record {
		fd, err := OpenPostingList(dataDir, key, value)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		WritePostingListEntry(fd, docid, value)
		fd.(*os.File).Close()
	}
	return docid
}

func (db *FsScoreDb) Query(numResults int, weights map[string]float32) []int64 {
	fieldItrs := make([]DocItr, len(weights))
	idx := 0
	for key, _ := range weights {
		itrs, err := OpenPostingListRange(db.dataDir, key)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		// TODO weights!
		fieldItrs[idx] = NewFieldDocItr(itrs)
		idx += 1
	}
	// TODO LinearCombinationDocItr
	return BridgeQuery(numResults, weights, fieldItrs[0])
}

type PostingListDocItr struct {
	score    float32
	docId    int64
	min, max float32

	rangePrefix uint32
	reader      io.ByteReader
}

func NewPostingListDocItr(reader io.ByteReader, rangePrefix uint32) *PostingListDocItr {
	return &PostingListDocItr{
		score:       0.0,
		docId:       -1,
		min:         0.0,
		max:         1.0,
		rangePrefix: rangePrefix,
		reader:      reader,
	}
}

func (op *PostingListDocItr) Name() string { return "PostingListDocItr" }
func (op *PostingListDocItr) DocId() int64 {
	return op.docId
}
func (op *PostingListDocItr) Score() float32 {
	return op.score
}
func (op *PostingListDocItr) SetBounds(min, max float32) bool {
	op.min = min
	op.max = max
	return true
}
func (op *PostingListDocItr) Next() bool {
	fd := op.reader
	for {
		docIncr, err := binary.ReadVarint(fd)
		if err == io.EOF {
			return false
		}
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		var valueBits uint32
		b1, err := fd.ReadByte()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		b2, err := fd.ReadByte()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		valueBits = op.rangePrefix | uint32(b1)<<8 | uint32(b2)
		score := math.Float32frombits(valueBits)
		fmt.Printf("READ docincr: %v score: %v [%v:%v]\n", docIncr, score, op.min, op.max)
		op.docId = docIncr
		if op.min <= score && score <= op.max {
			op.score = score
			return true
		}
	}
}

func WritePostingListEntry(fd io.Writer, docIncr int64, score float32) {
	buf := make([]byte, 10+2)
	sz := binary.PutVarint(buf, docIncr)
	scoreBits := math.Float32bits(score)
	buf[sz+1] = byte((scoreBits >> 8) & 0xff)
	buf[sz+2] = byte(scoreBits & 0xff)
	fmt.Printf("write score %v -> %#x   buf: %v\n", score, scoreBits, buf[:sz+2])
	fd.Write(buf[:sz+2])
}
