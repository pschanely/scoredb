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
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func FindPostingListFile(dataDir string, key string, value float32) string {
	scoreBits := math.Float32bits(value)
	if !Exists(dataDir) {
		os.Mkdir(dataDir, 0755)
	}
	fieldDir := dataDir + "/" + key
	if !Exists(fieldDir) {
		os.Mkdir(fieldDir, 0755)
	}
	return fieldDir + "/" + fmt.Sprintf("%04x", scoreBits>>16)
}

func OpenPostingList(filename string, value float32) (io.Writer, error) {
	var fd *os.File
	var err error
	if Exists(filename) {
		fd, err = os.OpenFile(filename, os.O_RDWR | os.O_APPEND, 0666)
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
		rangePrefix = uint32(bytes[0])<<24 | uint32(bytes[1])<<16
		results[idx] = NewPostingListDocItr(reader, rangePrefix)
	}
	return results, nil
}

func (db *FsScoreDb) BulkIndex(records []map[string]float32) []int64 {
	dataDir := db.dataDir
	fds := make(map[string]io.Writer)
	ids := make([]int64, len(records))
	for idx, record := range records {
		docid := db.nextId
		db.nextId += 1
		for key, value := range record {
			filename := FindPostingListFile(dataDir, key, value)
			_, ok := fds[filename]
			if ! ok {
				fd, err := OpenPostingList(filename, value)
				if err != nil {
					panic(fmt.Sprintf("%v", err))
				}
				fds[filename] = fd
			}
			WritePostingListEntry(fds[filename], docid, value)
			ids[idx] = docid
		}
	}
	for _, fd := range(fds) {
		fd.(*os.File).Close()
	}
	return ids
}

func (db *FsScoreDb) Index(record map[string]float32) int64 {
	dataDir := db.dataDir

	docid := db.nextId
	db.nextId += 1
	for key, value := range record {
		filename := FindPostingListFile(dataDir, key, value)
		fd, err := OpenPostingList(filename, value)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		WritePostingListEntry(fd, docid, value)
		
		fd.(*os.File).Close()
	}
	return docid
}

func (db *FsScoreDb) Query(numResults int, weights map[string]float32) []int64 {
	fieldItrs := make([]LinearComponent, len(weights))
	idx := 0
	for key, weight := range weights {
		itrs, err := OpenPostingListRange(db.dataDir, key)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		fieldItrs[idx].docItr = NewFieldDocItr(key, itrs)
		fieldItrs[idx].coef = weight
		idx += 1
	}
	return BridgeQuery(numResults, weights, NewLinearDocItr(fieldItrs))
}

type PostingListDocItr struct {
	score    float32
	docId    int64
	min, max float32
	rangePrefix uint32
	reader      io.ByteReader
}

func NewPostingListDocItr(reader io.ByteReader, rangePrefix uint32) DocItr {
	itr := &PostingListDocItr{
		score:       0.0,
		docId:       -1,
		min:         float32(math.Inf(-1)),
		max:         float32(math.Inf(1)),
		rangePrefix: rangePrefix,
		reader:      reader,
	}
	bound1 := math.Float32frombits(rangePrefix | 0x0000)
	bound2 := math.Float32frombits(rangePrefix | 0xffff)
	if bound1 < bound2 {
		itr.min, itr.max = bound1, bound2
	} else {
		itr.min, itr.max = bound2, bound1
	}
	return itr
}

func (op *PostingListDocItr) Name() string { return "PostingListDocItr" }
func (op *PostingListDocItr) DocId() int64 {
	return op.docId
}
func (op *PostingListDocItr) Score() float32 {
	return op.score
}
func (op *PostingListDocItr) GetBounds() (min, max float32) {
	return op.min, op.max
}
func (op *PostingListDocItr) SetBounds(min, max float32) bool {
	if min > op.min {
		op.min = min
	}
	if max < op.max {
		op.max = max
	}
	if op.min > op.max {
		return false
	}
	return true
}
func (op *PostingListDocItr) Next(minId int64) bool {
	fd := op.reader
	for {
		docId, err := binary.ReadVarint(fd)
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
		if docId <  minId {
			continue
		}
		valueBits = op.rangePrefix | uint32(b1)<<8 | uint32(b2)
		score := math.Float32frombits(valueBits)
		op.docId = docId
		op.score = score
		return true
	}
}

func WritePostingListEntry(fd io.Writer, docIncr int64, score float32) {
	buf := make([]byte, 10+2)
	sz := binary.PutVarint(buf, docIncr)
	scoreBits := math.Float32bits(score)
	buf[sz+1] = byte((scoreBits >> 8) & 0xff)
	buf[sz+2] = byte(scoreBits & 0xff)
	fd.Write(buf[:sz+2])
}
