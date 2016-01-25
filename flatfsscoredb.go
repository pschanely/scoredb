package scoredb

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path"
	"unsafe"
	"github.com/edsrzf/mmap-go"
)

type FlatFsScoreDb struct {
	dataDir string
	nextId  int64
}

func NewFlatFsScoreDb(dataDir string) *FlatFsScoreDb {
	err := EnsureDirectory(dataDir)
	if err != nil {
		panic(err)
	}
	return &FlatFsScoreDb{
		dataDir: dataDir,
		nextId: 1, // TODO load this from fs somehow.  also, detect new fields and do something about that
	}
}


func (db *FlatFsScoreDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	var err error
	//var bytes [4]byte
	ids := make([]int64, len(records))
	root := db.dataDir
	fds := make(map[string]*os.File)
	for idx, record := range records {
		ids[idx] = db.nextId
		db.nextId += 1
		for key, value := range record {
			var fd *os.File
			fd, exists := fds[key]
			if ! exists {
				filename := path.Join(root, key)
				if Exists(filename) {
					fd, err = os.OpenFile(filename, os.O_RDWR, 0666)
				} else {
					fd, err = os.Create(filename)
				}
				if err != nil {
					return nil, err
				}
				_, err = fd.Seek(0, 2) // Goto EOF (whence=2 means "relative to end")
				if err != nil {
					return nil, err
				}
				fds[key] = fd
			}
			//bits := math.Float32bits(value)
			//bytes[0] = byte(bits >> 24)
			//bytes[1] = byte(bits >> 16)
			//bytes[2] = byte(bits >> 8)
			//bytes[3] = byte(bits)
			bytesToWrite := (*((*[4]byte)(unsafe.Pointer(&value))))[:]
			_, err = fd.Write(bytesToWrite)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, fd := range fds {
		err = fd.Close()
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (db *FlatFsScoreDb) FieldDocItr(fieldName string) DocItr {
	root := db.dataDir
	fd, err := os.OpenFile(path.Join(root, fieldName), os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return NewMemoryDocItr([]float32{}, []int64{})
		}
		panic(fmt.Sprintf("%v", err))
	}
	stat, err := fd.Stat()
	if err != nil {
		panic(err)
	}
	mapSlice, err := mmap.Map(fd, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	return &FlatFieldItr{
		fd: fd,
		reader: bufio.NewReader(fd),
		mmap: (*((*[10000000]float32)(unsafe.Pointer(&mapSlice[0]))))[:],
		numDocs: int(stat.Size() / 4),
		docId: -1,
		min: 0,//float32(math.Inf(-1)), 
		max: float32(math.Inf(1)),
	}
}

type FlatFieldItr struct {
	fd *os.File
	reader *bufio.Reader
	mmap []float32
	numDocs int
	docId int64
	score float32
	min, max float32
}

func (op *FlatFieldItr) Name() string { return "FlatFieldDocItr" }
func (op *FlatFieldItr) Cur() (int64, float32) {
	return op.docId, op.score
}
func (op *FlatFieldItr) GetBounds() (min, max float32) {
	return op.min, op.max
}
func (op *FlatFieldItr) SetBounds(min, max float32) bool {
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

func (op *FlatFieldItr) Close() {
	op.fd.Close()
}

func (op *FlatFieldItr) Next(minId int64) bool {
	if minId == 0 { minId = 1}
	idx := int(minId - 1)
	if idx < op.numDocs {
		op.docId = minId
		op.score = op.mmap[idx]
		return true
	} else {
		return false
	}
	/*
	var bytes [4]byte
	byteSlice := bytes[:]
	for {
		if op.docId >= minId {
			break
		}
		_, err := op.reader.Read(byteSlice)
		if err != nil {
			if err == io.EOF {
				return false
			}
			panic(fmt.Sprintf("%v", err))
		}
		if op.docId == -1 { // start at 1   TODO: consider changing initial doc to zero
			op.docId = 1
		} else {
			op.docId += 1
		}
	}
	op.score = *((*float32)(unsafe.Pointer(&bytes[0])))
	//op.score = math.Float32frombits(uint32(bytes[0]) << 24 | uint32(bytes[1]) << 16 | uint32(bytes[2]) << 8 | uint32(bytes[3])) // portable way
	return true
*/
}


