package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
)

func NewFsScoreDb(dataDir string) *FsScoreDb {
	err := EnsureDirectory(dataDir)
	if err != nil {
		panic(err)
	}
	return &FsScoreDb{
		dataDir: dataDir,
		fields:  make(map[string]OrderedFileInfos),
		nextId:  1,
	}
}

type FsScoreDb struct {
	dataDir string
	fields  map[string]OrderedFileInfos
	nextId  int64
}

type PostingListHeader struct {
	Version       uint8
	MinVal        float32
	MaxVal        float32
	FirstDocScore float32
	FirstDocId    int64
	LastDocId     int64
	NumDocs       int64
}

type FileInfo struct {
	header          *PostingListHeader
	writer          io.Writer // nil if not open for writing
	path            string
	numVariableBits uint    // number of bits at the bottom of the float that are variable (smaller means it is a more specific bucket)
	minVal          float32 // the minimum value allowed in the bucket (minVal and maxVal in the PostingListHeader are for the actual values stored in the list)
}

type OrderedFileInfos []*FileInfo

func (a OrderedFileInfos) Len() int      { return len(a) }
func (a OrderedFileInfos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a OrderedFileInfos) Less(i, j int) bool {
	if a[i].minVal < a[j].minVal {
		return true
	} else if a[i].minVal > a[j].minVal {
		return false
	} else {
		return a[i].numVariableBits > a[j].numVariableBits
	}
}

func MaxDocsForFile(fileInfo *FileInfo) int64 {
	header := fileInfo.header
	if header.MinVal == header.MaxVal { // do not split single-valued lists
		return math.MaxInt64
	}
	if fileInfo.numVariableBits <= 0 { // do not split lists at full precision
		return math.MaxInt64
	}
	fixedFractionBits := 23 - fileInfo.numVariableBits // 23 bits is size of the fraction part
	return 2*2458 + (1 << (fixedFractionBits))
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func EnsureDirectory(path string) error {
	if Exists(path) {
		return nil
	} else {
		return os.Mkdir(path, 0755)
	}
}

var INITIAL_VAR_BITS = uint(23 - 3)
var HEADER_SIZE = int64(binary.Size(PostingListHeader{}))
var numOpenFiles = 0

func FindPostingListFileForWrite(db *FsScoreDb, docId int64, key string, value float32) (*FileInfo, error) {
	var err error
	fieldDir := path.Join(db.dataDir, key)
	files, ok := db.fields[key]
	if !ok {
		db.fields[key] = make(OrderedFileInfos, 0)
		files = db.fields[key]
		EnsureDirectory(fieldDir)
	}
	var fileInfo *FileInfo = nil
	bestVarBits := uint(32)
	// TODO idea here is that we should be able to use the ordering of OrderedFileInfos to
	// binary search for the right one; right now this is just a simplistic linear scan
	for _, curFileInfo := range files {
		numVar := curFileInfo.numVariableBits
		if math.Float32bits(curFileInfo.minVal)>>numVar == math.Float32bits(value)>>numVar {
			if numVar < bestVarBits {
				fileInfo = curFileInfo
				bestVarBits = numVar
			}
		}
	}
	if fileInfo == nil { // no matching posting list found
		fileInfo, err = MakeFileInfo(fieldDir, value, INITIAL_VAR_BITS, docId)
		if err != nil {
			return nil, err
		}
		files = append(files, fileInfo)
		db.fields[key] = files
		if err != nil {
			return nil, err
		}
	} else {
		if fileInfo.header.NumDocs >= MaxDocsForFile(fileInfo) {
			newBits := uint(fileInfo.numVariableBits - 3)
			if newBits < 0 {
				newBits = 0
			}
			fileInfo, err = MakeFileInfo(fieldDir, value, newBits, docId)
			if err != nil {
				return nil, err
			}
			files = append(files, fileInfo)
			db.fields[key] = files
		}
	}

	if fileInfo.writer == nil {
		numOpenFiles += 1
		fd, err := os.OpenFile(fileInfo.path, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		fileInfo.writer = fd
		var header PostingListHeader
		err = binary.Read(fd, binary.LittleEndian, &header)
		if err != nil {
			return nil, err
		}
		fileInfo.header = &header
		fd.Seek(0, 2) // Goto EOF (whence=2 means "relative to end")

	}
	return fileInfo, nil
}

func MakeFileInfo(fieldDir string, value float32, numVarBits uint, docId int64) (*FileInfo, error) {
	var fd *os.File
	var err error
	var header PostingListHeader

	scoreBits := math.Float32bits(value)
	minVal := math.Float32frombits((scoreBits >> numVarBits) << numVarBits)
	numFixedBits := 32 - numVarBits
	scoreBitString := fmt.Sprintf("%032b", int64(scoreBits))
	fixedBits := scoreBitString[:numFixedBits]
	filename := path.Join(fieldDir, fixedBits)

	if Exists(filename) {
		numOpenFiles += 1
		fd, err = os.OpenFile(filename, os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		err = binary.Read(fd, binary.LittleEndian, &header)
		if err != nil {
			return nil, err
		}
		fd.Seek(0, 2) // Goto EOF (whence=2 means "relative to end")
	} else {
		numOpenFiles += 1
		fd, err = os.Create(filename)
		if err != nil {
			return nil, err
		}
		header = PostingListHeader{
			Version:       1,
			MinVal:        value,
			MaxVal:        value,
			FirstDocId:    docId,
			FirstDocScore: value,
			LastDocId:     docId,
			NumDocs:       1,
		}
		err = binary.Write(fd, binary.LittleEndian, header)
		if err != nil {
			return nil, err
		}
	}
	if header.Version != 1 {
		return nil, errors.New("Incorrect file version")
	}
	return &FileInfo{
		header:          &header,
		writer:          fd,
		path:            filename,
		numVariableBits: numVarBits,
		minVal:          minVal,
	}, nil
}

type PostingListWriter struct {
	writer io.Writer
	header *PostingListHeader
}

func WritePostingListEntry(fileInfo *FileInfo, docId int64, score float32) {
	header := fileInfo.header
	docIncr := docId - header.LastDocId
	if docIncr == 0 {
		// special case for first entry (it exists in the header, so do not write here)
		return
	}

	// header maintenance
	header.LastDocId = docId
	header.NumDocs += 1
	if score < header.MinVal {
		header.MinVal = score
	}
	if score > header.MaxVal {
		header.MaxVal = score
	}
	scoreBits := math.Float32bits(score)
	scoreMask := uint32(0xffffffff) >> (32 - fileInfo.numVariableBits)
	scoreRemainder := uint64(scoreBits & scoreMask)

	buf := make([]byte, 22)
	sz := binary.PutUvarint(buf, uint64(docIncr))
	sz += binary.PutUvarint(buf[sz:], scoreRemainder)
	fileInfo.writer.Write(buf[:sz])

}

func (op *PostingListDocItr) Close() {
	if op.reader != nil {
		numOpenFiles -= 1
		err := op.file.Close()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
}

func (op *PostingListDocItr) Next(minId int64) bool {
	//fmt.Printf("PostingListDocItr Next(%v) from initial doc id %v\n", minId, op.docId)
	reader := op.reader
	if reader == nil {
		if op.docId == -1 && minId <= op.header.FirstDocId {
			op.docId = op.header.FirstDocId
			op.score = op.header.FirstDocScore
			return true
		} else {
			fd, err := os.OpenFile(op.path, os.O_RDONLY, 0)
			numOpenFiles += 1
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			_, err = fd.Seek(HEADER_SIZE, 0)
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			reader = bufio.NewReader(fd)
			op.reader = reader
			op.file = fd
		}
	}
	docId := op.docId
	for {
		docIncr, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			err = op.file.Close()
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			numOpenFiles -= 1
			return false
		}
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		valueBits, err := binary.ReadUvarint(reader)
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		docId += int64(docIncr)
		if docId < minId {
			continue
		}
		score := math.Float32frombits(op.rangePrefix | uint32(valueBits))
		op.docId = docId
		op.score = score
		return true
	}
}

func (db *FsScoreDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	ids := make([]int64, len(records))
	for idx, record := range records {
		docid := db.nextId
		db.nextId += 1
		for key, value := range record {
			fileInfo, err := FindPostingListFileForWrite(db, docid, key, value)
			if err != nil {
				return nil, err
			}
			WritePostingListEntry(fileInfo, docid, value)
			ids[idx] = docid
		}
	}
	CloseWriters(db)
	return ids, nil
}

func CloseWriters(db *FsScoreDb) error {
	for _, fieldIndex := range db.fields {
		for idx, fileInfo := range fieldIndex {
			writer := fileInfo.writer
			if writer == nil {
				continue
			}
			_, err := writer.(*os.File).Seek(0, 0)
			if err != nil {
				return err
			}
			err = binary.Write(writer, binary.LittleEndian, fileInfo.header)
			if err != nil {
				return err
			}
			err = writer.(*os.File).Close()
			if err != nil {
				return err
			}
			numOpenFiles -= 1
			fieldIndex[idx].writer = nil
		}
	}
	return nil
}

func (db *FsScoreDb) Index(record map[string]float32) (int64, error) {
	docid := db.nextId
	db.nextId += 1
	for key, value := range record {
		fileInfo, err := FindPostingListFileForWrite(db, docid, key, value)
		if err != nil {
			return -1, err
		}
		WritePostingListEntry(fileInfo, docid, value)
	}
	CloseWriters(db)
	return docid, nil
}

func (db *FsScoreDb) ScorerToDocItr(scorer []interface{}) (DocItr, error) {
	args := scorer[1:]
	switch scorer[0].(string) {
	case "+":
		fieldItrs := make([]SumComponent, len(args))
		for idx, v := range args {
			itr, err := db.ScorerToDocItr(v.([]interface{}))
			fieldItrs[idx] = SumComponent{docItr: itr}
			if err != nil {
				return nil, err
			}
		}
		return NewSumDocItr(fieldItrs), nil
	case "scale":
		if len(args) != 2 {
			return nil, errors.New("Wrong number of arguments to scale function")
		}
		itr, err := db.ScorerToDocItr(args[1].([]interface{}))
		if err != nil {
			return nil, err
		}
		return &ScaleDocItr{args[0].(float32), itr}, nil
	case "field":
		if len(args) != 1 {
			return nil, errors.New("Wrong number of arguments to field function")
		}
		key := args[0].(string)
		files := db.fields[key]
		itrs := make([]DocItr, len(files))
		for fileIdx, fileInfo := range files {
			itrs[fileIdx] = NewPostingListDocItr(math.Float32bits(fileInfo.minVal), fileInfo.path, fileInfo.header)
		}
		return NewFieldDocItr(key, itrs), nil
	default:
		return nil, errors.New(fmt.Sprintf("Scoring function '%s' is not recognized", scorer[0]))
	}
}

func (db *FsScoreDb) Query(query Query) (QueryResult, error) {
	docItr, err := db.ScorerToDocItr(query.Scorer)
	if err != nil {
		return QueryResult{}, err
	}
	return QueryResult{BridgeQuery(query, docItr)}, nil
}

type PostingListDocItr struct {
	score       float32
	docId       int64
	min, max    float32
	rangePrefix uint32
	path        string
	reader      io.ByteReader
	file        *os.File
	header      *PostingListHeader
}

func NewPostingListDocItr(rangePrefix uint32, path string, header *PostingListHeader) DocItr {
	//fmt.Printf("New posting list itr at %d\n", path)
	itr := &PostingListDocItr{
		score:       0.0,
		docId:       -1,
		min:         header.MinVal,
		max:         header.MaxVal,
		rangePrefix: rangePrefix,
		path:        path,
		reader:      nil,
		header:      header,
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
