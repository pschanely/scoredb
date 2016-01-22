package scoredb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"time"
)

func NewFsScoreDb(dataDir string) *FsScoreDb {
	err := EnsureDirectory(dataDir)
	if err != nil {
		panic(err)
	}
	fields := make(map[string]OrderedFileInfos)

	// Load pre-existing file headers
	fieldNames, err := ioutil.ReadDir(dataDir)
	if err != nil {
		panic(err)
	}
	for _, fieldName := range fieldNames {
		fieldPath := path.Join(dataDir, fieldName.Name())
		fmt.Printf(" L! %v\n", fieldPath)
		fields[fieldPath] = make(OrderedFileInfos, 0)
		dataFiles, err := ioutil.ReadDir(fieldPath)
		if err != nil {
			panic(err)
		}
		for _, dataFile := range dataFiles {
			numVarBits := 32 - len(dataFile.Name())
			prefixVal, err := strconv.ParseInt(dataFile.Name(), 2, 32)
			if err != nil {
				continue
			}
			
			dataFilePath := path.Join(fieldPath, dataFile.Name())
			fmt.Printf(" L2 %v\n", dataFilePath)
			fd, err := os.OpenFile(dataFilePath, os.O_RDONLY, 0)
			if err != nil {
				panic(err)
			}
			var header PostingListHeader
			err = binary.Read(fd, binary.LittleEndian, &header)
			if err != nil {
				panic(err)
			}
			fileInfo := &FileInfo{
				header:          &header,
				path:            dataFilePath,
				numVariableBits: uint(numVarBits),
				minVal:          math.Float32frombits(uint32(prefixVal << uint(numVarBits))),
			}
			fmt.Printf(" f i %+v\n", fileInfo)
			fields[fieldName.Name()] = append(fields[fieldName.Name()], fileInfo)
		}
		
	}

	return &FsScoreDb{
		dataDir: dataDir,
		fields: fields,
		nextId:  1,
	}
}

type FsScoreDb struct {
	dataDir string
	fields  map[string]OrderedFileInfos
	nextId  int64
}

type PostingListHeader struct {
	FirstDocId    int64
	LastDocId     int64
	NumDocs       int64
	MinVal        float32
	MaxVal        float32
	FirstDocScore float32
	Version       uint8
	// padding to make struct 8-byte aligned when using encoding/binary operations:
	_             uint8
	_             uint16
}

type FileInfo struct {
	header          *PostingListHeader
	writer          *BitWriter
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
	return 20*1568 + (1 << (fixedFractionBits))
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func EnsureDirectory(dir string) error {
	if Exists(dir) {
		return nil
	} else {
		parent := path.Dir(dir)
		EnsureDirectory(parent)
		return os.Mkdir(dir, 0755)
	}
}

var INITIAL_VAR_BITS = uint(23 - 0)
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
		var header PostingListHeader
		err = binary.Read(fd, binary.LittleEndian, &header)
		if err != nil {
			return nil, err
		}
		fileInfo.header = &header
		writer, err := NewBitWriter(fd)
		if err != nil {
			return nil, err
		}
		fileInfo.writer = writer
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
	writer, err := NewBitWriter(fd)
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		header:          &header,
		writer:          writer,
		path:            filename,
		numVariableBits: numVarBits,
		minVal:          minVal,
	}, nil
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

	if scoreRemainder == 0 {
		fileInfo.writer.WriteVarUInt32(uint32(docIncr << 1))
	} else {
		fileInfo.writer.WriteVarUInt32(uint32((docIncr << 1) | 1))
		fileInfo.writer.WriteBits(scoreRemainder, fileInfo.numVariableBits)
	}

}

func (op *PostingListDocItr) Close() {
	if op.reader != nil {
		numOpenFiles -= 1
		err := op.reader.Close()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
	}
}

func (op *PostingListDocItr) Next(minId int64) bool {
	reader := op.reader
	if reader == nil {
		if op.docId == -1 && minId <= op.header.FirstDocId {
			op.docId = op.header.FirstDocId
			op.score = op.header.FirstDocScore
			return true
		} else {
			fmt.Printf("%08d Open       @doc %08d %s\n", time.Now().UnixNano() % 100000000, minId, op.path)
			fd, err := os.OpenFile(op.path, os.O_RDONLY, 0)
			numOpenFiles += 1
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			_, err = fd.Seek(HEADER_SIZE, 0)
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			reader, err = NewBitReader(fd)
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
			op.reader = reader
		}
	}
	docId := op.docId
	for {
		if docId == op.maxDocId {
			return false
		}
		pair, err := reader.ReadVarUInt32()
		if err != nil {
			panic(fmt.Sprintf("%v", err))
		}
		docIncr := pair >> 1
		var valueBits uint64
		if pair & 1 == 1 {
			valueBits, err = reader.ReadBits(op.numVarBits)
			if err != nil {
				panic(fmt.Sprintf("%v", err))
			}
		}
		if docIncr == 0 {
			panic(fmt.Sprintf("Inconsistent file data @ %v %v", reader.MmapPtr * 8, op.path))
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
			origPos, err := writer.File.Seek(0, 1) // save position to restore later
			if err != nil {
				return err
			}
			_, err = writer.File.Seek(0, 0)
			if err != nil {
				return err
			}
			err = binary.Write(writer.File, binary.LittleEndian, fileInfo.header)
			if err != nil {
				return err
			}
			_, err = writer.File.Seek(origPos, 0)
			if err != nil {
				return err
			}
			err = writer.Close()
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

func (db *FsScoreDb) FieldDocItr(fieldName string) DocItr {
	files, ok := db.fields[fieldName]
	fmt.Printf(" field %v in %+v\n", fieldName, db.fields)
	if ! ok {
		return NewMemoryScoreDocItr([]float32{})
	}
	itrs := make([]DocItr, len(files))
	for fileIdx, fileInfo := range files {
		itrs[fileIdx] = NewPostingListDocItr(math.Float32bits(fileInfo.minVal), fileInfo.path, fileInfo.header, fileInfo.numVariableBits)
	}
	return NewFieldDocItr(fieldName, itrs)
}

type PostingListDocItr struct {
	score       float32
	docId       int64
	maxDocId    int64
	min, max    float32
	numVarBits  uint
	rangePrefix uint32
	path        string
	reader      *BitReader
	header      *PostingListHeader
}

func NewPostingListDocItr(rangePrefix uint32, path string, header *PostingListHeader, numVarBits uint) DocItr {
	itr := &PostingListDocItr{
		score:       0.0,
		docId:       -1,
		maxDocId:    header.LastDocId,
		min:         header.MinVal,
		max:         header.MaxVal,
		numVarBits:  numVarBits,
		rangePrefix: rangePrefix,
		path:        path,
		header:      header,
	}
	return itr
}

func (op *PostingListDocItr) Name() string { return "PostingListDocItr" }
func (op *PostingListDocItr) Cur() (int64, float32) {
	return op.docId, op.score
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
