package scoredb

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"unsafe"
	"github.com/edsrzf/mmap-go"
)

type BitWriter struct {
	BufferedWriter *bufio.Writer
	File *os.File
	Cur uint64
	CurBitsUsed uint
}

func FileIsAtEnd(file *os.File) bool {
	stat, _ := file.Stat()
	pos, _ := file.Seek(0, 1)
	return pos == stat.Size()
}

func WriteNativeLong(val uint64, writer io.Writer) error {
	byteSlice := (*((*[8]byte)(unsafe.Pointer(&val))))[:]
	_, err := writer.Write(byteSlice)
	return err
}

func ReadNativeLong(buf []byte) uint64 {
	return *((*uint64)(unsafe.Pointer(&buf[0])))
}


func NewBitWriter(file *os.File) (*BitWriter, error) {	
	writer := BitWriter{File: file}
	if ! FileIsAtEnd(file) {
		buf := make([]byte, 16)
		
		file.Seek(-16, 2) // Goto EOF (whence=2 means "relative to end")
		nRead, err := file.Read(buf)
		if nRead != 16 {
			return nil, err
		}
		writer.CurBitsUsed = uint(ReadNativeLong(buf[8:]))
		writer.Cur =  ReadNativeLong(buf) >> (64 - writer.CurBitsUsed)

		file.Seek(-16, 2) // Goto EOF (whence=2 means "relative to end")
	}
	writer.BufferedWriter = bufio.NewWriter(file)
	return &writer, nil
}

func (writer *BitWriter) Close() error {
	bitsUsed := writer.CurBitsUsed
	WriteNativeLong(writer.Cur << (64 - bitsUsed), writer.BufferedWriter)
	WriteNativeLong(uint64(bitsUsed), writer.BufferedWriter)
	err := writer.BufferedWriter.Flush()
	if err != nil {
		return err
	}
	return writer.File.Close()
}

func (writer *BitWriter) WriteBits(val uint64, numBits uint) error { // assumes val is all zeros above numBits
	cur, bitsUsed := writer.Cur, writer.CurBitsUsed
	overflow := int(bitsUsed + numBits) - 64
	if overflow >= 0 { // split the write
		initialBits := numBits - uint(overflow)
		cur = (cur << initialBits) | (val >> uint(overflow))
		err := WriteNativeLong(cur, writer.BufferedWriter)
		if err != nil {
			return err
		}
		writer.Cur = val
		writer.CurBitsUsed = uint(overflow)
	} else {
		writer.Cur = (cur << numBits) | val
		writer.CurBitsUsed += numBits
	}
	return nil
}

func (writer *BitWriter) WriteVarUInt32(val uint32) error {
	var sizeFactor uint64
	if        val & 0xfffffff0 == 0 {
		sizeFactor = 0
	} else if val & 0xffffff00 == 0 {
		sizeFactor = 1
	} else if val & 0xffff0000 == 0 {
		sizeFactor = 2
	} else {
		sizeFactor = 3
	}
	writer.WriteBits(sizeFactor, 2)
	numBits := uint(4 << sizeFactor)
	writer.WriteBits(uint64(val), numBits)
	return nil
}



type BitReader struct {
	OrigMmap *mmap.MMap
	Mmap []uint64
	MmapPtr uint
	MmapPtrBitsLeft uint
	File *os.File
	Cur uint64
	CurBitsLeft uint
}

func NewBitReader(file *os.File) (*BitReader, error) {	
	mapSlice, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	curPos, err := file.Seek(0, 1)
	if curPos % 8 != 0 {
		panic(fmt.Sprintf("BitReader started at byte %v; must be 8 byte aligned", curPos))
	}
	return &BitReader{
		File: file,
		OrigMmap: &mapSlice,
		Mmap: (*((*[10000000]uint64)(unsafe.Pointer(&mapSlice[0]))))[:],
		MmapPtr: uint(curPos / 8),
		MmapPtrBitsLeft: 64,
	}, nil
}

func (reader *BitReader) Close() error {
	reader.Mmap = []uint64{}
	err := reader.OrigMmap.Unmap()
	if err != nil {
		return err
	}
	return reader.File.Close()
}

func (reader *BitReader) Refill(cur uint64, bitsLeft uint, numNeeded uint) (uint64, uint, error) {
	wanted := 64 - bitsLeft
	if wanted >= reader.MmapPtrBitsLeft { 
		bits := reader.Mmap[reader.MmapPtr] << (64 - reader.MmapPtrBitsLeft)
		cur = cur | (bits >> bitsLeft)
		bitsLeft += reader.MmapPtrBitsLeft
		wanted -= reader.MmapPtrBitsLeft
		reader.MmapPtrBitsLeft = 64
		reader.MmapPtr += 1
		if wanted == 0 {
			return cur, bitsLeft, nil
		}
	}
	bits := reader.Mmap[reader.MmapPtr] << (64 - reader.MmapPtrBitsLeft)
	cur = cur | (bits >> bitsLeft)
	reader.MmapPtrBitsLeft -= wanted
	bitsLeft = 64
	return cur, bitsLeft, nil
}
		
func (reader *BitReader) ReadBits(numBits uint) (uint64, error) {
	cur, bitsLeft := reader.Cur, reader.CurBitsLeft
	var err error
	if bitsLeft < numBits {
		cur, bitsLeft, err = reader.Refill(cur, bitsLeft, numBits)
		if err != nil {
			return 0, err
		}
	}
	val := cur >> (64 - numBits)
	cur = cur << numBits
	bitsLeft -= numBits
	reader.Cur, reader.CurBitsLeft = cur, bitsLeft
	return val, nil
}

func (reader *BitReader) ReadVarUInt32() (uint32, error) {
	sizeFactor, err := reader.ReadBits(2)
	if err != nil {
		return 0, err
	}
	numNeeded := uint(4 << sizeFactor)
	val, err := reader.ReadBits(numNeeded)
	if err != nil {
		return 0, err
	}
        return uint32(val), nil                                                                                                                          }
