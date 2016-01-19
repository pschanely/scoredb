package main

import (
	"bufio"
	//"fmt"
	"io"
	"os"
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
	//fmt.Printf("File is at end ? %v %v\n", pos, stat.Size())
	return pos == stat.Size()
}

func NewBitWriter(file *os.File) (*BitWriter, error) {	
	writer := BitWriter{File: file, BufferedWriter: bufio.NewWriter(file)}
	if ! FileIsAtEnd(file) {
		buf := make([]byte, 1)

		file.Seek(-1, 2) // Goto EOF (whence=2 means "relative to end")
		nRead, err := file.Read(buf)
		if nRead != 1 {
			return nil, err
		}
		paddingBits := uint(buf[0])

		file.Seek(-2, 2) // Goto EOF (whence=2 means "relative to end")
		nRead, err = file.Read(buf)
		if nRead != 1 {
			return nil, err
		}
		writer.Cur = uint64(buf[0]) >> paddingBits
		writer.CurBitsUsed = 8 - paddingBits

		file.Seek(-2, 2) // Goto EOF (whence=2 means "relative to end")
	}
	//fmt.Printf("OPEN %064b %v\n", writer.Cur, writer.CurBitsUsed)
	return &writer, nil
	
}

func (writer *BitWriter) Close() error {
	// fill out last byte and flush file
	cur, bitsUsed := writer.Cur, writer.CurBitsUsed
	paddingBits := 8 - bitsUsed
	writer.BufferedWriter.WriteByte(byte(cur << paddingBits))
	writer.BufferedWriter.WriteByte(byte(paddingBits))
	//pos1, _ := writer.File.Seek(0, 1)
	//buffrd1 := writer.BufferedWriter.Buffered()
	err := writer.BufferedWriter.Flush()
	if err != nil {
		return err
	}
	//pos2, _ := writer.File.Seek(0, 1)
	//buffrd2 := writer.BufferedWriter.Buffered()
	//fmt.Printf("Close %064b %v curpos: %v %v-%v %v\n", cur << paddingBits, paddingBits, pos1, buffrd1, buffrd2, pos2)
	return writer.File.Close()
}

func (writer *BitWriter) WriteBits(val uint64, numBits uint) error { // assumes val is all zeros above numBits
	cur, bitsUsed := writer.Cur, writer.CurBitsUsed
	cur = (cur << numBits) | val
	bitsUsed += numBits
	//fmt.Printf("Write raw bits buf %064b (%v)\n", cur, bitsUsed)
	//pos1, _ := writer.File.Seek(0, 1)
	//buffrd1 := writer.BufferedWriter.Buffered()
	for bitsUsed >= 8 {
		bitsUsed -= 8
		err := writer.BufferedWriter.WriteByte(byte(cur >> bitsUsed))
		if err != nil {
			return err
		}
	}
	//pos2, _ := writer.File.Seek(0, 1)
	//buffrd2 := writer.BufferedWriter.Buffered()
	//fmt.Printf("Write raw bits val: %064b (%v) curpos: %v %v-%v %v\n", val, numBits, pos1, buffrd1, buffrd2, pos2)
	writer.Cur, writer.CurBitsUsed = cur, bitsUsed
	return nil
}

func (writer *BitWriter) WriteVarUInt32(val uint32) error {
	cur, bitsUsed := writer.Cur, writer.CurBitsUsed
	var sizeFactor uint
	if        val & 0xfffffff0 == 0 {
		sizeFactor = 0
	} else if val & 0xffffff00 == 0 {
		sizeFactor = 1
	} else if val & 0xffff0000 == 0 {
		sizeFactor = 2
	} else {
		sizeFactor = 3
	}
	//fmt.Printf(" WRITTEN START val: %v cur:%064b  (%v)\n", val, cur, bitsUsed)
	cur = (cur << 2) | uint64(sizeFactor)
	bitsUsed += 2
	numBits := uint(4 << sizeFactor)
	cur = (cur << numBits) | uint64(val)
	bitsUsed += numBits
	//fmt.Printf(" WRITTEN END   val: %v cur:%064b  (%v) (bitsz: %v)\n", val, cur, bitsUsed, numBits)

	//pos1, _ := writer.File.Seek(0, 1)
	//buffrd1 := writer.BufferedWriter.Buffered()
	for bitsUsed >= 8 {
		bitsUsed -= 8
		err := writer.BufferedWriter.WriteByte(byte(cur >> bitsUsed))
		if err != nil {
			return err
		}
	}
	//pos2, _ := writer.File.Seek(0, 1)
	//buffrd2 := writer.BufferedWriter.Buffered()
	//fmt.Printf("Write var bits val: %064b (%v) curpos: %v %v-%v %v\n", val, numBits, pos1, buffrd1, buffrd2, pos2)
	writer.Cur, writer.CurBitsUsed = cur, bitsUsed
	return nil
}



type BitReader struct {
	BufferedReader *bufio.Reader
	File *os.File
	Cur uint64
	CurBitsLeft uint
}

func NewBitReader(file *os.File) (*BitReader, error) {	
	return &BitReader{BufferedReader:bufio.NewReader(file), File:file}, nil
}

func (reader *BitReader) Close() error {
	return reader.File.Close()
}

func (reader *BitReader) Refill(cur uint64, bitsLeft uint, numNeeded uint) (uint64, uint, error) {
	for bitsLeft <= 56 {
		rdr := reader.BufferedReader
		byt, err := rdr.ReadByte()
		if err != nil {
			if err == io.EOF && bitsLeft >= numNeeded {
				return cur, bitsLeft, nil
			} else {
				return cur, bitsLeft, err
			}
		}
		bitsLeft += 8
		cur = cur | (uint64(byt) << (64 - bitsLeft))
	}
	return cur, bitsLeft, nil
}
		
func (reader *BitReader) ReadBits(numBits uint) (uint64, error) {
	cur, bitsLeft := reader.Cur, reader.CurBitsLeft
	//fmt.Printf(" READ FIXED START cur:%064b (%v) [%v bit to read]\n", cur, bitsLeft, numBits)
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
	cur, bitsLeft := reader.Cur, reader.CurBitsLeft
	var err error
	if bitsLeft < 2 {
		cur, bitsLeft, err = reader.Refill(cur, bitsLeft, 2)
		if err != nil {
			return 0, err
		}
	}
	//fmt.Printf(" READ START cur:%064b (%v)\n", cur, bitsLeft)
	numNeeded := uint(4 << (cur >> 62))
	cur = cur << 2
	bitsLeft -= 2
	if bitsLeft < numNeeded {
		cur, bitsLeft, err = reader.Refill(cur, bitsLeft, numNeeded)
		if err != nil {
			return 0, err
		}
	}
	val := cur >> (64 - numNeeded)
	cur = cur << numNeeded
	bitsLeft -= numNeeded
	//fmt.Printf(" READ END   cur:%064b (%v) produced: %v\n", cur, bitsLeft, val)
	reader.Cur, reader.CurBitsLeft = cur, bitsLeft
	return uint32(val), nil
}
