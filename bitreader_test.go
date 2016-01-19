package main

import (
	"os"
	"testing"
)

func TestBitReader(t *testing.T) {
	filename := "bitreader.test"
	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer os.Remove(filename)
	
	wtr, err := NewBitWriter(file)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wtr.WriteVarUInt32(7)
	wtr.WriteBits(42, 21) 
	wtr.WriteVarUInt32(0)
	wtr.WriteVarUInt32(1)
	wtr.WriteVarUInt32(2)
	wtr.WriteVarUInt32(123)
	wtr.WriteVarUInt32(12345)
	wtr.WriteVarUInt32(1234567)
	wtr.WriteVarUInt32(123456789)
	err = wtr.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// try adding mroe stuff at the end
	file, err = os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wtr, err = NewBitWriter(file)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wtr.WriteVarUInt32(7654321)
	err = wtr.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
	
	fd, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("%v", err)
	}
	rdr, err := NewBitReader(fd)
	if err != nil {
		t.Fatalf("%v", err)
	}
	val, err := rdr.ReadVarUInt32()
	if err != nil || val != 7 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	fixedval, err := rdr.ReadBits(21)
	if err != nil || fixedval != 42 {
		t.Fatalf("val:%v, err:%v", fixedval, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 0 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 1 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 2 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 123 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 12345 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 1234567 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 123456789 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	val, err = rdr.ReadVarUInt32()
	if err != nil || val != 7654321 {
		t.Fatalf("val:%v, err:%v", val, err)
	}
	err = rdr.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}

}
