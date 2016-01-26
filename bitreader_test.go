package scoredb

import (
	"os"
	"testing"
)

func TestBitReader(t *testing.T) {
	filename := RmAllTestData()("bitreader")
	defer RmAllTestData()

	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("%v", err)
	}

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

func TestBitReaderVolume(t *testing.T) {
	filename := RmAllTestData()("bitreader.volume")
	defer RmAllTestData()

	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("%v", err)
	}

	wtr, err := NewBitWriter(file)
	if err != nil {
		t.Fatalf("%v", err)
	}

	for i := 0; i < 200; i++ {
		wtr.WriteVarUInt32(uint32(i * i))
		wtr.WriteBits(uint64(i), uint(i%23)+10)
	}
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
	for i := 0; i < 200; i++ {
		val, err := rdr.ReadVarUInt32()
		if err != nil || int(val) != i*i {
			t.Fatalf("val:%v, err:%v", val, err)
		}
		fixedval, err := rdr.ReadBits(uint(i%23) + 10)
		if err != nil || int(fixedval) != i {
			t.Fatalf("val:%v, err:%v", fixedval, err)
		}
	}
	err = rdr.Close()
	if err != nil {
		t.Fatalf("%v", err)
	}
}
