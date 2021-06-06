// 二进制文件读写实现

package fsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
)

func (f *fileHashmap) readByte(absPos int64, size uint64, file *os.File) []byte {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	if size >= DefaultMaxValueSize {
		return nil
	}

	byteString := make([]byte, size)

	if _, err := file.ReadAt(byteString, absPos); err != nil {
		if err == io.EOF {
			return nil
		}
		panic(err)
	}

	return byteString
}

func (f *fileHashmap) writeByte(absPos int64, value []byte, file *os.File) {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	if value == nil || len(value) == 0 || len(value) >= DefaultMaxValueSize {
		return
	}

	if _, err := file.WriteAt(value, absPos); err != nil {
		panic(err)
	}
}

func (f *fileHashmap) writeUInt32(absPos int64, val uint32, file *os.File) {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	var byteString bytes.Buffer
	if err := binary.Write(&byteString, binary.LittleEndian, val); err != nil {
		panic(err)
	}

	if _, err := file.WriteAt(byteString.Bytes(), absPos); err != nil {
		panic(err)
	}
}

func (f *fileHashmap) readUInt32(absPos int64, file *os.File) uint32 {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	var val uint32
	byteString := make([]byte, 4)

	if _, err := file.ReadAt(byteString, absPos); err != nil {
		if err == io.EOF {
			return math.MaxUint32
		}
		panic(err)
	}

	if err := binary.Read(bytes.NewBuffer(byteString), binary.LittleEndian, &val); err != nil {
		panic(err)
	}

	return val
}

func (f *fileHashmap) writeUInt64(absPos int64, val uint64, file *os.File) {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, val); err != nil {
		panic(err)
	}

	if _, err := file.WriteAt(buf.Bytes(), absPos); err != nil {
		panic(err)
	}
}

func (f *fileHashmap) readUInt64(absPos int64, file *os.File) uint64 {
	if file == nil {
		panic(fmt.Errorf("file not open"))
	}

	var val uint64
	byteString := make([]byte, 8)

	if _, err := file.ReadAt(byteString, absPos); err != nil {
		if err == io.EOF {
			return 0
		}
		panic(err)
	}

	if err := binary.Read(bytes.NewBuffer(byteString), binary.LittleEndian, &val); err != nil {
		panic(err)
	}

	return val
}
