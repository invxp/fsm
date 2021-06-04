// 二进制文件读写实现

package fsm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func (f *FileHashmap) writeUInt32(pos int64, val uint32, filePos uint) {
	if f.fileList[filePos] == nil {
		panic(fmt.Errorf("file %d not open", filePos))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, val); err != nil {
		panic(err)
	}

	if _, err := f.fileList[filePos].WriteAt(buf.Bytes(), pos); err != nil {
		panic(err)
	}
}

func (f *FileHashmap) readUInt32(absPos int64, filePos uint) uint32 {
	if f.fileList[filePos] == nil {
		panic(fmt.Errorf("file %d not open", filePos))
	}

	var val uint32
	bs := make([]byte, 4)

	if _, err := f.fileList[filePos].ReadAt(bs, absPos); err != nil {
		if err == io.EOF {
			return math.MaxUint32
		}
		panic(err)
	}

	if err := binary.Read(bytes.NewBuffer(bs), binary.LittleEndian, &val); err != nil {
		panic(err)
	}

	return val
}

func (f *FileHashmap) writeUInt64(pos int64, val uint64, filePos uint) {
	if f.fileList[filePos] == nil {
		panic(fmt.Errorf("file %d not open", filePos))
	}

	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, val); err != nil {
		panic(err)
	}

	if _, err := f.fileList[filePos].WriteAt(buf.Bytes(), pos); err != nil {
		panic(err)
	}
}

func (f *FileHashmap) readUInt64(pos int64, filePos uint) uint64 {
	if f.fileList[filePos] == nil {
		panic(fmt.Errorf("file %d not open", filePos))
	}

	var val uint64
	bs := make([]byte, 8)

	if _, err := f.fileList[filePos].ReadAt(bs, pos); err != nil {
		if err == io.EOF {
			return 0
		}
		panic(err)
	}

	if err := binary.Read(bytes.NewBuffer(bs), binary.LittleEndian, &val); err != nil {
		panic (err)
	}

	return val
}