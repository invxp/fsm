package fsm

import (
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"time"
)

// fileHashmap KV-FileStorage
type fileHashmap struct {
	//最大索引槽数量（可配置）
	maxSlotCount uint32
	//最大索引数量（可配置）
	maxIndexCount uint32
	//最多文件数量（可配置）
	maxFileCount uint
	//索引列表
	indexList map[uint]*os.File
	//数据列表
	dataList map[uint]*os.File
}

// hashcode 算Key的Hash值
func (f *fileHashmap) hashcode(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// beginTime 获取文件最开始的写入时间
// 占Header的[04-11]
func (f *fileHashmap) beginTime(absFilePos uint) uint64 {
	return f.readUInt64(0+4, f.indexList[absFilePos])
}

// writeBeginTime 写入文件最开始的写入时间
// 占Header的[04-11]
func (f *fileHashmap) writeBeginTime(absFilePos uint) uint64 {
	var now uint64 = 0
	if now = f.beginTime(absFilePos); now == 0 {
		now = uint64(time.Now().Unix())
		f.writeUInt64(0+4, now, f.indexList[absFilePos])
	}
	return now
}

// writeEndTime 写入文件最后的写入时间
// 占Header的[12-19]
func (f *fileHashmap) writeEndTime(endTime uint64, absFilePos uint) {
	f.writeUInt64(0+4+8, endTime, f.indexList[absFilePos])
}

// nextWriteableIndexOffset 获取下一次可写入索引的游标位置（最多 1<<32 - 1 个）
// 占Header的[20-23]
func (f *fileHashmap) nextWriteableIndexOffset(absFilePos uint) uint32 {
	available := f.readUInt32(0+4+8+8, f.indexList[absFilePos])
	if available >= math.MaxUint32 {
		available = 0
	}
	return available
}

// writeNextWriteableIndexOffset 下一次可写入索引的游标位置
// 占Header的[20-23]
func (f *fileHashmap) writeNextWriteableIndexOffset(absFilePos uint) uint32 {
	pos := f.nextWriteableIndexOffset(absFilePos) + 1
	if pos >= f.maxIndexCount {
		pos = 0
	}
	f.writeUInt32(0+4+8+8, pos, f.indexList[absFilePos])
	return pos
}

// nextWriteableDataOffset 获取下一次可写入索引的游标位置（最多 1<<32 - 1 个）
// 占Header的[24-27]
func (f *fileHashmap) nextWriteableDataOffset(absFilePos uint) uint32 {
	available := f.readUInt32(0+4+8+8+4, f.indexList[absFilePos])
	if available >= math.MaxUint32 {
		available = 0
	}
	return available
}

// writeNextWriteableDataOffset 下一次可写入索引的游标位置
// 占Header的[24-27]
func (f *fileHashmap) writeNextWriteableDataOffset(absDataPos uint32, absFilePos uint) {
	f.writeUInt32(0+4+8+8+4, absDataPos, f.indexList[absFilePos])
}

// writeIndex 写入16字节的索引信息
func (f *fileHashmap) writeIndex(absIndexPos int64, keyHash, value, timeDiff, slotValue uint32, absFilePos uint) {
	now := time.Now().Unix()
	f.writeUInt32(absIndexPos, keyHash, f.indexList[absFilePos])
	f.writeUInt32(absIndexPos+4, value, f.indexList[absFilePos])
	f.writeUInt32(absIndexPos+4+4, uint32(now)-timeDiff, f.indexList[absFilePos])
	f.writeUInt32(absIndexPos+4+4+4, slotValue, f.indexList[absFilePos])
	f.writeEndTime(uint64(now), absFilePos)
	f.writeNextWriteableIndexOffset(absFilePos)
}

// readIndex 读索引
func (f *fileHashmap) readIndex(absIndexPos int64, absFilePos uint) (keyHash uint32, value uint32, timeDiff uint32, prevIndex uint32) {
	return f.readUInt32(absIndexPos, f.indexList[absFilePos]),
		f.readUInt32(absIndexPos+4,
			f.indexList[absFilePos]),
		f.readUInt32(absIndexPos+4+4, f.indexList[absFilePos]),
		f.readUInt32(absIndexPos+4+4+4, f.indexList[absFilePos])
}

// writeData 写入数据
func (f *fileHashmap) writeData(absDataPos int64, value []byte, absFilePos uint) {
	//+last shift offset
	//current > last, more code ...
	//current < last, less code ...
	//TODO
	fmt.Println(f.readNextAvailableWritePos(uint32(absDataPos), len(value), absFilePos))

	f.writeUInt32(absDataPos, uint32(len(value)), f.dataList[absFilePos])
	f.writeByte(absDataPos+4, value, f.dataList[absFilePos])
	f.writeNextWriteableDataOffset(f.readNextAvailableWritePos(uint32(absDataPos), len(value), absFilePos), absFilePos)
}

// readData 读数据
func (f *fileHashmap) readData(absDataPos int64, absFilePos uint) []byte {
	return f.readByte(absDataPos+4, uint64(f.readUInt32(absDataPos, f.dataList[absFilePos])), f.dataList[absFilePos])
}

// readNextAvailableWritePos

/*
||||---------
||||-11111111-||||-222222-||||-3333333333333-||||-4---------------------------------
||||-11110000-||||-222222-||||-3333333333333-||||-4---------------------------------
||||-1111111111111111111111-||||-33333333333-||||-4---------------------------------
*/

func (f *fileHashmap) readNextAvailableWritePos(absDataPos uint32, bytesLength int, absFilePos uint) uint32 {
	writeOffset := absDataPos

	for {
		pos := f.readUInt32(int64(writeOffset), f.dataList[absFilePos])

		writeOffset += 4

		if pos < math.MaxUint32 {
			//See
			writeOffset += pos
		} else {
			writeOffset += uint32(bytesLength)
		}

		if writeOffset-absDataPos >= uint32(bytesLength) {
			break
		}
	}

	return writeOffset
}

// Get Key=String
// 返回Key数组（可以重复）
func (f *fileHashmap) getIndex(key string) []uint32 {
	values := make([]uint32, 0)

	if key == "" {
		return values
	}

	keyHash := f.hashcode(key)

	absFilePos := uint(keyHash) % f.maxFileCount

	slotPos := keyHash % f.maxSlotCount

	absSlotPos := int64(HeaderSize) + int64(slotPos)*int64(SlotSize)

	slotValue := f.readUInt32(absSlotPos, f.indexList[absFilePos])

	if slotValue >= math.MaxUint32 {
		return nil
	}

	lastIndex := slotValue

	nextIndex := slotValue

	for {
		absIndexPos := int64(HeaderSize) + int64(f.maxSlotCount)*int64(SlotSize) + int64(nextIndex)*int64(IndexSize)

		hash, value, _, prevIndex := f.readIndex(absIndexPos, absFilePos)

		if keyHash == hash {
			values = append(values, value)
		}

		if (prevIndex == lastIndex) || (nextIndex == 0 && prevIndex == 0) {
			return values
		}

		nextIndex = prevIndex
	}
}
