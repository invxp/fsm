package fsm

import (
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
	//文件列表
	fileList map[uint]*os.File
}

// hashcode 算Key的Hash值
func (f *fileHashmap) hashcode(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// beginTime 获取文件最开始的写入时间
// 占Header的[04-11]
func (f *fileHashmap) beginTime(filePos uint) uint64 {
	return f.readUInt64(0+4, filePos)
}

// writeBeginTime 写入文件最开始的写入时间
// 占Header的[04-11]
func (f *fileHashmap) writeBeginTime(filePos uint) uint64 {
	var now uint64 = 0
	if now = f.beginTime(filePos); now == 0 {
		now = uint64(time.Now().Unix())
		f.writeUInt64(0+4, now, filePos)
	}
	return now
}

// writeEndTime 写入文件最后的写入时间
// 占Header的[12-19]
func (f *fileHashmap) writeEndTime(time uint64, filePos uint) {
	f.writeUInt64(0+4+8, time, filePos)
}

// nextWriteableIndexOffset 获取下一次可写入索引的游标位置（最多 1<<32 - 1 个）
// 占Header的[20-24]
func (f *fileHashmap) nextWriteableIndexOffset(filePos uint) uint32 {
	available := f.readUInt32(0+4+8+8, filePos)
	if available == math.MaxUint32 {
		available = 0
	}
	return available
}

// writeNextWriteableIndexOffset 下一次可写入索引的游标位置
// 占Header的[20-24]
func (f *fileHashmap) writeNextWriteableIndexOffset(filePos uint) uint32 {
	pos := f.nextWriteableIndexOffset(filePos) + 1
	if pos >= f.maxIndexCount {
		pos = 0
	}
	f.writeUInt32(0+4+8+8, pos, filePos)
	return pos
}

// writeIndex 写入16字节的索引信息
func (f *fileHashmap) writeIndex(pos int64, keyHash, value, timeDiff, slotValue uint32, absFilePos uint) {
	now := time.Now().Unix()
	f.writeUInt32(pos, keyHash, absFilePos)
	f.writeUInt32(pos+4, value, absFilePos)
	f.writeUInt32(pos+4+4, uint32(now)-timeDiff, absFilePos)
	f.writeUInt32(pos+4+4+4, slotValue, absFilePos)
	f.writeEndTime(uint64(now), absFilePos)
	f.writeNextWriteableIndexOffset(absFilePos)
}

// readIndex 读索引
func (f *fileHashmap) readIndex(absIndexPos int64, absFilePos uint) (keyHash uint32, value uint32, timeDiff uint32, prevIndex uint32) {
	return f.readUInt32(absIndexPos, absFilePos), f.readUInt32(absIndexPos+4, absFilePos), f.readUInt32(absIndexPos+4+4, absFilePos), f.readUInt32(absIndexPos+4+4+4, absFilePos)
}
