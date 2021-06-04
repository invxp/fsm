package fsm

import (
	"hash/crc32"
	"math"
	"time"
)

// hashcode 算Key的Hash值
func (f *FileHashmap) hashcode(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// beginTime 获取文件最开始的写入时间
// 占Header的[04-11]
func (f *FileHashmap) beginTime(filePos uint) uint64 {
	return f.readUInt64(0 + 4, filePos)
}

// writeBeginTime 写入文件最开始的写入时间
// 占Header的[04-11]
func (f *FileHashmap) writeBeginTime(filePos uint) uint64 {
	var now uint64 = 0
	if now = f.beginTime(filePos); now == 0 {
		now = uint64(time.Now().Unix())
		f.writeUInt64(0 + 4, now, filePos)
	}
	return now
}

// writeEndTime 写入文件最后的写入时间
// 占Header的[12-19]
func (f *FileHashmap) writeEndTime(time uint64, filePos uint) {
	f.writeUInt64(0 + 4 + 8, time, filePos)
}

// nextWriteableIndexOffset 获取下一次可写入索引的游标位置（最多 1<<32 - 1 个）
// 占Header的[20-24]
func (f *FileHashmap) nextWriteableIndexOffset(filePos uint) uint32 {
	available := f.readUInt32(0 + 4 + 8 + 8, filePos)
	if available == math.MaxUint32 {
		available = 0
	}
	return available
}

// writeNextWriteableIndexOffset 下一次可写入索引的游标位置
// 占Header的[20-24]
func (f *FileHashmap) writeNextWriteableIndexOffset(filePos uint) uint32 {
	pos := f.nextWriteableIndexOffset(filePos) + 1
	if pos >= f.maxIndexCount {
		pos = 0
	}
	f.writeUInt32(0 + 4 + 8 + 8, pos, filePos)
	return pos
}

// writeIndex 写入16字节的索引信息
func (f *FileHashmap) writeIndex(pos int64, keyHash, value, timeDiff, slotValue uint32, filePos uint) {
	f.writeUInt32(pos, keyHash, filePos)
	f.writeUInt32(pos + 4, value, filePos)
	f.writeUInt32(pos + 4 + 4, timeDiff, filePos)
	f.writeUInt32(pos + 4 + 4 + 4, slotValue, filePos)
}