package fsm

import (
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

// HeaderSize 头信息大小
// [00-03]（预留字段）4字节
// [04-11]（索引写入的起始时间）8字节
// [12-19]（索引写入的结束时间）8字节
// [20-23]（下次索引写入的位置）4字节
// [24-39]（预留字段）16字节
const HeaderSize = 40

// SlotSize Hash槽的大小，固定4字节
const SlotSize = 4

// IndexSize 索引的大小
// [00-03]（KeyHash）4字节
// [04-07]（Value）4字节
// [08-11]（与索引写入的起始时间的时间差）4字节
// [12-15]（链表Index）4字节
const IndexSize = 16

// FileIndex Get返回的结构体
type FileIndex struct {
	Key 	string
	Value 	uint32
	Time    uint32
	Index   uint32
}

// FileHashmap KV-FileStorage
type FileHashmap struct {
	//最大索引槽数量（可配置）
	maxSlotCount 	uint32
	//最大索引数量（可配置）
	maxIndexCount 	uint32
	//最多文件数量（可配置）
	maxFileCount uint
	//文件列表
	fileList map[uint]*os.File
	//是否打印日志
	printLog bool
}

// Set Key=String, Value = uint32
func (f *FileHashmap) Set(key string, value uint32) {
	keyHash := f.hashcode(key)
	filePos := uint(keyHash) % f.maxFileCount
	slotPos := keyHash % f.maxSlotCount
	absSlotPos := int64(HeaderSize) + int64(slotPos) * int64(SlotSize)
	slotValue := f.readUInt32(absSlotPos, filePos)

	if slotValue == math.MaxUint32 {
		slotValue = 0
	}

	absIndexStartPos := int64(HeaderSize) + int64(f.maxSlotCount) * int64(SlotSize)

	currentAvailableSize := f.nextWriteableIndexOffset(filePos)

	absIndexWritePos := absIndexStartPos + int64(currentAvailableSize) * int64(IndexSize)

	lastTime := f.writeBeginTime(filePos)
	now := time.Now().Unix()
	f.writeUInt32(absSlotPos, currentAvailableSize, filePos)

	lastAvailableSize := f.nextWriteableIndexOffset(filePos)

	f.writeIndex(absIndexWritePos, keyHash, value, uint32(now) - uint32(lastTime), slotValue, filePos)

	f.writeEndTime(uint64(now), filePos)

	f.writeNextWriteableIndexOffset(filePos)

	if f.printLog {
		log.Printf("Set --- IndexPos: %d, SlotValue: %d, AvailiableSize: %d->%d\n", absIndexWritePos, slotValue, currentAvailableSize, lastAvailableSize)
	}
}

// Get Key=String
// 返回Key数组（可以重复）
func (f *FileHashmap) Get(key string) []FileIndex {
	fi := make([]FileIndex, 0)

	keyHash := f.hashcode(key)
	filePos := uint(keyHash) % f.maxFileCount
	slotPos := keyHash % f.maxSlotCount
	absSlotPos := int64(HeaderSize) + int64(slotPos) * int64(SlotSize)
	slotValue := f.readUInt32(absSlotPos, filePos)

	if slotValue == math.MaxUint32 {
		return nil
	}

	lastIndex := slotValue

	nextIndex := slotValue

	for {
		absIndexPos := int64(HeaderSize) + int64(f.maxSlotCount)*int64(SlotSize) + int64(nextIndex)*int64(IndexSize)

		keyHashRead := f.readUInt32(absIndexPos, filePos)

		value := f.readUInt32(absIndexPos + 4, filePos)

		timeDiff := f.readUInt32(absIndexPos + 4 + 4, filePos)

		prevIndex := f.readUInt32(absIndexPos + 4 + 4 + 4, filePos)

		if keyHash == keyHashRead {
			fi = append(fi, FileIndex{key, value, timeDiff, prevIndex})
		}

		if f.printLog {
			log.Printf("Get --- IndexPos: %d, SlotValue: %d, NextIndex: %d, LastIndex: %d, Value: %d\n", absIndexPos, nextIndex, prevIndex, lastIndex, value)
		}

		if (prevIndex == lastIndex) || (nextIndex == 0 && prevIndex == 0) {
			return fi
		}

		nextIndex = prevIndex
	}
}

// LoadFiles 加载索引文件（选定一个目录）
func (f *FileHashmap) LoadFiles(absDirPath string) {
	if f.fileList == nil {
		f.fileList = make(map[uint]*os.File)
	}

	err := os.MkdirAll(absDirPath, 0755)

	if err != nil {
		panic(err)
	}

	for i := uint(0); i < f.maxFileCount; i++ {
		var err error
		f.fileList[i], err = os.OpenFile(absDirPath + string(os.PathSeparator) + strconv.FormatUint(uint64(i), 10), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
	}

	log.Println("loading", len(f.fileList), "files")
}