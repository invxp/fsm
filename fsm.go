package fsm

import (
	"log"
	"math"
	"os"
	"strconv"
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

// DefaultMaxSlotCount 默认最大槽的数量（500W）
const DefaultMaxSlotCount = 5000000

// DefaultMaxIndexCount 默认最大索引的数量，推荐槽数量的4倍（4 * 500W = 2000W）
const DefaultMaxIndexCount = 20000000

// DefaultMaxFileCount 默认最大索引文件数量
const DefaultMaxFileCount = 1024

// NewFileHashMap 初始化对象（选定一个目录）
func NewFileHashMap(maxSlotCount, maxIndexCount uint32, maxFileCount uint, database string) *fileHashmap {
	if maxSlotCount == 0 {
		maxSlotCount = DefaultMaxSlotCount
	}
	if maxIndexCount == 0 {
		maxIndexCount = DefaultMaxIndexCount
	}
	if maxFileCount == 0 {
		maxFileCount = DefaultMaxFileCount
	}

	hashMap := &fileHashmap{maxSlotCount,
		maxIndexCount,
		maxFileCount,
		make(map[uint]*os.File)}

	err := os.MkdirAll(database, 0755)

	if err != nil {
		panic(err)
	}

	for i := uint(0); i < hashMap.maxFileCount; i++ {
		var err error
		hashMap.fileList[i], err = os.OpenFile(database+string(os.PathSeparator)+strconv.FormatUint(uint64(i), 10), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
	}

	log.Println("loading db", database, maxFileCount, "files")

	return hashMap
}

// Set Key=String, Value = uint32
func (f *fileHashmap) Set(key string, value uint32) {
	if key == "" {
		return
	}
	keyHash := f.hashcode(key)

	absFilePos := uint(keyHash) % f.maxFileCount

	absSlotPos := int64(HeaderSize) + int64(keyHash%f.maxSlotCount)*int64(SlotSize)

	slotValue := f.readUInt32(absSlotPos, absFilePos)

	if slotValue == math.MaxUint32 {
		slotValue = 0
	}

	currentAvailableSize := f.nextWriteableIndexOffset(absFilePos)

	f.writeUInt32(absSlotPos, currentAvailableSize, absFilePos)

	absIndexStartPos := int64(HeaderSize) + int64(f.maxSlotCount)*int64(SlotSize) + int64(currentAvailableSize*IndexSize)

	f.writeIndex(absIndexStartPos, keyHash, value, uint32(f.writeBeginTime(absFilePos)), slotValue, absFilePos)
}

// Get Key=String
// 返回Key数组（可以重复）
func (f *fileHashmap) Get(key string) []uint32 {
	values := make([]uint32, 0)

	if key == "" {
		return values
	}

	keyHash := f.hashcode(key)

	absFilePos := uint(keyHash) % f.maxFileCount

	slotPos := keyHash % f.maxSlotCount

	absSlotPos := int64(HeaderSize) + int64(slotPos)*int64(SlotSize)

	slotValue := f.readUInt32(absSlotPos, absFilePos)

	if slotValue == math.MaxUint32 {
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
