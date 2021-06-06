package fsm

import (
	"log"
	"math"
	"os"
	"strconv"
)

//TODO

//1.    写数据时遍历找到写到哪里，并且在这里面找到最后一次记录
//1.1.1 如果满足(<=)写入数据
//1.1.2 看剩余空间是否<=5
//1.1.3 是则写入一个新链路
//2. 如果不满足，继续往后找，直到找到合适的位置
//3. 找到后写入数据
//4. 写入下次可写入数据的位置

// HeaderSize 头信息大小
// [00-03]（预留字段）4字节
// [04-11]（索引写入的起始时间）8字节
// [12-19]（索引写入的结束时间）8字节
// [20-23]（下次索引写入的位置）4字节
// [24-27]（下次数据写入的位置）4字节
// [28-39]（预留字段）12字节
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

// DefaultDataFolderName 默认数据路径名
const DefaultDataFolderName = "data"

// DefaultMaxValueSize 默认最大Value长度
const DefaultMaxValueSize = math.MaxUint16

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
		make(map[uint]*os.File),
		make(map[uint]*os.File)}

	err := os.MkdirAll(database+string(os.PathSeparator)+DefaultDataFolderName, 0755)

	if err != nil {
		panic(err)
	}

	for i := uint(0); i < hashMap.maxFileCount; i++ {
		var err error
		hashMap.indexList[i], err = os.OpenFile(database+string(os.PathSeparator)+strconv.FormatUint(uint64(i), 10), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
		hashMap.dataList[i], err = os.OpenFile(database+string(os.PathSeparator)+DefaultDataFolderName+string(os.PathSeparator)+strconv.FormatUint(uint64(i), 10), os.O_CREATE|os.O_RDWR|os.O_SYNC|os.O_TRUNC, 0666)
		if err != nil {
			panic(err)
		}
	}

	log.Println("loading db", database, maxFileCount, "files")

	return hashMap
}

// SetD Key=String, Value = uint32
func (f *fileHashmap) SetD(key string, value []byte) {
	if key == "" {
		return
	}

	if len(value) >= DefaultMaxValueSize {
		return
	}

	keyHash := f.hashcode(key)

	absFilePos := uint(keyHash) % f.maxFileCount

	absSlotPos := int64(HeaderSize) + int64(keyHash%f.maxSlotCount)*int64(SlotSize)

	slotValue := f.readUInt32(absSlotPos, f.indexList[absFilePos])

	if slotValue >= math.MaxUint32 {
		slotValue = 0
	}

	currentAvailableIndexSize := f.nextWriteableIndexOffset(absFilePos)

	currentAvailableDataSize := f.nextWriteableDataOffset(absFilePos)

	absIndexStartPos := int64(HeaderSize) + int64(f.maxSlotCount)*int64(SlotSize) + int64(currentAvailableIndexSize*IndexSize)

	f.writeIndex(absIndexStartPos, keyHash, currentAvailableDataSize, uint32(f.writeBeginTime(absFilePos)), slotValue, absFilePos)

	f.writeUInt32(absSlotPos, currentAvailableIndexSize, f.indexList[absFilePos])

	f.writeData(int64(currentAvailableDataSize), value, absFilePos)
}

// GetD Key=String
// 返回Key数组（可以重复）
func (f *fileHashmap) GetD(key string) [][]byte {
	dataBytes := make([][]byte, 0)

	if key == "" {
		return dataBytes
	}

	keyHash := f.hashcode(key)
	absFilePos := uint(keyHash) % f.maxFileCount

	indexList := f.getIndex(key)

	for _, offset := range indexList {
		if data := f.readData(int64(offset), absFilePos); len(data) > 0 {
			dataBytes = append(dataBytes, data)
		}
	}

	return dataBytes
}

// Set Key=String, Value = uint32
func (f *fileHashmap) Set(key string, value uint32) {
	if key == "" {
		return
	}
	keyHash := f.hashcode(key)

	absFilePos := uint(keyHash) % f.maxFileCount

	absSlotPos := int64(HeaderSize) + int64(keyHash%f.maxSlotCount)*int64(SlotSize)

	slotValue := f.readUInt32(absSlotPos, f.indexList[absFilePos])

	if slotValue >= math.MaxUint32 {
		slotValue = 0
	}

	currentAvailableSize := f.nextWriteableIndexOffset(absFilePos)

	absIndexStartPos := int64(HeaderSize) + int64(f.maxSlotCount)*int64(SlotSize) + int64(currentAvailableSize*IndexSize)

	f.writeIndex(absIndexStartPos, keyHash, value, uint32(f.writeBeginTime(absFilePos)), slotValue, absFilePos)

	f.writeUInt32(absSlotPos, currentAvailableSize, f.indexList[absFilePos])
}
