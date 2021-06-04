package fsm

import (
	"fmt"
	"os"
	"testing"
)

func printKV(t *testing.T, f *FileHashmap, key string, exceptCount int, exceptValues ...uint32) {
	kv :=  f.Get(key)
	if exceptCount != len(kv) {
		t.Fatalf("except: %d, total: %d", exceptCount, len(kv))
	}
	for i, v := range kv {
		if exceptValues[i] != v.Value {
			t.Fatalf("except value: %d, current: %d", exceptValues[i], v.Value)
		}
		fmt.Println("Key", v.Key, "Value", v.Value, "Time", v.Time, "Next", v.Index)
	}
}

func Test_Main(t *testing.T) {
	fhm := &FileHashmap{
		2,
		3,
		1,
		make(map[uint]*os.File),
	false}

	fhm.LoadFiles("idx")
	printKV(t, fhm, "0", 0)
	fhm.Set("0", 1)
	printKV(t, fhm, "0", 1, 1)
	fhm.Set("0", 2)
	printKV(t, fhm, "0", 2, 2, 1)
	fhm.Set("0", 3)
	printKV(t, fhm, "0", 3, 3, 2, 1)
}

func Test_Dup(t *testing.T) {
	fhm := &FileHashmap{
		2,
		3,
		1,
		make(map[uint]*os.File),
	false}

	fhm.LoadFiles("idx")
	fhm.Set("0", 1)
	fhm.Set("0", 2)
	fhm.Set("0", 3)
	fhm.Set("0", 4)
	printKV(t, fhm, "0", 3, 4, 3, 2)
	fhm.Set("0", 5)
	printKV(t, fhm, "0", 3, 5, 4, 3)
	fhm.Set("0", 6)
	printKV(t, fhm, "0", 3, 6, 5, 4)
	fhm.Set("0", 7)
	printKV(t, fhm, "0", 3, 7, 6, 5)
	fhm.Set("0", 8)
	printKV(t, fhm, "0", 3, 8, 7, 6)
	fhm.Set("0", 9)
	printKV(t, fhm, "0", 3, 9, 8, 7)
	fhm.Set("0", 10)
	printKV(t, fhm, "0", 3, 10, 9, 8)
	fhm.Set("0", 11)
	printKV(t, fhm, "0", 3, 11, 10, 9)
}

func Test_Full(t *testing.T) {
	fhm := &FileHashmap{
		100,
		10,
		10,
		make(map[uint]*os.File),
	false}

	fhm.LoadFiles("idx")

	fhm.Set("0", 10)
	fhm.Set("1", 1)
	fhm.Set("2", 2)
	fhm.Set("3", 3)
	fhm.Set("4", 4)
	fhm.Set("5", 5)
	fhm.Set("6", 6)
	fhm.Set("7", 7)
	fhm.Set("8", 8)
	fhm.Set("9", 9)

	printKV(t, fhm, "0", 1, 10)
	printKV(t, fhm, "1", 1, 1)
	printKV(t, fhm, "2", 1, 2)
	printKV(t, fhm, "3", 1, 3)
	printKV(t, fhm, "4", 1, 4)
	printKV(t, fhm, "5",1 ,5)
	printKV(t, fhm, "6",1 ,6)
	printKV(t, fhm, "7",1 ,7)
	printKV(t, fhm, "8",1,8)
	printKV(t, fhm, "9",1,9)
}