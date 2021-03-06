package fsm

import (
	"testing"
)

func getKV(t *testing.T, f *fileHashmap, key string, exceptCount int, exceptValues ...uint32) {
	kv := f.getIndex(key)
	if exceptCount != len(kv) {
		t.Fatalf("except: %d, total: %d", exceptCount, len(kv))
	}

	for i, value := range kv {
		if exceptValues[i] != value {
			t.Fatalf("except value: %d, current: %d", exceptValues[i], value)
		}
	}
}

func getKVS(t *testing.T, f *fileHashmap, key string, exceptCount int, exceptValues ...[]byte) {
	kv := f.GetD(key)
	if exceptCount != len(kv) {
		t.Fatalf("except: %d, total: %d", exceptCount, len(kv))
	}

	for i, v := range kv {
		if string(exceptValues[i]) != string(v) {
			t.Fatalf("except value: %d, current: %d", exceptValues[i], v)
		}
	}
}

func TestDataLess(t *testing.T) {
	fhm := NewFileHashMap(
		2,
		3,
		1,
		"idx",
	)

	fhm.SetD("0", []byte("111"))
	getKVS(t, fhm, "0", 1, []byte("111"))
	fhm.SetD("0", []byte("2222"))
	getKVS(t, fhm, "0", 2, []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("333333"))
	getKVS(t, fhm, "0", 3, []byte("333333"), []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("44"))
	getKVS(t, fhm, "0", 3, []byte("44"), []byte("333333"), []byte("2222"))
	fhm.SetD("0", []byte("555"))
	getKVS(t, fhm, "0", 3, []byte("555"), []byte("44"), []byte("333333"))
	fhm.SetD("0", []byte("6666"))
	getKVS(t, fhm, "0", 3, []byte("6666"), []byte("555"), []byte("44"))
	fhm.SetD("0", []byte("7"))
	getKVS(t, fhm, "0", 3, []byte("7"), []byte("6666"), []byte("555"))
}

func TestDataSame(t *testing.T) {
	fhm := NewFileHashMap(
		2,
		3,
		1,
		"idx",
	)

	fhm.SetD("0", []byte("111"))
	getKVS(t, fhm, "0", 1, []byte("111"))
	fhm.SetD("0", []byte("2222"))
	getKVS(t, fhm, "0", 2, []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("333333"))
	getKVS(t, fhm, "0", 3, []byte("333333"), []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("666"))
	getKVS(t, fhm, "0", 3, []byte("666"), []byte("333333"), []byte("2222"))
	fhm.SetD("0", []byte("7777"))
	getKVS(t, fhm, "0", 3, []byte("7777"), []byte("666"), []byte("333333"))
	fhm.SetD("0", []byte("888888"))
	getKVS(t, fhm, "0", 3, []byte("888888"), []byte("7777"), []byte("666"))
	fhm.SetD("0", []byte("999"))
	getKVS(t, fhm, "0", 3, []byte("999"), []byte("888888"), []byte("7777"), )
}

func TestDataMore(t *testing.T) {
	fhm := NewFileHashMap(
		2,
		3,
		1,
		"idx",
	)

	fhm.SetD("0", []byte("111"))
	getKVS(t, fhm, "0", 1, []byte("111"))
	fhm.SetD("0", []byte("2222"))
	getKVS(t, fhm, "0", 2, []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("333333"))
	getKVS(t, fhm, "0", 3, []byte("333333"), []byte("2222"), []byte("111"))
	fhm.SetD("0", []byte("1111"))
	getKVS(t, fhm, "0", 2, []byte("1111"), []byte("333333"))

	fhm.SetD("0", []byte("66666"))
	getKVS(t, fhm, "0", 2, []byte("66666"), []byte("1111"))
}

func TestSingle(t *testing.T) {
	fhm := NewFileHashMap(
		2,
		3,
		1,
		"idx",
	)

	getKV(t, fhm, "0", 0)
	fhm.Set("0", 1)
	getKV(t, fhm, "0", 1, 1)
	fhm.Set("0", 2)
	getKV(t, fhm, "0", 2, 2, 1)
	fhm.Set("0", 3)
	getKV(t, fhm, "0", 3, 3, 2, 1)
}

func TestDup(t *testing.T) {
	fhm := NewFileHashMap(
		2,
		3,
		1,
		"idx",
	)
	fhm.Set("0", 1)
	fhm.Set("0", 2)
	fhm.Set("0", 3)
	fhm.Set("0", 4)
	getKV(t, fhm, "0", 3, 4, 3, 2)
	fhm.Set("0", 5)
	getKV(t, fhm, "0", 3, 5, 4, 3)
	fhm.Set("0", 6)
	getKV(t, fhm, "0", 3, 6, 5, 4)
	fhm.Set("0", 7)
	getKV(t, fhm, "0", 3, 7, 6, 5)
	fhm.Set("0", 8)
	getKV(t, fhm, "0", 3, 8, 7, 6)
	fhm.Set("0", 9)
	getKV(t, fhm, "0", 3, 9, 8, 7)
	fhm.Set("0", 10)
	getKV(t, fhm, "0", 3, 10, 9, 8)
	fhm.Set("0", 11)
	getKV(t, fhm, "0", 3, 11, 10, 9)
}

func Test_Full(t *testing.T) {
	fhm := NewFileHashMap(
		100,
		10,
		10,
		"idx",
	)

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

	getKV(t, fhm, "0", 1, 10)
	getKV(t, fhm, "1", 1, 1)
	getKV(t, fhm, "2", 1, 2)
	getKV(t, fhm, "3", 1, 3)
	getKV(t, fhm, "4", 1, 4)
	getKV(t, fhm, "5", 1, 5)
	getKV(t, fhm, "6", 1, 6)
	getKV(t, fhm, "7", 1, 7)
	getKV(t, fhm, "8", 1, 8)
	getKV(t, fhm, "9", 1, 9)
}
