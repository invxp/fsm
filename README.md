# FSM

FileHashmap 文件Hash索引（支持重复的Key）基于Go实现, 能力列表:

1. Get(key string) []FileIndex
2. Set(key string, value uint32)

具体介绍:

* 优化了Key存储逻辑（固定的Key只会存在一个文件里）
* 新增了过期机制（添加了下一次可以写入的游标）
* 待补充实现细节...

## 如何使用

可以直接阅读源码来学习具体的实现, 如果实在懒得看, 可以按照下面做:

```go
package main

func Test_Main(t *testing.T) {
	// 初始化
	// 最大索引槽数量
	// 最大索引数量
	// 最多文件数量
	// 文件具柄初始化
	// 是否打印日志
	fhm := &FileHashmap{
		2,
		3,
		1,
		make(map[uint]*os.File),
		false}

	// 选定加载索引的目录
	fhm.LoadFiles("idx")
	// 打印KV
	printKV(t, fhm, "0", 0)
	// 写入KV
	fhm.Set("0", 1)
	// 打印KV
	printKV(t, fhm, "0", 1, 1)
}
```

测试用例可以这样做:

```
$ go test -v -race -run @XXXXX(具体方法名)
PASS / FAILED
```

或测试全部用例:
```
$ go test -v -race
```

## TODO
1. Value切换成可变长度String
2. Value可以修改 + Key不可重复
3. 删除Key
4. 支持高可用（多机）
