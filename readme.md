# FSM

FileHashmap 文件Hash索引（支持重复的Key）基于Go实现, 能力列表:

1. Get(key string) []values
2. Set(key string, value uint32)

具体介绍:

* 优化了Key存储逻辑（固定的Key只会存在一个文件里）
* 新增了过期机制（添加了下一次可以写入的游标）
* 待补充细节...

## 如何使用

可以直接阅读源码来学习具体的实现, 如果实在懒得看, 可以按照下面做:

```go
package main

import (
	"github.com/invxp/fsm"
	"testing"
)

func TestSingle(t *testing.T) {
	// 初始化
	// 最大索引槽数量
	// 最大索引数量
	// 最多文件数量
	// 索引目录
	fileHashMap := fsm.NewFileHashMap(
		fsm.DefaultMaxSlotCount,
		fsm.DefaultMaxIndexCount,
		fsm.DefaultMaxFileCount,
		"idx",
	)

	// 写入KV
	fileHashMap.Set("Key", 1)
	
	// 打印KV
	fileHashMap.Get("Key")
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