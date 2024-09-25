package kvraft

import (
	"fmt"
	"log"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func max(x, y int32) int32 {
	if x >= y {
		return x
	}
	return y
}

func abs(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

func compareCommand(cmd1 *Command, cmd2 *Command) bool {
	cmp := cmd1.Type == cmd2.Type && cmd1.Key == cmd2.Key
	if cmp && cmd1.Type != GET {
		cmp = cmd1.Value == cmd2.Value
	}
	return cmp
}

func (kv *KVServer) getServerDetail() string {
	return fmt.Sprintf("KvServer %d", kv.me)
}
