package shardkv

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

func compareCommand(cmd1 *Command, cmd2 *Command) bool {
	return cmd1.Type == cmd2.Type &&
		cmd1.ClientId == cmd2.ClientId &&
		cmd1.CmdId == cmd2.CmdId
}

func (kv *ShardKV) getServerDetail() string {
	return fmt.Sprintf("ShardKvServer %d", kv.me)
}

func cmdTypeToStr(opType int) string {
	switch opType {
	case GET:
		return "Get"
	case PUT:
		return "Put"
	case APPEND:
		return "Append"
	}
	log.Fatalf("Transfter OpType Failed: %d", opType)
	return ""
}
