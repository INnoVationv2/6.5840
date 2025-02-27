package kvraft

import (
	"6.5840/logger"
	"fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Debug(format, a...)
	}
	return
}

func (kv *KVServer) getServerDetail() string {
	return fmt.Sprintf("KvServer %d", kv.me)
}
