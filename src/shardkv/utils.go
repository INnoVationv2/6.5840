package shardkv

import (
	"6.5840/logger"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Debug(format, a...)
	}
	return
}

func maxi32(x, y int32) int32 {
	if x >= y {
		return x
	}
	return y
}
