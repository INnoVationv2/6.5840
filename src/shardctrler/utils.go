package shardctrler

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

func (sc *ShardCtrler) getServerDetail() string {
	return fmt.Sprintf("ShardCtrler %d", sc.me)
}

func maxi32(x, y int32) int32 {
	if x >= y {
		return x
	}
	return y
}

func maxInt(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

func contains(slice []int, num int) bool {
	for _, value := range slice {
		if value == num {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) createNewConfByOldConf(oldConf *Config) (newConf *Config) {
	newConf = &Config{
		Num:    sc.getConfigNo(),
		Shards: oldConf.Shards,
		Groups: make(map[int][]string),
	}
	for gid, serverAddr := range oldConf.Groups {
		newConf.Groups[gid] = make([]string, len(serverAddr))
		copy(newConf.Groups[gid], serverAddr)
	}
	return
}
