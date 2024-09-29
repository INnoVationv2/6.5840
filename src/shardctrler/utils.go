package shardctrler

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardCtrler) getServerDetail() string {
	return fmt.Sprintf("ShardCtrler %d", sc.me)
}

func compareCmd(cmd1, cmd2 *Command) bool {
	return cmd1.Type == cmd2.Type &&
		cmd1.ClientId == cmd2.ClientId &&
		cmd1.CmdId == cmd2.CmdId
}

func maxi32(x, y int32) int32 {
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

func (sc *ShardCtrler) checkIfCommandAlreadyExecuted(clientId int64, commandId int32) bool {
	if sc.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (sc *ShardCtrler) createNewConfig() *Config {
	return &Config{
		Num:    sc.getConfigNo(),
		Groups: make(map[int][]string),
	}
}

func (sc *ShardCtrler) createNewConfigByOldConf(conf *Config) *Config {
	return &Config{
		Num:    sc.getConfigNo(),
		Groups: make(map[int][]string),
	}
}
