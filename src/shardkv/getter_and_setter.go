package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"sync/atomic"
)

func (kv *ShardKV) getServerDetail() string {
	return fmt.Sprintf("ShardKV %d_%d", kv.gid, kv.me)
}

func (kv *ShardKV) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}

func (kv *ShardKV) setAppliedLogIdx(logIdx int32) {
	DPrintf("[%s]Update appliedLogIdx To %d", kv.getServerDetail(), logIdx)
	atomic.StoreInt32(&kv.appliedLogIdx, logIdx)
}

func (kv *ShardKV) getHistory(clientId int64, cmdId int32) (val string, ok bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok = kv.history[clientId][cmdId]
	DPrintf("[%v][GetHistory]ClientId:%d,CmdId:%d,Val:%s", kv.getServerDetail(), clientId, cmdId, val)
	return
}

func (kv *ShardKV) setHistory(clientId int64, cmdId int32, newVal string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.history[clientId][cmdId]; !ok {
		kv.history[clientId] = make(map[int32]string)
	}
	kv.history[clientId][cmdId] = newVal
}

func (kv *ShardKV) deleteHistory(clientId int64, cmdId int32) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.history[clientId], cmdId)
}

func (kv *ShardKV) addSubmitCmd(cmd *Command, result *Reply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, cmdId := cmd.ClientId, cmd.CmdId
	if _, ok := kv.submitCmd[clientId]; !ok {
		kv.submitCmd[clientId] = make(map[int32]*Reply)
	}
	kv.submitCmd[clientId][cmdId] = result
}

func (kv *ShardKV) getSubmitCmd(cmd *Command) (reply *Reply, ok bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	reply, ok = kv.submitCmd[cmd.ClientId][cmd.CmdId]
	return
}

func (kv *ShardKV) deleteSubmitCmd(cmd *Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.submitCmd[cmd.ClientId], cmd.CmdId)
}

func (kv *ShardKV) setShardConfig(conf *shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.shardConf = conf
}

func (kv *ShardKV) getShardConfig() *shardctrler.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.shardConf
}

func (kv *ShardKV) setMatchIndex(clientId int64, newMatchIdx int32) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if newMatchIdx > kv.matchIndex[clientId] {
		kv.matchIndex[clientId] = newMatchIdx
	}
}

func (kv *ShardKV) getMatchIndex(clientId int64) int32 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.matchIndex[clientId]
}
