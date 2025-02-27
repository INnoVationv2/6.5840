package kvraft

import (
	"sync/atomic"
)

func (kv *KVServer) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}

func (kv *KVServer) setAppliedLogIdx(logIdx int32) {
	DPrintf("[%s]Update appliedLogIdx To %d", kv.getServerDetail(), logIdx)
	atomic.StoreInt32(&kv.appliedLogIdx, logIdx)
}

func (kv *KVServer) getRaftTerm() int {
	term, _ := kv.rf.GetState()
	return term
}

func (kv *KVServer) getHistory(clientId int64, cmdId int32) (val string, ok bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, ok = kv.history[clientId][cmdId]
	DPrintf("[%v][GetHistory]ClientId:%d,CmdId:%d,Val:%s", kv.getServerDetail(), clientId, cmdId, val)
	return
}

func (kv *KVServer) setHistory(clientId int64, cmdId int32, newVal string) {
	if _, ok := kv.history[clientId][cmdId]; !ok {
		kv.history[clientId] = make(map[int32]string)
	}
	kv.history[clientId][cmdId] = newVal
}

func (kv *KVServer) deleteHistory(clientId int64, cmdId int32) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.history[clientId], cmdId)
}

func (kv *KVServer) addSubmitCmd(cmd *Command, result *Reply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, cmdId := cmd.ClientId, cmd.CmdId
	if _, ok := kv.submitCmd[clientId]; !ok {
		kv.submitCmd[clientId] = make(map[int32]*Reply)
	}
	kv.submitCmd[clientId][cmdId] = result
}

func (kv *KVServer) getSubmitCmd(cmd *Command) *Reply {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.submitCmd[cmd.ClientId][cmd.CmdId]
}

func (kv *KVServer) deleteSubmitCmd(cmd *Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.submitCmd[cmd.ClientId], cmd.CmdId)
}
