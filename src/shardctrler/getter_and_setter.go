package shardctrler

import "sync/atomic"

func (sc *ShardCtrler) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&sc.appliedLogIdx)
}

func (sc *ShardCtrler) setAppliedLogIdx(logIdx int32) {
	atomic.StoreInt32(&sc.appliedLogIdx, logIdx)
}

func (sc *ShardCtrler) getHistory(clientId int64, cmdId int32) (val *Config, ok bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	val, ok = sc.history[clientId][cmdId]
	DPrintf("[%v][GetHistory]ClientId:%d,CmdId:%d,Val:%v", sc.getServerDetail(), clientId, cmdId, val)
	return
}

func (sc *ShardCtrler) setHistory(clientId int64, cmdId int32, newVal *Config) {
	if _, ok := sc.history[clientId][cmdId]; !ok {
		sc.history[clientId] = make(map[int32]*Config)
	}
	sc.history[clientId][cmdId] = newVal
}

func (sc *ShardCtrler) deleteHistory(clientId int64, cmdId int32) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.history[clientId], cmdId)
}

func (sc *ShardCtrler) addSubmitCmd(cmd *Command, reply *Reply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	clientId, cmdId := cmd.ClientId, cmd.CommandId
	if _, ok := sc.submitCmd[clientId]; !ok {
		sc.submitCmd[clientId] = make(map[int32]*Reply)
	}
	sc.submitCmd[clientId][cmdId] = reply
}

func (sc *ShardCtrler) getSubmitCmd(cmd *Command) (*Reply, bool) {
	res, ok := sc.submitCmd[cmd.ClientId][cmd.CommandId]
	return res, ok
}

func (sc *ShardCtrler) deleteSubmitCmd(cmd *Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.submitCmd[cmd.ClientId], cmd.CommandId)
}
