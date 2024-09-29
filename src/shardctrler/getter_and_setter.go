package shardctrler

import "sync/atomic"

func (sc *ShardCtrler) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&sc.appliedLogIdx)
}

func (sc *ShardCtrler) setAppliedLogIdx(logIdx int32) {
	atomic.StoreInt32(&sc.appliedLogIdx, logIdx)
}

func (sc *ShardCtrler) getRaftTerm() int32 {
	return atomic.LoadInt32(&sc.raftTerm)
}

func (sc *ShardCtrler) setRaftTerm(term int32) {
	atomic.StoreInt32(&sc.raftTerm, term)
}

func (sc *ShardCtrler) getLatestConfig() *Config {
	return &sc.configs[sc.configCnt]
}
