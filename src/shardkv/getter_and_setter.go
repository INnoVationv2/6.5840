package shardkv

import "sync/atomic"

func (kv *ShardKV) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}

func (kv *ShardKV) setAppliedLogIdx(logIdx int32) {
	atomic.StoreInt32(&kv.appliedLogIdx, logIdx)
}

func (kv *ShardKV) getRaftTerm() int32 {
	return atomic.LoadInt32(&kv.raftTerm)
}

func (kv *ShardKV) setRaftTerm(term int32) {
	atomic.StoreInt32(&kv.raftTerm, term)
}
