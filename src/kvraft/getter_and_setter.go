package kvraft

import "sync/atomic"

func (kv *KVServer) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}

func (kv *KVServer) setAppliedLogIdx(logIdx int32) {
	atomic.StoreInt32(&kv.appliedLogIdx, logIdx)
}

func (kv *KVServer) getRaftTerm() int32 {
	return atomic.LoadInt32(&kv.raftTerm)
}

func (kv *KVServer) setRaftTerm(term int32) {
	atomic.StoreInt32(&kv.raftTerm, term)
}
