package kvraft

import "sync/atomic"

func (kv *KVServer) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}

func (kv *KVServer) getTerm() int32 {
	return atomic.LoadInt32(&kv.raftTerm)
}

func (kv *KVServer) setTerm(term int32) {
	atomic.StoreInt32(&kv.raftTerm, term)
}
