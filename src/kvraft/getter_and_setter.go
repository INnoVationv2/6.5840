package kvraft

import "sync/atomic"

func (kv *KVServer) getAppliedLogIdx() int32 {
	return atomic.LoadInt32(&kv.appliedLogIdx)
}
