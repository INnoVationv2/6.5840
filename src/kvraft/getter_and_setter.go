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

func (kv *KVServer) getRole() int32 {
	return atomic.LoadInt32(&kv.role)
}

func (kv *KVServer) isLeader() bool {
	return atomic.LoadInt32(&kv.role) == LEADER
}

func (kv *KVServer) turnToLeader() {
	atomic.StoreInt32(&kv.role, LEADER)
}

func (kv *KVServer) turnToFollower() {
	atomic.StoreInt32(&kv.role, FOLLOWER)
}
