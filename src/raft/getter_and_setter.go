package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) getElectionTimer() int32 {
	return atomic.LoadInt32(&rf.heartbeat)
}

func (rf *Raft) closeElectionTimer() {
	atomic.StoreInt32(&rf.heartbeat, 0)
}

func (rf *Raft) startElectionTimer() {
	atomic.StoreInt32(&rf.heartbeat, 1)
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(newRole int32) {
	atomic.StoreInt32(&rf.role, newRole)
}

func (rf *Raft) resetHeartbeatTimer(idx int) {
	atomic.StoreInt64(&rf.lastSendTime[idx], getCurrentTime())
}

func (rf *Raft) heartbeatTimeout(idx int) bool {
	gap := time.Millisecond * 100
	lastSendTime := atomic.LoadInt64(&rf.lastSendTime[idx])
	return getCurrentTime()-lastSendTime >= gap.Milliseconds()
}

func (rf *Raft) getLastLogIndex() int32 {
	if len(rf.log) == 0 {
		return rf.snapshot.LastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int32 {
	if len(rf.log) == 0 {
		return rf.snapshot.LastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLogSz() int32 {
	return rf.getLastLogIndex() + 1
}

func (rf *Raft) getLogPosByIdx(idx int32) int32 {
	if rf.snapshot != nil {
		idx = idx - rf.snapshot.LastIncludedIndex - 1
	}
	return idx
}

func (rf *Raft) getLogTermByIdx(idx int32) int32 {
	pos := rf.getLogPosByIdx(idx)
	if pos < 0 || len(rf.log) == 0 {
		return rf.snapshot.LastIncludedTerm
	}
	return rf.log[pos].Term
}
