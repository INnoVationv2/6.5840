package raft

import (
	"sync/atomic"
)

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setVotedFor(votedFor int32) {
	atomic.StoreInt32(&rf.votedFor, votedFor)
}

func (rf *Raft) getVotedFor() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) setRole(newRole int32) {
	atomic.StoreInt32(&rf.role, newRole)
}

func (c *commitment) setMatchIdx(serverNo int, matchIndex int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndex[serverNo] = max(c.matchIndex[serverNo], matchIndex)
}

func (rf *Raft) setLastLog(lastLogIdx, lastLogTerm int32) {
	rf.statusMu.Lock()
	defer rf.statusMu.Unlock()

	rf.lastLogIdx, rf.lastLogTerm = lastLogIdx, lastLogTerm
}

func (rf *Raft) getLastLogIndex() int32 {
	lastLogIdx, _ := rf.getLastLog()
	return lastLogIdx
}

func (rf *Raft) getLastLog() (int32, int32) {
	rf.statusMu.RLock()
	defer rf.statusMu.RUnlock()
	return rf.lastLogIdx, rf.lastLogTerm
}

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) setCurrentTerm(newTerm int32) {
	atomic.StoreInt32(&rf.currentTerm, newTerm)
}

//
//func (rf *Raft) getSnapshot() *Snapshot {
//	rf.snapshotLock.Lock()
//	defer rf.snapshotLock.Unlock()
//	return rf.snapshot
//}

func (rf *Raft) incThreadCnt() {
	atomic.AddInt32(&rf.threadCnt, 1)
}

func (rf *Raft) decThreadCnt() {
	atomic.AddInt32(&rf.threadCnt, -1)
}

func (rf *Raft) getThreadCnt() int32 {
	return atomic.LoadInt32(&rf.threadCnt)
}
