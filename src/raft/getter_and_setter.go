package raft

import (
	"sync/atomic"
)

func (rf *Raft) setRole(newRole int32) {
	atomic.StoreInt32(&rf.role, newRole)
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setVotedFor(votedFor int32) {
	atomic.StoreInt32(&rf.votedFor, votedFor)
}

func (rf *Raft) getVotedFor() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) setLastLog(idx, term int32) {
	rf.statusMu.Lock()
	defer rf.statusMu.Unlock()
	rf.lastLogIdx, rf.lastLogTerm = idx, term
}

func (rf *Raft) getLastLog() (int32, int32) {
	rf.statusMu.RLock()
	defer rf.statusMu.RUnlock()
	return rf.lastLogIdx, rf.lastLogTerm
}

func (rf *Raft) getLastLogIndex() int32 {
	lastLogIdx, _ := rf.getLastLog()
	return lastLogIdx
}

func (rf *Raft) getLogTermByIdx(idx int32) int32 {
	var entry LogEntry
	if err := rf.log.getOne(idx, &entry); err != nil {
		return rf.snapshot.lastIncludeTerm()
	}
	return entry.Term
}

func (rf *Raft) setCurrentTerm(newTerm int32) {
	atomic.StoreInt32(&rf.currentTerm, newTerm)
}

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}
