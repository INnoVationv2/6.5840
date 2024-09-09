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

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) setCurrentTerm(newTerm int32) {
	atomic.StoreInt32(&rf.currentTerm, newTerm)
}

func (rf *Raft) incCurrentTerm() int32 {
	return atomic.AddInt32(&rf.currentTerm, 1)
}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(newRole int32) {
	atomic.StoreInt32(&rf.role, newRole)
}

func (rf *Raft) getVoteFor() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) setVoteFor(newId int32) {
	atomic.StoreInt32(&rf.votedFor, newId)
}

func (rf *Raft) getMatchIndex(idx int) int32 {
	return atomic.LoadInt32(&rf.matchIndex[idx])
}

func (rf *Raft) setMatchIndex(idx int, newIndex int32) {
	atomic.StoreInt32(&rf.matchIndex[idx], newIndex)
}

func (rf *Raft) getNextIndex(idx int) int32 {
	return atomic.LoadInt32(&rf.nextIndex[idx])
}

func (rf *Raft) setNextIndex(idx int, newIndex int32) {
	atomic.StoreInt32(&rf.nextIndex[idx], newIndex)
}

func (rf *Raft) addNextIndex(idx int, newIndex int32) int32 {
	return atomic.AddInt32(&rf.nextIndex[idx], newIndex)
}

func (rf *Raft) getCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commitIndex)
}

func (rf *Raft) setCommitIndex(commitIndex int32) {
	atomic.StoreInt32(&rf.commitIndex, commitIndex)
}

func (rf *Raft) resetHeartbeatTimer(idx int) {
	atomic.StoreInt64(&rf.lastSendTime[idx], getCurrentTime())
}

func (rf *Raft) heartbeatTimeout(idx int) bool {
	gap := time.Millisecond * 100
	lastSendTime := atomic.LoadInt64(&rf.lastSendTime[idx])
	return getCurrentTime()-lastSendTime >= gap.Milliseconds()
}
