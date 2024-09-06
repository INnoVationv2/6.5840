package raft

import "sync/atomic"

func (rf *Raft) getHeartbeat() bool {
	return atomic.LoadInt32(&rf.heartbeat) != 0
}

func (rf *Raft) setHeartbeat(heartbeat bool) {
	if heartbeat {
		atomic.CompareAndSwapInt32(&rf.heartbeat, 0, 1)
	} else {
		atomic.CompareAndSwapInt32(&rf.heartbeat, 1, 0)
	}
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

func (rf *Raft) getLeaderId() int32 {
	return atomic.LoadInt32(&rf.leaderId)
}

func (rf *Raft) setLeaderId(newId int32) {
	atomic.StoreInt32(&rf.leaderId, newId)
}

func (rf *Raft) getVoteFor() int32 {
	return atomic.LoadInt32(&rf.votedFor)
}

func (rf *Raft) setVoteFor(newId int32) {
	atomic.StoreInt32(&rf.votedFor, newId)
}

func (rf *Raft) getLastSendTime(idx int) int64 {
	return atomic.LoadInt64(&rf.lastSendTime[idx])
}

func (rf *Raft) setLastSendTime(idx int) {
	atomic.StoreInt64(&rf.lastSendTime[idx], getCurrentTime())
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

func (rf *Raft) getLogSz() int32 {
	return atomic.LoadInt32(&rf.logSz)
}

func (rf *Raft) setLogSz(LogSz int32) {
	atomic.StoreInt32(&rf.logSz, LogSz)
}
