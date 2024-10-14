package raft

import (
	"sync/atomic"
)

//func (rf *Raft) getLastContact() int64 {
//	return atomic.LoadInt64(&rf.lastContact)
//}
//
//func (rf *Raft) setLastContact() {
//	atomic.StoreInt64(&rf.lastContact, now())
//}

func (rf *Raft) getRole() int32 {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(newRole int32) {
	atomic.StoreInt32(&rf.role, newRole)
}

func (rf *Raft) setMatchIdx(serverNo int, matchIndex int32) {
	atomic.StoreInt32(&rf.commitment.matchIndex[serverNo], matchIndex)
}

//func (rf *Raft) resetHeartbeatTimer() {
//	atomic.StoreInt64(&rf.lastContact, getCurrentTime())
//}

//func heartbeatTimeout(lastContact int64) bool {
//	gap := time.Millisecond * 100
//	return now()-lastContact >= gap.Milliseconds()
//}

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

//
//	func (rf *Raft) getLogIndexByIdx(idx int32) int32 {
//		pos := rf.getLogPosByIdx(idx)
//		if pos < 0 {
//			return rf.snapshot.LastIncludedIndex
//		}
//		return rf.log[pos].Index
//	}

func (rf *Raft) getLogTermByIdx(idx int32) int32 {
	pos := rf.getLogPosByIdx(idx)
	if pos < 0 {
		return rf.snapshot.LastIncludedTerm
	}
	return rf.log[pos].Term
}

func (rf *Raft) getCurrentTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) incCurrentTerm() int32 {
	return atomic.AddInt32(&rf.currentTerm, 1)
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
