package raft

import (
	"sync/atomic"
	"time"
)

func (rf *Raft) sendHeartbeat() {
	gapMs := int64(100)
	gap := time.Duration(gapMs) * time.Millisecond
	times := int32(1)
	for !rf.killed() {
		DPrintf("[%s]%dth Send Heartbeat\n", rf.getServerDetail(), atomic.LoadInt32(&times))

		for idx := range rf.peers {
			if !rf.isLeader() {
				return
			}
			if calGap(rf.getLastSendTime(idx)) < gapMs {
				continue
			}
			go rf.sendEntriesToFollower(idx)
		}
		time.Sleep(gap)
		atomic.AddInt32(&times, 1)
	}
}

func (rf *Raft) handleHeartbeat(args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%s]Get Heartbeat:%v\n", rf.getServerDetail(), *args)
	rf.setHeartbeat(true)
	role := rf.getRole()
	if role == CANDIDATE || (role == LEADER && args.Term > rf.getCurrentTerm()) {
		rf.setRole(FOLLOWER)
		rf.setCurrentTerm(args.Term)
		rf.setLeaderId(args.LeaderId)
		rf.setVoteFor(args.LeaderId)
		DPrintf("[%s]Heartbeat Back To Follwer\n", rf.getRoleStr())
		return
	}
}
