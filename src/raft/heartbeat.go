package raft

import (
	"sync/atomic"
	"time"
)

type HeartbeatArgs struct {
	Term     int32
	LeaderId int32
}

func (rf *Raft) buildHeartbeatArgs() *HeartbeatArgs {
	return &HeartbeatArgs{
		Term:     rf.getCurrentTerm(),
		LeaderId: rf.me,
	}
}

func (rf *Raft) sendHeartbeat() {
	times := int32(1)
	reply := &EmptyStruct{}

	gap := time.Duration(100) * time.Millisecond
	for !rf.killed() {
		DPrintf("[%s]%dth Send Heartbeat\n", rf.getServerDetail(), atomic.LoadInt32(&times))
		if rf.getRole() != Leader {
			return
		}
		args := rf.buildHeartbeatArgs()
		for idx := range rf.peers {
			if int32(idx) == rf.me {
				continue
			}
			idx := idx
			go func() {
				ok := rf.peers[idx].Call("Raft.Heartbeat", args, reply)
				if !ok {
					DPrintf("[%s]%dth Send Heartbeat To %d Timeout\n", rf.getServerDetail(), atomic.LoadInt32(&times), idx)
				}
			}()
		}
		time.Sleep(gap)
		atomic.AddInt32(&times, 1)
	}
}

func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *EmptyStruct) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%s]Get Heartbeat:%v\n", rf.getServerDetail(), *args)

	if args.Term < rf.getCurrentTerm() {
		return
	}

	rf.setHeartbeat(true)
	role := rf.getRole()
	if role == Candidate || (role == Leader && args.Term > rf.getCurrentTerm()) {
		rf.setRole(Follower)
		rf.setCurrentTerm(args.Term)
		rf.setLeaderId(args.LeaderId)
		rf.setVoteFor(args.LeaderId)
		DPrintf("[%s]Heartbeat Back To Follwer\n", rf.getRoleStr())
		return
	}
}
