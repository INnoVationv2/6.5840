package raft

import (
	"time"
)

type RequestVoteArgs struct {
	Term         int32
	CandidateId  int32
	LastLogIndex int32
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
	ServerIDx   int32
}

func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	lastLogIdx := int32(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	return &RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}
}

// 实现Follower投票规则
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%s]Get Vote Request:%v", rf.getServerDetail(), *args)

	currentTerm := rf.getCurrentTerm()
	reply.Term = currentTerm
	reply.VoteGranted = false

	if args.Term < currentTerm {
		DPrintf("[%s]RequestVote Term Is Expired", rf.getServerDetail())
		return
	} else if args.Term > currentTerm {
		rf.turnToFollower(args.Term, -1)
		rf.persist()
		reply.Term = args.Term
	} else if rf.getVoteFor() != -1 && rf.getVoteFor() != args.CandidateId {
		DPrintf("[%s]Already Voted In This Term", rf.getServerDetail())
		return
	}

	// 比较日志，只投给日志至少和自己一样新的Candidate
	lastLogIdx := int32(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	if !compareLog(args.LastLogIndex, args.LastLogTerm, lastLogIdx, lastLogTerm) {
		DPrintf("[%s]RequestVote Candidate Log Is Too Old", rf.getServerDetail())
		return
	}

	reply.VoteGranted = true
	rf.setVoteFor(args.CandidateId)
	rf.persist()
	DPrintf("[%s]Vote", rf.getServerDetail())
}

func (rf *Raft) ticker() {
	DPrintf("[%s]Join To Cluster", rf.getServerDetail())
	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())
		if rf.isLeader() {
			continue
		}

		DPrintf("[%v]Check Timtout.", rf.getServerDetail())
		if rf.getElectionTimer() == 0 {
			rf.startElectionTimer()
			continue
		}

		rf.mu.Lock()
		DPrintf("[%s]Election Timout.", rf.getServerDetail())
		rf.setRole(CANDIDATE)
		rf.incCurrentTerm()
		rf.setVoteFor(rf.me)
		rf.closeElectionTimer()
		rf.persist()
		requestVoteArgs := rf.buildRequestVoteArgs()
		go rf.startElection(requestVoteArgs)
		rf.mu.Unlock()
	}

	DPrintf("[%s]Disconnect From Cluster", rf.getServerDetail())
}

func (rf *Raft) startElection(args *RequestVoteArgs) {
	DPrintf("[%s]Timeout!!! Start New Election", rf.getServerDetail())
	voteReplyChan := make(chan *RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		serverIdx := idx
		DPrintf("[%v]Send Request Vote To %d", rf.getServerDetail(), serverIdx)
		go func() {
			reply := &RequestVoteReply{ServerIDx: int32(serverIdx)}
			ok := rf.sendRequestVote(serverIdx, args, reply)
			if !ok {
				reply.VoteGranted = false
			}
			voteReplyChan <- reply
		}()
	}

	voteCount, replyNum := int32(0), len(rf.peers)-1
	for replyNum > 0 && !rf.killed() {
		replyNum--
		select {
		case reply := <-voteReplyChan:
			rf.mu.Lock()
			if rf.getRole() != CANDIDATE || args.Term != rf.getCurrentTerm() || rf.killed() {
				DPrintf("[%v]Stop Election", rf.getServerDetail())
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted == true {
				DPrintf("[%v]%d Vote", rf.getServerDetail(), reply.ServerIDx)
				voteCount++
				if voteCount >= rf.majority {
					DPrintf("[%v]Get Majority Vote, Become Leader", rf.getServerDetail())
					rf.turnToLeader()
					rf.persist()
					rf.mu.Unlock()
					go rf.sendHeartbeat()
					return
				}
			} else {
				DPrintf("[%v]%d Not Vote", rf.getServerDetail(), reply.ServerIDx)
				if reply.Term > rf.getCurrentTerm() {
					DPrintf("[%s]Server %d's Term>CurrentTerm, Back To Follower", rf.getServerDetail(), reply.ServerIDx)
					rf.turnToFollower(reply.Term, -1)
					rf.persist()
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()
		}
	}

	DPrintf("[%s]Term:%d Not Get Majority Vote", rf.getServerDetail(), args.Term)
}

func (rf *Raft) sendRequestVote(idx int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[idx].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("[%v]Send Request Vote To %d Timeout.", rf.getServerDetail(), idx)
		return false
	}
	return true
}
