package raft

import (
	"sync/atomic"
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
}

func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
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
	candidateId := args.CandidateId

	if args.Term < currentTerm {
		DPrintf("[%s]Candidate:%d Term:%d < My Term:%d, Not Vote",
			rf.getServerDetail(), candidateId, args.Term, currentTerm)
		return
	}

	if args.Term > currentTerm {
		DPrintf("[%v]Candidate:%d Term:%d > My Term:%d, Turn To Follower", rf.getServerDetail(), candidateId, args.Term, currentTerm)
		rf.turnToFollower(args.Term, -1)
		rf.persist()
		reply.Term = args.Term
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[%s]Already Vote To %d In Term:%d", rf.getServerDetail(), rf.votedFor, currentTerm)
		return
	}

	// 比较日志，只投给日志至少和自己一样新的Candidate
	lastLogIdx, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	if !compareLog(args.LastLogIndex, args.LastLogTerm, lastLogIdx, lastLogTerm) {
		DPrintf("[%s]Candidate:%d Log Is Too Old", rf.getServerDetail(), args.CandidateId)
		return
	}

	// 如果投票给对方，就重置选举超时器，防止冲突
	rf.closeElectionTimer()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()
	DPrintf("[%s]Vote To %d", rf.getServerDetail(), rf.votedFor)
}

func (rf *Raft) ticker() {
	// For Crash Recover Use
	if rf.role == LEADER {
		go rf.sendHeartbeat()
	}
	DPrintf("[%s]Join To Cluster", rf.getServerDetail())
	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())
		if rf.isLeader() {
			continue
		}

		DPrintf("[%v]Check Timtout.", rf.getServerDetail())
		if rf.getElectionTimer() == 0 {
			DPrintf("[%v]Not Timeout", rf.getServerDetail())
			rf.startElectionTimer()
			continue
		}

		DPrintf("[%s]Election Timout.", rf.getServerDetail())
		rf.mu.Lock()
		rf.setRole(CANDIDATE)
		rf.incCurrentTerm()
		rf.votedFor = rf.me
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
	voteCount := rf.majority
	for serverNo := range rf.peers {
		if int32(serverNo) == rf.me {
			continue
		}
		go rf.requestVote(serverNo, args, &voteCount)
	}
}

func (rf *Raft) requestVote(serverNo int, args *RequestVoteArgs, voteCount *int32) {
	DPrintf("[%v]Send Request Vote RPC To %d", rf.getServerDetail(), serverNo)
	reply := &RequestVoteReply{}
	ok := rf.peers[serverNo].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("[%v]Send Request Vote To %d Timeout.", rf.getServerDetail(), serverNo)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.getRole() != CANDIDATE || args.Term != rf.currentTerm {
		DPrintf("[%v]Stop Election", rf.getServerDetail())
		return
	}

	if reply.VoteGranted == false {
		DPrintf("[%v]%d Not Vote", rf.getServerDetail(), serverNo)
		if reply.Term > rf.getCurrentTerm() {
			DPrintf("[%s]Server %d's Term>CurrentTerm, Back To Follower", rf.getServerDetail(), serverNo)
			rf.turnToFollower(reply.Term, -1)
			rf.persist()
		}
		return
	}

	DPrintf("[%v]%d Vote", rf.getServerDetail(), serverNo)
	if atomic.AddInt32(voteCount, -1) == 0 {
		DPrintf("[%v]Get Majority Vote, Become Leader", rf.getServerDetail())
		rf.turnToLeader()
		rf.persist()
		go rf.sendHeartbeat()
		return
	}
}
