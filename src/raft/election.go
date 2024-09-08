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

func (rf *Raft) buildRequestVoteArgs(term int32) *RequestVoteArgs {
	lastLogIdx := int32(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	return &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
	}
}

// 实现Follower投票规则
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("[%s]Before Get Vote Request:%v", rf.getServerDetail(), *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%s]Get Vote Request:%v", rf.getServerDetail(), *args)

	currentTerm := rf.getCurrentTerm()
	reply.Term = currentTerm
	reply.VoteGranted = false

	if args.Term > currentTerm {
		rf.turnToFollower(args.Term, -1)
		rf.persist()
		reply.Term = args.Term
	} else if args.Term < currentTerm {
		DPrintf("[%s]RequestVote Term Too Small", rf.getServerDetail())
		return
	} else if rf.getVoteFor() != -1 {
		DPrintf("[%s]RequestVote Already Voted", rf.getServerDetail())
		return
	}

	lastLogIdx, lastLogTerm := int32(0), int32(0)
	logLen := len(rf.log)
	if logLen != 0 {
		lastLogIdx = int32(logLen - 1)
		lastLogTerm = rf.log[lastLogIdx].Term
	}
	if !compareLog(args.LastLogIndex, args.LastLogTerm,
		lastLogIdx, lastLogTerm) {
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
	times := 0
	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())

		times++
		DPrintf("[%v]%dth Check Timtout.", rf.getServerDetail(), times)

		if rf.isLeader() {
			continue
		}

		if rf.getElectionTimer() == 1 {
			DPrintf("[%s]Election Timout.", rf.getServerDetail())
			rf.setRole(CANDIDATE)
			go rf.startElection()
		}
		rf.enableElectionTimer()
	}

	DPrintf("[%s]Disconnect From Cluster", rf.getServerDetail())
}

func (rf *Raft) startElection() {
	//DPrintf("[%s]Before Election Lock", rf.getServerDetail())
	rf.mu.Lock()
	//DPrintf("[%s]After Election Lock", rf.getServerDetail())
	if rf.getRole() != CANDIDATE {
		DPrintf("[%s]Role!=Candidate, Stop New Election", rf.getServerDetail())
		rf.mu.Unlock()
		return
	}

	term := rf.incCurrentTerm()
	rf.setVoteFor(rf.me)
	rf.persist()

	rf.resetElectionTimer()
	args := rf.buildRequestVoteArgs(term)
	rf.mu.Unlock()

	DPrintf("[%s]Timeout!!! Start New Election", rf.getServerDetail())
	voteReplyChan := make(chan *RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		DPrintf("[%v]Send Request Vote To %d", rf.getServerDetail(), idx)
		go rf.getRequestVote(idx, args, voteReplyChan)
	}

	voteCount, replyNum := int32(0), len(rf.peers)-1
	for replyNum > 0 && !rf.killed() &&
		rf.getRole() == CANDIDATE &&
		term == rf.getCurrentTerm() {
		replyNum--
		select {
		case reply := <-voteReplyChan:
			rf.mu.Lock()
			if reply.VoteGranted == true {
				DPrintf("[%v]%d Vote", rf.getServerDetail(), reply.ServerIDx)
				voteCount++
				if voteCount >= rf.majority && rf.getRole() == CANDIDATE {
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
					DPrintf("[%s]startElection %d Term>CurrentTerm, Back To Follower", rf.getServerDetail(), reply.ServerIDx)
					rf.turnToFollower(reply.Term, reply.ServerIDx)
					rf.persist()
				}
			}
			rf.mu.Unlock()
		}
	}

	DPrintf("[%s]Term:%d Not Get Majority Vote", rf.getServerDetail(), term)
}

func (rf *Raft) getRequestVote(idx int, args *RequestVoteArgs, voteReplies chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	ok := rf.peers[idx].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("[%v]Send Request Vote To %d Timeout.", rf.getServerDetail(), idx)
		reply.VoteGranted = false
	}
	reply.ServerIDx = int32(idx)
	voteReplies <- reply
}
