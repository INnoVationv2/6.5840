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
}

func (rf *Raft) buildRequestVoteArgs(term, lastLogIdx, LastLogTerm int32) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  LastLogTerm,
	}
}

// 实现Follower投票规则
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%s]Get Vote Request:%v\n", rf.getServerDetail(), *args)

	currentTerm := rf.getCurrentTerm()
	reply.Term = currentTerm
	reply.VoteGranted = false

	if args.Term > currentTerm {
		rf.turnToFollower(args.Term)
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
	DPrintf("[%s]Vote", rf.getServerDetail())
}

func (rf *Raft) ticker() {
	DPrintf("[%s]Join To Cluster\n", rf.getServerDetail())

	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())
		if rf.isLeader() {
			continue
		}

		if rf.getElectionTimer() == 1 {
			rf.setRole(CANDIDATE)
			go rf.startElection()
		}
		rf.enableElectionTimer()
	}

	DPrintf("[%s]Disconnect From Cluster\n", rf.getServerDetail())
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.getRole() != CANDIDATE {
		rf.mu.Unlock()
		return
	}

	term := rf.incCurrentTerm()
	rf.setVoteFor(rf.me)
	rf.resetElectionTimer()

	lastLogIdx := int32(len(rf.log) - 1)
	lastLogTerm := rf.log[lastLogIdx].Term
	args := rf.buildRequestVoteArgs(term, lastLogIdx, lastLogTerm)
	rf.mu.Unlock()

	DPrintf("[%s]Timeout!!! Start New Election\n", rf.getServerDetail())
	voteReplyChan := make(chan *RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		DPrintf("[%v]Send Request Vote To %d\n", rf.getServerDetail(), idx)
		go rf.getRequestVote(idx, args, voteReplyChan)
	}

	voteCount, replyNum := int32(0), len(rf.peers)-1
	for replyNum > 0 && !rf.killed() &&
		rf.getRole() == CANDIDATE &&
		term == rf.getCurrentTerm() {
		replyNum--
		select {
		case reply := <-voteReplyChan:
			if reply.VoteGranted == false {
				if reply.Term > rf.getCurrentTerm() {
					rf.mu.Lock()
					DPrintf("[%s]startElection Reply Term>CurrentTerm, Back To Follower\n", rf.getServerDetail())
					rf.turnToFollower(reply.Term)
					rf.mu.Unlock()
				}
				continue
			}
			voteCount++
			if voteCount >= rf.majority {
				rf.turnToLeader()
				DPrintf("[%s]Get Majority Vote, Become Leader\n", rf.getServerDetail())
				go rf.sendHeartbeat()
				return
			}
		}
	}

	DPrintf("[%s]Not Get Majority Vote\n", rf.getServerDetail())
}

func (rf *Raft) getRequestVote(idx int, args *RequestVoteArgs, voteReplies chan *RequestVoteReply) {
	reply := &RequestVoteReply{}
	ok := rf.peers[idx].Call("Raft.RequestVote", args, reply)
	if !ok {
		DPrintf("[%v]Send Request Vote To %d Timeout.\n", rf.getServerDetail(), idx)
		reply.VoteGranted = false
	}
	voteReplies <- reply
}
