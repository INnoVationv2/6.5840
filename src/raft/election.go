package raft

import (
	"fmt"
	"time"
)

type RequestVoteArgs struct {
	Term        int32
	CandidateId int32
}

func (rf *Raft) buildRequestVoteArgs(term int32) *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%s]Get Vote Request:%v", rf.getServerDetail(), *args)

	rf.setHeartbeat(true)
	vote := true
	if args.Term < rf.getCurrentTerm() {
		vote = false
	} else if args.Term > rf.getCurrentTerm() {
		vote = true
		rf.setCurrentTerm(args.Term)
		if rf.getRole() != Follower {
			rf.setRole(Follower)
			DPrintf("[%s]RequestVote Back To Follower\n", rf.getServerDetail())
		}
	} else {
		vote = rf.getVoteFor() == -1
	}

	log := fmt.Sprintf("[%s]Get Vote Request:%v, ", rf.getServerDetail(), *args)
	if vote {
		rf.setVoteFor(args.CandidateId)
		log += "Vote"
	} else {
		log += "Not Vote"
	}
	reply.VoteGranted = vote
	reply.Term = rf.getCurrentTerm()
	DPrintf(log)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ticker() {
	DPrintf("[%s]Join To Cluster\n", rf.getServerDetail())

	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())
		if rf.getRole() == Leader {
			continue
		}

		DPrintf("[%s]Check If Timeout\n", rf.getServerDetail())
		if rf.getHeartbeat() == false || rf.getVoteFor() == -1 {
			rf.setRole(Candidate)
			go rf.startElection()
		}
		rf.setHeartbeat(false)
	}

	DPrintf("[%s] Disconnect From Cluster\n", rf.getServerDetail())
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.getRole() != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.setVoteFor(rf.me)
	term := rf.incCurrentTerm()
	args := rf.buildRequestVoteArgs(term)
	rf.mu.Unlock()

	DPrintf("[%s]Start New Election\n", rf.getServerDetail())
	voteReplyChan := make(chan *RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		DPrintf("[%s]Send Request Vote To %d\n", rf.getServerDetail(), idx)
		go rf.getRequestVote(idx, args, voteReplyChan)
	}

	voteCount, replyNum := int32(1), 0
	for replyNum < len(rf.peers)-1 && !rf.killed() &&
		rf.getRole() == Candidate && term == rf.getCurrentTerm() {
		replyNum++
		select {
		case reply := <-voteReplyChan:
			if reply.VoteGranted == false {
				continue
			}
			voteCount++
			if voteCount > int32(len(rf.peers)>>1) {
				rf.mu.Lock()
				rf.setRole(Leader)
				rf.setLeaderId(rf.me)
				rf.mu.Unlock()
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
	ok := rf.sendRequestVote(idx, args, reply)
	if !ok {
		DPrintf("Send Request Vote To %d Timeout.\n", args.CandidateId)
		reply.VoteGranted = false
	}
	voteReplies <- reply
}
