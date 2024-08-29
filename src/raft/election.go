package raft

import (
	"fmt"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int32
	CandidateId int32
}

func (rf *Raft) buildRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:        rf.getCurrentTerm(),
		CandidateId: rf.me,
	}
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int32
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v %d %d_%d]Get Vote Request:%v", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me, *args)

	rf.setHeartbeat(true)
	vote := true
	if args.Term < rf.getCurrentTerm() {
		vote = false
	} else if args.Term > rf.getCurrentTerm() {
		vote = true
		if rf.getRole() != Follower {
			rf.setRole(Follower)
			DPrintf("[%v %d %d_%d]RequestVote Back To Follower\n", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
		}
	} else {
		vote = rf.getVoteFor() == -1
	}

	log := fmt.Sprintf("[%v %d %d_%d]Get Vote Request:%v, ", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me, *args)
	if vote {
		rf.setCurrentTerm(args.Term)
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

// 随机超时时间
func (rf *Raft) ticker() {
	DPrintf("[%v %d %d_%d]Join To Cluster\n", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)

	for !rf.killed() {
		time.Sleep(getRandomTimeoutMs())
		if rf.getRole() == Leader {
			continue
		}

		DPrintf("[%v %d %d_%d]Check If Timeout\n", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
		if rf.getHeartbeat() == false || rf.getVoteFor() == -1 {
			rf.setRole(Candidate)
			go rf.startElection()
		}
		rf.setHeartbeat(false)
	}

	DPrintf("Server %d_%d Disconnect From Cluster\n", rf.name, rf.me)
}

// 开始新选举
func (rf *Raft) startElection() {
	if rf.getRole() != Candidate {
		return
	}
	rf.mu.Lock()
	rf.setVoteFor(rf.me)
	term := rf.incCurrentTerm()
	args := rf.buildRequestVoteArgs()
	rf.mu.Unlock()

	DPrintf("[%v %d %d_%d]Start New Election\n", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
	voteReplyChan := make(chan *RequestVoteReply, len(rf.peers))
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		DPrintf("[%v %d %d_%d]Send Request Vote To %d\n", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me, idx)
		go rf.getRequestVote(idx, args, voteReplyChan)
	}

	voteCount, replyNum := int32(1), 0
	for replyNum < len(rf.peers)-1 && !rf.killed() && rf.getRole() == Candidate && term == rf.getCurrentTerm() {
		replyNum++
		select {
		case reply := <-voteReplyChan:
			if reply.VoteGranted == true {
				voteCount++
				if voteCount > int32(len(rf.peers)>>1) {
					rf.mu.Lock()
					rf.setRole(Leader)
					rf.setLeaderId(rf.me)
					rf.mu.Unlock()
					DPrintf("[%v %d %d_%d]Get Majority Vote, Become Leader\n",
						rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
					go rf.sendHeartbeat()
					return
				}
			}
		}
	}

	DPrintf("[Server %d_%d]Not Get Majority Vote\n", rf.name, rf.me)
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
