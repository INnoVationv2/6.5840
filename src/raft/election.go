package raft

import (
	"fmt"
)

type RequestVoteRequest struct {
	Term         int32
	CandidateId  int32
	LastLogIndex int32
	LastLogTerm  int32
}

func (req *RequestVoteRequest) String() string {
	return fmt.Sprintf("{Term:%d CandidateId:%d LastLogIndex:%d LastLogTerm:%d}",
		req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
}

type voteResult struct {
	RequestVoteResponse
	voterId int
}

type RequestVoteResponse struct {
	Term        int32
	VoteGranted bool
}

func (res *RequestVoteResponse) String() string {
	return fmt.Sprintf("{Term:%d VoteGranted:%v}", res.Term, res.VoteGranted)
}

func (rf *Raft) handleRequestVoteRPC(req *RequestVoteRequest, res *RequestVoteResponse) {
	DPrintf("[%s]Received Vote Request RPC:%v", rf.getServerDetail(), *req)

	currentTerm := rf.getCurrentTerm()
	res.Term = currentTerm
	res.VoteGranted = false
	candidateId := req.CandidateId

	if req.Term < currentTerm {
		DPrintf("[%s]Candidate:%d Term:%d < My Term:%d, Not Vote", rf.getServerDetail(), candidateId, req.Term, currentTerm)
		return
	}

	if req.Term > currentTerm {
		DPrintf("[%v]Candidate:%d Term:%d > My Term:%d, Turn To Follower", rf.getServerDetail(), candidateId, req.Term, currentTerm)
		rf.turnToFollower(req.Term, -1)
		rf.persist()
		res.Term = req.Term
	} else if rf.votedFor != -1 && rf.votedFor != req.CandidateId {
		DPrintf("[%s]Already Vote To %d In Term:%d", rf.getServerDetail(), rf.votedFor, currentTerm)
		return
	}

	// 比较日志，只投给日志至少和自己一样新的Candidate
	//lastLogIdx, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	//if !compareLog(req.LastLogIndex, req.LastLogTerm, lastLogIdx, lastLogTerm) {
	//	DPrintf("[%s]Candidate:%d Log Is Too Old", rf.getServerDetail(), req.CandidateId)
	//	return
	//}

	// 如果投票给对方，就重置选举超时器，防止冲突
	res.VoteGranted = true
	rf.votedFor = req.CandidateId
	rf.persist()
	rf.updateLastContact()
	DPrintf("[%s]Vote To %d", rf.getServerDetail(), rf.votedFor)
}

//
//func (rf *Raft) ticker() {
//	// For Crash Recover Use
//	if rf.role == LEADER {
//		rf.turnToLeader()
//	}
//	DPrintf("[%s]Join To Cluster", rf.getServerDetail())
//	for {
//		time.Sleep(getRandomTimeoutMs())
//		if rf.killed() {
//			break
//		}
//		if rf.isLeader() {
//			continue
//		}
//
//		DPrintf("[%v]Check Timtout.", rf.getServerDetail())
//		if rf.getElectionTimer() == 0 {
//			DPrintf("[%v]Not Timeout", rf.getServerDetail())
//			rf.startElectionTimer()
//			continue
//		}
//
//		DPrintf("[%s]Election Timout.", rf.getServerDetail())
//		rf.mu.Lock()
//		rf.setRole(CANDIDATE)
//		rf.incCurrentTerm()
//		rf.votedFor = rf.me
//		rf.closeElectionTimer()
//		rf.persist()
//		requestVoteArgs := rf.buildRequestVoteArgs()
//		go rf.startElection(requestVoteArgs)
//		rf.mu.Unlock()
//	}
//
//	DPrintf("[%s]Disconnect From Cluster", rf.getServerDetail())
//}
//
//func (rf *Raft) startElection(req *RequestVoteRequest) {
//	DPrintf("[%s]Timeout!!! Start New Election", rf.getServerDetail())
//	voteCount := rf.majority
//	for serverNo := range rf.peers {
//		if int32(serverNo) == rf.me {
//			continue
//		}
//		rf.requestVote(serverNo, req, &voteCount)
//	}
//}

func (rf *Raft) electSelf() <-chan *voteResult {
	newTerm := rf.getCurrentTerm() + 1
	rf.setCurrentTerm(newTerm)
	rf.votedFor = rf.me

	rf.logMu.RLock()
	req := &RequestVoteRequest{
		Term:         newTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.logMu.RUnlock()

	voteCh := make(chan *voteResult, len(rf.peers))

	askVote := func(peerNo int) {
		rf.goFunc(func() {
			DPrintf("[%v]Request %d's Vote", rf.getServerDetail(), peerNo)
			res := &voteResult{voterId: peerNo}
			ok := rf.sendRequestVote(peerNo, req, &res.RequestVoteResponse)
			if !ok {
				DPrintf("[%v]Request Vote From %d Timeout.", rf.getServerDetail(), peerNo)
				res.Term = req.Term
				res.VoteGranted = false
			}
			voteCh <- res
		}, "AskVote")
	}

	for serverNo := range rf.peers {
		if serverNo == int(rf.me) {
			continue
		}
		askVote(serverNo)
	}

	return voteCh
}
