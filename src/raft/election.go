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
	DPrintf("[%s]Receive Vote Request RPC:%v", rf.getServerDetail(), *req)

	currentTerm := rf.getCurrentTerm()
	res.Term, res.VoteGranted = currentTerm, false

	if req.Term < currentTerm {
		DPrintf("[%s]Candidate[%d]'s Term:%d < My Term:%d, Not Vote",
			rf.getServerDetail(), req.CandidateId, req.Term, currentTerm)
		return
	}
	votedFor := rf.getVotedFor()
	if req.Term == currentTerm &&
		votedFor != -1 && votedFor != req.CandidateId {
		DPrintf("[%s]Already Vote To %d In Term %d",
			rf.getServerDetail(), votedFor, currentTerm)
		return
	}

	persist := false

	if req.Term > currentTerm {
		rf.turnToFollower(req.Term, -1)
		res.Term = req.Term
		persist = true
	}

	// 比较日志，只投给日志至少和自己一样新的Candidate
	lastLogIdx, lastLogTerm := rf.getLastLog()
	if compareLog(req.LastLogIndex, req.LastLogTerm, lastLogIdx, lastLogTerm) {
		// Vote，并重置选举超时器，防止冲突
		res.VoteGranted = true
		rf.setVotedFor(req.CandidateId)
		rf.setLastContact()
		persist = true
		DPrintf("[%s]Vote To %d", rf.getServerDetail(), req.CandidateId)
	} else {
		DPrintf("[%s]Candidate[%v]'s Log is too old, "+
			"Not Vote", rf.getServerDetail(), req.CandidateId)
	}

	if persist {
		rf.persist()
	}
}

func (rf *Raft) electSelf() <-chan *voteResult {
	newTerm := rf.getCurrentTerm() + 1
	rf.setCurrentTerm(newTerm)
	rf.setVotedFor(rf.me)
	rf.persist()

	req := &RequestVoteRequest{
		Term:        newTerm,
		CandidateId: rf.me,
	}
	req.LastLogIndex, req.LastLogTerm = rf.getLastLog()

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
