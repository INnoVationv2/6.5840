package raft

import "time"

func (rf *Raft) sendRPC(target int, req interface{}, res interface{}) bool {
	rpcCh := make(chan bool, 1)
	rf.goFunc(func() {
		ok := false
		switch req.(type) {
		case *RequestVoteRequest:
			ok = rf.sendRequestVote(target, req.(*RequestVoteRequest), res.(*RequestVoteResponse))
		case *AppendEntriesRequest:
			ok = rf.sendAppendEntries(target, req.(*AppendEntriesRequest), res.(*AppendEntriesResponse))
		case *InstallSnapshotRequest:
			ok = rf.sendInstallSnapshot(target, req.(*InstallSnapshotRequest), res.(*InstallSnapshotResponse))
		}
		rpcCh <- ok
	}, "SendRPC")

	// 300ms没有收到结果就返回
	select {
	case ok := <-rpcCh:
		return ok
	case <-time.After(300 * time.Millisecond):
		return false
	}
}

func (rf *Raft) sendRequestVote(target int, req *RequestVoteRequest, res *RequestVoteResponse) bool {
	return rf.peers[target].Call("Raft.RequestVote", req, res)
}

func (rf *Raft) sendAppendEntries(target int, req *AppendEntriesRequest, res *AppendEntriesResponse) bool {
	return rf.peers[target].Call("Raft.AppendEntries", req, res)
}

func (rf *Raft) sendInstallSnapshot(target int, req *InstallSnapshotRequest, res *InstallSnapshotResponse) bool {
	return rf.peers[target].Call("Raft.InstallSnapshot", req, res)
}

func (rf *Raft) RequestVote(req *RequestVoteRequest, res *RequestVoteResponse) {
	if rf.killed() {
		return
	}
	DPrintf("[%v]Receive Request Vote RPC:%v", rf.getServerDetail(), req)
	rf.handleRpc(req, res)
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	if rf.killed() {
		return
	}
	DPrintf("[%v]Receive Append Entries RPC:%v", rf.getServerDetail(), req)
	rf.handleRpc(req, res)
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, res *InstallSnapshotResponse) {
	if rf.killed() {
		return
	}
	DPrintf("[%v]Receive Install Snapshot RPC:%v", rf.getServerDetail(), req)
	rf.handleRpc(req, res)
}

type RPC struct {
	req       interface{}
	res       interface{}
	replyChan chan struct{}
}

func (rf *Raft) handleRpc(req interface{}, res interface{}) {
	rpc := &RPC{
		req:       req,
		res:       res,
		replyChan: make(chan struct{}, 1),
	}

	select {
	case rf.rpcCh <- rpc:
	case <-rf.shutdownCh:
	}

	select {
	case <-rpc.replyChan:
	case <-rf.shutdownCh:
	}
	return
}
