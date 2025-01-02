package raft

import (
	"fmt"
	"time"
)

type InstallSnapshotRequest struct {
	LeaderId          int32
	Term              int32
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              []byte
}

func (req *InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%d Leader:%v LastIncludedIndex:%d LastIncludedTerm:%d}", req.Term, req.LeaderId, req.LastIncludedIndex, req.LastIncludedTerm)
}

type InstallSnapshotResponse struct {
	Term int32
}

func (res *InstallSnapshotResponse) String() string {
	return fmt.Sprintf("{Term:%d}", res.Term)
}

func (rf *Raft) sendSnapshotToFollower(s *replicationState) {
	DPrintf("[%v]Send Snapshot To %d", rf.getServerDetail(), s.id)
	lastIncludedIdx, lastIncludedTerm, data := rf.snapshot.getSnapshot()
	req := &InstallSnapshotRequest{
		LeaderId:          rf.me,
		Term:              rf.getCurrentTerm(),
		LastIncludedIndex: lastIncludedIdx,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}

	var res *InstallSnapshotResponse
	for {
		if !rf.isLeader() || rf.killed() {
			return
		}
		res = &InstallSnapshotResponse{}
		if rf.sendRPC(s.id, req, res) {
			break
		}
		time.Sleep(time.Millisecond * 100)
		DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), s.id)
	}

	if res.Term > rf.getCurrentTerm() {
		DPrintf("[%v][Snapshot] Follower[%d]'s Term > My Term, Back To Follower\n", rf.getServerDetail(), s.id)
		rf.turnToFollower(res.Term, -1)
		rf.persist()
		asyncNotifyCh(*s.stepDown)
		return
	}

	idx := req.LastIncludedIndex
	s.nextIndex = idx + 1
	rf.commitment.setMatchIdx(s.id, idx)
	DPrintf("[%v]Success Send Snapshot To %d, "+
		"Update NextIndex To %d, MatchIndex To %d", rf.getServerDetail(), s.id, idx+1, idx)
}

func (rf *Raft) handleSnapshotRPC(req *InstallSnapshotRequest, res *InstallSnapshotResponse) {
	DPrintf("[%v]Received Install Snapshot RPC %v", rf.getServerDetail(), req)
	myTerm := rf.getCurrentTerm()
	res.Term = myTerm

	if req.Term < myTerm {
		return
	}

	if req.Term > myTerm || rf.getRole() == CANDIDATE {
		DPrintf("[%v]Received Snapshot From %d, Term[%d] > My Term[%d], Turn to follower", rf.getServerDetail(),
			req.Term, myTerm, req.LeaderId)
		rf.turnToFollower(req.Term, req.LeaderId)
		rf.persist()
	}

	if !rf.snapshot.setSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data) {
		return
	}
	rf.commitment.setCommitIdx(rf.snapshot.lastIncludeIndex())

	idx, term := rf.getLastLog()
	DPrintf("[%v]LastIncludedIndex:%d, LastIncludedTerm:%d, Remove Log Before %d, logLen:%d",
		rf.getServerDetail(), idx, term, req.LastIncludedIndex, rf.log.len())
	rf.log.deleteBefore(req.LastIncludedIndex)
	DPrintf("[%v]After Delete, logLen:%d", rf.getServerDetail(), rf.log.len())

	if rf.log.len() == 0 {
		rf.setLastLog(req.LastIncludedIndex, req.LastIncludedTerm)
		DPrintf("[%v]Update LastIncludedIndex To %d, LastIncludedTerm To %d",
			rf.getServerDetail(), req.LastIncludedIndex, req.LastIncludedTerm)
	}

	rf.persist()
	DPrintf("[%v]Success Install Snapshot, LastLogIdx:%d, Log Length:%d",
		rf.getServerDetail(), rf.snapshot.lastIncludeIndex(), rf.snapshot.lastIncludeTerm())
}

// 只有Send Command到Chan时，该方法才可能被调用
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("[%v]Build Snapshot, Index:%d, Sz:%d", rf.getServerDetail(), index, len(snapshot))

	lastIncludedIndex := int32(index)
	var entry LogEntry
	rf.log.getOne(lastIncludedIndex, &entry)
	if !rf.snapshot.setSnapshot(entry.Index, entry.Term, snapshot) {
		return
	}
	rf.log.deleteBefore(lastIncludedIndex)
	rf.persist()

	DPrintf("[%v]Success Build Snapshot, LastIncludedIndex:%d, LastIncludedTerm:%d, LogSz:%d",
		rf.getServerDetail(), rf.snapshot.lastIncludeIndex(), rf.snapshot.lastIncludeIndex(), rf.log.len())
}
