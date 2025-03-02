package raft

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type SnapShot interface {
	available() bool
	getSnapshotInfo() (int32, int32)
	lastIncludeIndex() int32
	lastIncludeTerm() int32
	data() []byte
	setSnapshot(int32, int32, []byte) bool
	getSnapshot() (int32, int32, []byte)
}

type Log interface {
	setLog([]LogEntry)

	getOne(int32, *LogEntry) error
	getRange(int32, int32) []LogEntry
	getAll() []LogEntry
	len() int

	deleteBefore(int32)
	deleteAfter(int32)
	appendOne(*LogEntry)
	appendSlice([]LogEntry)
}

type LogEntry struct {
	Index   int32
	Term    int32
	Command interface{}
}

func (entry *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", entry.Term, entry.Command)
}

type AppendEntriesRequest struct {
	Term         int32
	Leader       int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

func (args *AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d Leader:%v LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Leader, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesResponse struct {
	Term    int32
	Success bool
	XTerm   int32
	XIndex  int32
	XLen    int32
}

func (reply *AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%d Success:%v XTerm:%d XIndex:%d XLen:%d}",
		reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

type replicationState struct {
	id              int // Follower's ID
	leaderId        int32
	currentTerm     int32
	nextIndex       int32
	stepDown        *chan struct{}
	stopChan        chan struct{}
	dispatchLogChan chan struct{}

	commitment *commitment
}

type commitment struct {
	mu               sync.RWMutex
	startCommitIndex int32
	commitIndex      int32
	matchIndex       []int32
	commitCh         chan struct{}
}

func (c *commitment) getCommitIdx() int32 {
	return atomic.LoadInt32(&c.commitIndex)
}

func (c *commitment) setCommitIdx(newCommitIdx int32) {
	atomic.StoreInt32(&c.commitIndex, newCommitIdx)
}

func (c *commitment) setMatchIdx(serverNo int, matchIndex int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.matchIndex[serverNo] = max(c.matchIndex[serverNo], matchIndex)
}

func (c *commitment) matches(idx int, matchIdx int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if matchIdx > c.matchIndex[idx] {
		c.matchIndex[idx] = matchIdx
		c.recalculate()
	}
}

func (c *commitment) recalculate() {
	matched := make([]int32, len(c.matchIndex))
	copy(matched, c.matchIndex)
	sort.Sort(int32Slice(matched))
	newCommitIdx := matched[(len(matched)-1)/2]
	DPrintf("%v, %v, %v\n", matched, c.getCommitIdx(), newCommitIdx)
	if newCommitIdx > c.getCommitIdx() {
		c.setCommitIdx(newCommitIdx)
		asyncNotifyCh(c.commitCh)
	}
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	if !rf.IsLeader() {
		return -1, -1, false
	}
	rf.newLogMu.Lock()
	defer rf.newLogMu.Unlock()

	logEntry := &LogEntry{
		Index:   rf.getLastLogIndex() + 1,
		Term:    rf.getCurrentTerm(),
		Command: command,
	}

	rf.log.appendOne(logEntry)
	DPrintf("[%v]Append New Log, LastLogIdx:%d, LogLen:%d", rf.getServerDetail(), logEntry.Index, rf.log.len())

	rf.setLastLog(logEntry.Index, logEntry.Term)
	rf.persist()

	rf.commitment.setMatchIdx(int(rf.me), logEntry.Index)
	asyncNotifyCh(rf.newLogCh)
	return int(logEntry.Index), int(logEntry.Term), true
}

func (rf *Raft) buildAppendEntriesArgs(req *AppendEntriesRequest, s *replicationState, lastIdx int32) {
	DPrintf("[%v]Build AppendEntry For %d, NextIndex:%d",
		rf.getServerDetail(), s.id, s.nextIndex)
	req.Entries = rf.log.getRange(s.nextIndex, lastIdx)
	if req.Entries == nil {
		return
	}
	req.Leader = s.leaderId
	req.Term = s.currentTerm
	req.LeaderCommit = rf.commitment.getCommitIdx()
	req.PrevLogIndex = s.nextIndex - 1
	req.PrevLogTerm = rf.getLogTermByIdx(req.PrevLogIndex)
}

func (rf *Raft) applyLog() {
	// Apply Snapshot
	lastIdx := rf.snapshot.lastIncludeIndex()
	DPrintf("[%v]lastIdx:%d, lastApplied:%d", rf.getServerDetail(), lastIdx, rf.lastApplied)
	if lastIdx > rf.lastApplied {
		msg := ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: int(lastIdx),
			SnapshotTerm:  int(rf.snapshot.lastIncludeTerm()),
			Snapshot:      rf.snapshot.data(),
		}
		select {
		case rf.applyCh <- msg:
		case <-rf.shutdownCh:
			DPrintf("[%v]Raft Shutdown, Stop Send ApplyMsg", rf.getServerDetail())
			return
		}
		rf.lastApplied = lastIdx
		DPrintf("[%v]Using Snapshot Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
	}

	// Apply Log
	commitIdx := rf.commitment.getCommitIdx()
	DPrintf("[%v]CommitIdx:%d, LastApplied:%d", rf.getServerDetail(), commitIdx, rf.lastApplied)
	if commitIdx > rf.lastApplied {
		entries := rf.log.getRange(rf.lastApplied+1, commitIdx)
		DPrintf("[%v]Send [%d~%d] Log To Tester", rf.getServerDetail(),
			entries[0].Index, entries[len(entries)-1].Index)
		msg := ApplyMsg{CommandValid: true}
		for _, entry := range entries {
			DPrintf("[%v]Send Log %d To Tester", rf.getServerDetail(), entry.Index)
			msg.CommandIndex, msg.Command = int(entry.Index), entry.Command
			select {
			case rf.applyCh <- msg:
			case <-rf.shutdownCh:
				DPrintf("[%v]Raft Shutdown, Stop Send ApplyMsg", rf.getServerDetail())
				return
			}
		}
		rf.lastApplied = commitIdx
		DPrintf("[%v]Using LogEntry Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
	}
}

func (rf *Raft) sendHeartbeat(s *replicationState) {
	msg := fmt.Sprintf("Send Heartbeat to %d", s.id)
	DPrintf("[%v]Start %s", rf.getServerDetail(), msg)
	defer DPrintf("[%v]Stop %s", rf.getServerDetail(), msg)

	for {
		select {
		case <-s.stopChan:
			return
		case <-time.After(time.Millisecond * 100):
			DPrintf("[%v]%s", rf.getServerDetail(), msg)
			req := &AppendEntriesRequest{
				Term:         s.currentTerm,
				Leader:       s.leaderId,
				LeaderCommit: rf.commitment.getCommitIdx(),
			}
			res := &AppendEntriesResponse{}
			DPrintf("[%v]%v, Complete", rf.getServerDetail(), msg)
			// 不要求一定发送成功
			rf.goFunc(func() { rf.sendAppendEntries(s.id, req, res) }, msg)
		}
	}
}

func (rf *Raft) handleAppendEntriesRPC(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	DPrintf("[%v]Start Handle AppendEntries RPC:%v, logLen:%d\n", rf.getServerDetail(), req, rf.log.len())
	defer DPrintf("[%v]Complete Handle AppendEntries RPC:%v, logLen:%d\n", rf.getServerDetail(), req, rf.log.len())

	myTerm := rf.getCurrentTerm()
	res.Success, res.Term = false, myTerm
	if req.Term < myTerm {
		return
	}

	rf.setLastContact()

	if req.Term > myTerm || rf.getRole() != FOLLOWER {
		rf.turnToFollower(req.Term, req.Leader)
		res.Term = req.Term
		rf.persist()
	}

	lastLogIdx, lastLogTerm := rf.getLastLog()
	// 如果Log中没有req.PrevLogIndex的对应项,就返回错误
	if req.PrevLogIndex > 0 {
		var prevLogTerm int32
		if req.PrevLogIndex == lastLogIdx {
			prevLogTerm = lastLogTerm
		} else {
			var prevLogEntry LogEntry
			if err := rf.log.getOne(req.PrevLogIndex, &prevLogEntry); err != nil {
				DPrintf("[%v]req.PrevLogIndex[%d] > lastLogIdx[%d]", rf.getServerDetail(), req.PrevLogIndex, lastLogIdx)
				res.XIndex, res.XTerm = -1, -1
				res.XLen = lastLogIdx + 1
				return
			}
			prevLogTerm = prevLogEntry.Term
		}

		// 日志不匹配, 返回属于冲突Term(prevLogTerm)的最早LogEntry的index
		if req.PrevLogTerm != prevLogTerm {
			DPrintf("[%v]Log Not Match, req.prevLogIdx:%d, req.prevLogTerm:%d, realPrevLogTerm:%d",
				rf.getServerDetail(), req.PrevLogIndex, req.PrevLogTerm, prevLogTerm)
			var log, prevLog LogEntry
			for prevLogIdx := req.PrevLogIndex; ; prevLogIdx-- {
				err := rf.log.getOne(prevLogIdx, &prevLog)
				if err != nil || prevLog.Term < prevLogTerm {
					break
				}
				log = prevLog
			}
			res.XIndex, res.XTerm = log.Index, prevLogTerm
			DPrintf("[%v]res.XIndex:%d, res.XTerm:%d", rf.getServerDetail(), res.XIndex, res.XTerm)
			return
		}
	}

	var newEntries []LogEntry
	if len(req.Entries) > 0 {
		for idx, entry := range req.Entries {
			if entry.Index > lastLogIdx {
				newEntries = req.Entries[idx:]
				break
			}
			var storeEntry LogEntry
			if err := rf.log.getOne(entry.Index, &storeEntry); err != nil {
				DPrintf("[%v]Failed to get log entry index %d",
					rf.getServerDetail(), entry.Index)
				return
			}
			if entry.Term != storeEntry.Term {
				rf.log.deleteAfter(entry.Index)
				newEntries = req.Entries[idx:]
				break
			}
		}

		if len(newEntries) != 0 {
			rf.log.appendSlice(newEntries)
			lastLog := newEntries[len(newEntries)-1]
			rf.setLastLog(lastLog.Index, lastLog.Term)
			DPrintf("[%v]Append %d Log, LastLogIndex:%d, logLen:%d",
				rf.getServerDetail(), len(newEntries),
				rf.getLastLogIndex(), rf.log.len())
			rf.persist()
		}
	}

	// 更新CommitIndex, 要Term相同才Apply日志(Follower同样要遵循)
	newCommitIndex := min(req.LeaderCommit, rf.getLastLogIndex())
	DPrintf("req.LeaderCommit:%d, lastLogIndex:%d", newCommitIndex, rf.getLastLogIndex())
	DPrintf("newCommitIndex:%d, CommitIdx:%d, term:%d, logTerm:%d", newCommitIndex, rf.commitment.getCommitIdx(), rf.getCurrentTerm(), rf.getLogTermByIdx(newCommitIndex))
	if newCommitIndex > rf.commitment.getCommitIdx() &&
		rf.getCurrentTerm() == rf.getLogTermByIdx(newCommitIndex) {
		rf.commitment.setCommitIdx(newCommitIndex)
		asyncNotifyCh(rf.commitment.commitCh)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	res.Success = true
	DPrintf("[%v]AppendEntries Success, logLen:%d\n", rf.getServerDetail(), rf.log.len())
}

func (rf *Raft) logDispatcher(s *replicationState) {
	DPrintf("[%v]Start Log Dispatcher For %d", rf.getServerDetail(), s.id)
	defer DPrintf("[%v]Stop Log Dispatcher For %d", rf.getServerDetail(), s.id)

	rf.goFunc(func() { rf.sendHeartbeat(s) }, "sendHeartbeat")

	for {
		select {
		case <-s.stopChan:
			return
		case <-s.dispatchLogChan:
			rf.syncLogWithFollower(s, rf.getLastLogIndex())
		}
	}
}

func (rf *Raft) syncLogWithFollower(s *replicationState, lastIdx int32) {
	DPrintf("[%v]Sync Log With Follower %d, lastIdx:%d", rf.getServerDetail(), s.id, lastIdx)
	defer DPrintf("[%v]Complete Sync Log With Follower %d, lastIdx:%d", rf.getServerDetail(), s.id, lastIdx)

START:
	req := &AppendEntriesRequest{}
	if rf.buildAppendEntriesArgs(req, s, lastIdx); req.Entries == nil {
		goto SEND_SNAP
	}
	rf.sendLogToFollower(s, req)

CHECK_MORE:
	select {
	case <-s.stopChan:
		return
	default:
	}

	if s.nextIndex <= lastIdx {
		goto START
	}
	return

SEND_SNAP:
	rf.sendSnapshotToFollower(s)
	goto CHECK_MORE
}

func (rf *Raft) sendLogToFollower(s *replicationState, req *AppendEntriesRequest) {
	if len(req.Entries) == 0 {
		DPrintf("[%v]Stop Sync Log With %d, No Pending Log.", rf.getServerDetail(), s.id)
		return
	}
	DPrintf("[%v]Send AppendEntries RPC:%v To Follower:%v\n", rf.getServerDetail(), req, s.id)

	var res *AppendEntriesResponse
	for {
		if !rf.IsLeader() || rf.killed() {
			return
		}
		res = &AppendEntriesResponse{}
		if rf.sendRPC(s.id, req, res) {
			break
		}
		time.Sleep(time.Millisecond * 100)
		DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), s.id)
	}

	if res.Success {
		DPrintf("[%v]Success Send %d Log Entry To %d", rf.getServerDetail(), len(req.Entries), s.id)
		if len(req.Entries) != 0 {
			index := req.PrevLogIndex + int32(len(req.Entries))
			s.nextIndex = index + 1
			s.commitment.matches(s.id, index)
			DPrintf("[%v]Update %d nextIndex To %d, CommitIndex To %d",
				rf.getServerDetail(), s.id, s.nextIndex, rf.commitment.getCommitIdx())
		}
		return
	}

	DPrintf("[%v]Failed Send %d Log Entry To %d", rf.getServerDetail(), len(req.Entries), s.id)

	// 接下来都是reply.Success = false
	if res.Term > rf.getCurrentTerm() {
		DPrintf("[%v]Follower:%d Term > My Term, Back To Follower\n", rf.getServerDetail(), s.id)
		rf.turnToFollower(res.Term, -1)
		rf.persist()
		asyncNotifyCh(*s.stepDown)
		return
	}

	if res.XTerm == -1 && res.XIndex == -1 {
		// Follower日志比Leader短
		s.nextIndex = res.XLen
		DPrintf("[%v]Leader's Log Is Longer Than Follower, Decrease To %d", rf.getServerDetail(), res.XLen)
	} else {
		var entry, prevEntry LogEntry
		for idx := rf.getLastLogIndex(); idx >= 0; idx-- {
			err := rf.log.getOne(idx, &entry)
			if err != nil || entry.Term < res.XTerm {
				break
			}
			prevEntry = entry
		}
		if prevEntry.Term == res.XTerm {
			s.nextIndex = prevEntry.Index
		} else {
			s.nextIndex = res.XIndex
		}
		DPrintf("[%v]s.nextIndex:%d, logLen:%d", rf.getServerDetail(), s.nextIndex, rf.log.len())
	}
}
