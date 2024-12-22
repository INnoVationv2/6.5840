package raft

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TIMEOUT = iota
	ERROR
	COMPLETE
	SNAPSHOT_COMPLETE

	ERR_NOT_LEADER
	SUCCESS_APPEND_LOG
)

type LogFuture struct {
	logEntry *LogEntry
	status   int32
}

func (logFuture *LogFuture) setStatus(status int32) {
	atomic.StoreInt32(&logFuture.status, status)
}

func (logFuture *LogFuture) getStatus() int32 {
	return atomic.LoadInt32(&logFuture.status)
}

type SnapShot interface {
	lastIncludeIndex() int32
	lastIncludeTerm() int32
	data() []byte
	setSnapshot(int32, int32, []byte)
}

type Log interface {
	setLog([]LogEntry)

	getOne(int32) *LogEntry
	getRange(int32, int32) []LogEntry
	getAll() []LogEntry
	getLast() *LogEntry
	getLen() int

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
	id          int // Follower's ID
	leaderId    int32
	currentTerm int32
	nextIndex   int32
	//stopChan        chan struct{}
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
	logFuture := &LogFuture{
		logEntry: &LogEntry{
			Command: command,
		},
		status: 0,
	}
	rf.newLogCh <- logFuture
	for logFuture.getStatus() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if logFuture.getStatus() == ERR_NOT_LEADER {
		return -1, -1, false
	}
	return int(logFuture.logEntry.Index), int(logFuture.logEntry.Term), true
}

func (rf *Raft) buildAppendEntriesArgs(req *AppendEntriesRequest, s *replicationState, lastIdx int32) {
	req.Leader = s.leaderId
	req.Term = s.currentTerm
	req.LeaderCommit = rf.commitment.getCommitIdx()

	var prevLogTerm int32
	prevLogIdx := s.nextIndex - 1
	if prevLogIdx <= rf.snapshot.lastIncludeIndex() {
		prevLogTerm = rf.snapshot.lastIncludeTerm()
	} else {
		prevLogTerm = rf.log.getOne(prevLogIdx).Term
	}

	req.PrevLogIndex, req.PrevLogTerm = prevLogIdx, prevLogTerm
	req.Entries = rf.log.getRange(s.nextIndex, lastIdx)

	DPrintf("[%v]Build AppendEntry For %d, NextIndex:%d",
		rf.getServerDetail(), s.id, s.nextIndex)
}

func (rf *Raft) applyLog() {
	// Apply Snapshot
	lastIdx := rf.snapshot.lastIncludeIndex()
	if lastIdx > rf.lastApplied {
		snapshot := ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: int(lastIdx),
			SnapshotTerm:  int(rf.snapshot.lastIncludeTerm()),
			Snapshot:      rf.snapshot.data(),
		}
		rf.lastApplied = lastIdx
		DPrintf("[%v]Using Snapshot Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
		rf.applyCh <- snapshot
	}

	// Apply Log
	commitIdx := rf.commitment.getCommitIdx()
	if commitIdx > rf.lastApplied {
		commitLogs := rf.log.getRange(rf.lastApplied+1, commitIdx)
		rf.lastApplied = commitIdx
		rf.sendLogToTester(commitLogs)
		DPrintf("[%v]Using LogEntry Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
	}
}

func (rf *Raft) sendLogToTester(logs []LogEntry) {
	DPrintf("[%v]Send [%d~%d] Log To Tester", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index)
	msg := ApplyMsg{CommandValid: true}
	for _, log := range logs {
		DPrintf("[%v]Send Log %d To Tester", rf.getServerDetail(), log.Index)
		msg.CommandIndex, msg.Command = int(log.Index), log.Command
		rf.applyCh <- msg
	}
	DPrintf("[%v]Success Send %d~%d Log To Tester", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index)
}

func (rf *Raft) sendHeartbeat(s *replicationState) {
	DPrintf("[%v]Start Send Heartbeat to %d", rf.getServerDetail(), s.id)
	defer DPrintf("[%v]Stop Send Heartbeat to %d", rf.getServerDetail(), s.id)

	for rf.isLeader() {
		select {
		case <-rf.shutdownCh:
			return
		case <-time.After(time.Millisecond * 100):
			DPrintf("[%v]Send Heartbeat to %d", rf.getServerDetail(), s.id)
			req := &AppendEntriesRequest{
				Term:         s.currentTerm,
				Leader:       s.leaderId,
				LeaderCommit: rf.commitment.getCommitIdx(),
			}
			res := &AppendEntriesResponse{}
			DPrintf("[%v]Send Heartbeat to %d, Complete", rf.getServerDetail(), s.id)
			rf.sendRPC(s.id, req, res)
		}
	}
}

func (rf *Raft) handleAppendEntriesRPC(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	DPrintf("[%v]Start Handle AppendEntries RPC:%v, logLen:%d", rf.getServerDetail(), req, rf.log.getLen())
	DPrintf("[%v]Start Handle AppendEntries RPC:%v", rf.getServerDetail(), req)
	myTerm := rf.getCurrentTerm()
	res.Success, res.Term = false, myTerm
	if req.Term < myTerm {
		return
	}

	rf.setLastContact()

	if req.Term > myTerm || rf.getRole() == CANDIDATE {
		rf.turnToFollower(req.Term, req.Leader)
		res.Term = req.Term
		rf.persist()
	}

	// 如果Log中没有req.PrevLogIndex的对应项,就返回错误
	if req.PrevLogIndex > 0 {
		/*
			需要考虑req.PrevLogIndex < rf.snapshot.LastIncludeIndex的情况吗?
			什么情况下会出现这种情况?
			  当旧Leader断线重连，此时已经有了新的Leader, 生成了更新的snapshot,
			  但旧Leader还存有待发送的日志, 就会尝试向Follower发送,
			  但旧Leader的Term肯定小于最新Term,在第一个if处就会被淘汰
			所以不会出现上述情况, 下面的Assert就是进行判断的
		*/
		lastIncludedIdx := rf.snapshot.lastIncludeIndex()
		if lastIncludedIdx != -1 {
			Assert(req.PrevLogIndex >= lastIncludedIdx,
				fmt.Sprintf("req.PrevLogIndex:%d < rf.snapshot.LastIncludedIndex:%d",
					req.PrevLogIndex, lastIncludedIdx))
		}

		lastLogIdx, lastLogTerm := rf.getLastLog()
		var prevLogTerm int32
		if req.PrevLogIndex == lastLogIdx {
			prevLogTerm = lastLogTerm
		} else {
			logEntry := rf.log.getOne(req.PrevLogIndex)
			if logEntry == nil {
				// 没找到prevLogIdx对应的日志
				// 即req.PrevLogIndex > rf.lastLogIndex: 发来的日志太新
				res.XIndex, res.XTerm = -1, -1
				res.XLen = rf.getLastLogIndex() + 1
				return
			}
			prevLogTerm = logEntry.Term
		}

		// 日志不匹配, 返回属于冲突Term的最早LogEntry的index
		if req.PrevLogTerm != prevLogTerm {
			//rf.logMu.RLock()
			prevLogIdx := req.PrevLogIndex
			var log *LogEntry
			for {
				prevLog := rf.log.getOne(prevLogIdx)
				if prevLog == nil || prevLog.Term != prevLogTerm {
					break
				}
				log = prevLog
				prevLogIdx--
			}
			res.XIndex, res.XTerm = log.Index, prevLogTerm
			//rf.logMu.RUnlock()
			DPrintf("Log Not Match")
			return
		}
	}

	if len(req.Entries) != 0 {
		if req.PrevLogIndex != rf.getLastLogIndex() {
			// 可能存在日志冲突, 找到并截断
			i, j := req.PrevLogIndex+1, 0
			for {
				log := rf.log.getOne(i)
				if log == nil {
					break
				} else if log.Term != req.Entries[j].Term {
					rf.log.deleteAfter(i)
					break
				}
				i, j = i+1, j+1
			}
			// 截断args.Entries
			req.Entries = req.Entries[j:]
		}

		Assert(len(req.Entries) != 0, "Entry Shouldn't Be Empty")
		rf.log.appendSlice(req.Entries)

		lastLog := req.Entries[len(req.Entries)-1]
		rf.setLastLog(lastLog.Index, lastLog.Term)

		DPrintf("[%v]Append %d Log, LastLogIndex:%d, logLen:%d",
			rf.getServerDetail(), len(req.Entries),
			rf.getLastLogIndex(), rf.log.getLen())
		rf.persist()
	}

	// 更新CommitIndex, 要Term相同才Apply日志(Follower同样要遵循)
	newCommitIndex := min(req.LeaderCommit, rf.getLastLogIndex())
	log := rf.log.getOne(newCommitIndex)
	if newCommitIndex > rf.commitment.getCommitIdx() &&
		log != nil && req.Term == log.Term {
		rf.commitment.setCommitIdx(newCommitIndex)
		asyncNotifyCh(rf.commitment.commitCh)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	res.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) logDispatcher(s *replicationState) {
	DPrintf("[%v]Start Log Dispatcher For %d", rf.getServerDetail(), s.id)
	defer DPrintf("[%v]Stop Log Dispatcher For %d", rf.getServerDetail(), s.id)

	rf.goFunc(func() { rf.sendHeartbeat(s) }, "sendHeartbeat")

	for rf.isLeader() {
		select {
		case <-rf.shutdownCh:
			return
		case <-s.dispatchLogChan:
			rf.sendLogToFollower(s, rf.getLastLogIndex())
		}
	}
}

func (rf *Raft) sendLogToFollower(s *replicationState, lastIdx int32) {
	DPrintf("[%v]Sync Log With Follower %d", rf.getServerDetail(), s.id)
	req := &AppendEntriesRequest{}
	res := &AppendEntriesResponse{}
	rf.buildAppendEntriesArgs(req, s, lastIdx)
	if len(req.Entries) == 0 {
		DPrintf("[%v]Stop Sync Log With %d, No Pending Log.", rf.getServerDetail(), s.id)
	}

	DPrintf("[%v]Send AppendEntries RPC:%v To Follower:%v\n", rf.getServerDetail(), req, s.id)
	for rf.isLeader() {
		select {
		case <-rf.shutdownCh:
			return
		default:
		}
		if rf.sendRPC(s.id, req, res) {
			break
		}
		DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), s.id)
		time.Sleep(50 * time.Millisecond)
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

	// 接下来都是reply.Success = false
	if res.Term > rf.getCurrentTerm() {
		DPrintf("[%v]Follower:%d Term > My Term, Back To Follower\n", rf.getServerDetail(), s.id)
		rf.turnToFollower(res.Term, -1)
		rf.persist()
		return
	}

	if res.XTerm == -1 && res.XIndex == -1 {
		// Follower日志比Leader短
		s.nextIndex = res.XLen
	} else {
		idx := rf.getLastLogIndex()
		var log *LogEntry
		for {
			prevLog := rf.log.getOne(idx)
			if prevLog == nil || prevLog.Term < res.XTerm {
				break
			}
			log = prevLog
			idx--
		}
		if log.Term == res.XTerm {
			s.nextIndex = log.Index
		} else {
			s.nextIndex = res.XIndex
		}
	}
	DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n",
		rf.getServerDetail(), s.id, s.nextIndex)
	return
}
