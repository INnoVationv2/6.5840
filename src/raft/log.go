package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	TIMEOUT = iota
	ERROR
	COMPLETE
)

type LogEntry struct {
	Term    int32
	Command interface{}
}

func (entry *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", entry.Term, entry.Command)
}

func (rf *Raft) buildLogEntry(command interface{}, term int32) *LogEntry {
	return &LogEntry{
		Term:    term,
		Command: command,
	}
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d LeaderId:%v LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
	XTerm   int32
	XIndex  int32
	XLen    int32
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%d Success:%v XTerm:%d XIndex:%d XLen:%d}",
		reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, idx int, heartbeat bool) {
	args.Term = rf.getCurrentTerm()
	args.LeaderId = rf.me
	args.LeaderCommit = rf.getCommitIndex()

	nextIdx := rf.getNextIndex(idx)
	prevLogIdx := nextIdx - 1
	args.PrevLogIndex = prevLogIdx
	args.PrevLogTerm = rf.log[prevLogIdx].Term
	if !heartbeat {
		log := rf.log[nextIdx:]
		args.Entries = make([]LogEntry, len(log))
		copy(args.Entries, log)
	}
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() {
		isLeader = false
		return
	}

	currentTerm := rf.getCurrentTerm()
	logEntry := rf.buildLogEntry(command, currentTerm)
	rf.log = append(rf.log, *logEntry)
	rf.persist()
	DPrintf("[%v]Append Log, logSz:%d", rf.getServerDetail(), len(rf.log))

	go rf.syncLogWithFollower(currentTerm)

	return len(rf.log) - 1, int(currentTerm), true
}

// 发送日志到所有Server，达成一致后对日志提交
func (rf *Raft) syncLogWithFollower(term int32) {
	serverDetail := rf.getServerDetail()
	DPrintf("[%v]Start Sync Log", serverDetail)

	//jobChan := make(chan int)
	majority := rf.majority
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		idx := idx
		go func() {
			var status int
			for !rf.killed() && rf.isLeader() && term == rf.getCurrentTerm() {
				status = rf.sendEntriesToFollower(idx, false)
				// 如果是超时发送失败, 需要持续重试
				if status == TIMEOUT {
					DPrintf("[%v]Send AppendEntries RPC To %d Timeout, ReSending", rf.getServerDetail(), idx)
					continue
				}
				break
			}
			DPrintf("[%v]Send AppendEntries RPC To %d Complete, Status:%v, Killed:%v, isLeader:%v, term:%d",
				rf.getServerDetail(), idx, status, rf.killed(), rf.isLeader(), term)
			if status == COMPLETE {
				if atomic.AddInt32(&majority, -1) == 0 {
					DPrintf("[%v] Update CommitIndex", rf.getServerDetail())
					rf.updateCommitIndex(term)
				}
			}
		}()
	}
}

func (rf *Raft) updateCommitIndex(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() || term != rf.getCurrentTerm() {
		DPrintf("[%v]Sync Log Fail", rf.getServerDetail())
		return
	}

	N := rf.findCommitIndex()
	DPrintf("[%v]N:%d", rf.getServerDetail(), N)
	// 只提交当前任期的日志项
	if N > rf.getCommitIndex() && rf.log[N].Term == rf.getCurrentTerm() {
		rf.setCommitIndex(N)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), N)
	}

	commitIndex := rf.getCommitIndex()
	if commitIndex > rf.lastApplied {
		DPrintf("[%v]Apply %d~%d Tester\n", rf.getServerDetail(), rf.lastApplied+1, commitIndex)
		rf.sendCommitedLogToTester()
	}
}

func (rf *Raft) sendEntriesToFollower(idx int, heartbeat bool) int {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for !rf.killed() {
		rf.mu.Lock()
		if rf.getRole() != LEADER {
			rf.mu.Unlock()
			return ERROR
		}
		rf.buildAppendEntriesArgs(args, idx, heartbeat)
		if !heartbeat && len(args.Entries) == 0 {
			rf.mu.Unlock()
			return ERROR
		}
		rf.resetHeartbeatTimer(idx)
		serverDetail := rf.getServerDetail()
		rf.mu.Unlock()

		DPrintf("[%v]Heartbeat:%v Send AppendEntries RPC:%v to follower: %v\n", serverDetail, heartbeat, args, idx)
		ok := rf.peers[idx].Call("Raft.AcceptAppendEntries", args, reply)
		if !ok {
			DPrintf("[%v]Send AppendEntries RPC To %d Timeout", serverDetail, idx)
			return TIMEOUT
		}

		rf.mu.Lock()
		if rf.killed() || rf.getRole() != LEADER || rf.getCurrentTerm() != args.Term {
			DPrintf("[%v]killed:%v, Role:%v, Term:%d, CurrentTerm:%d", rf.getServerDetail(), rf.killed(), rf.getRoleStr(), args.Term, rf.getCurrentTerm())
			rf.mu.Unlock()
			return ERROR
		}

		if reply.Success {
			DPrintf("[%v]Success Send %d Log Entry To %d", serverDetail, len(args.Entries), idx)
			if len(args.Entries) != 0 {
				index := args.PrevLogIndex + int32(len(args.Entries))
				rf.setNextIndex(idx, max(rf.getNextIndex(idx), index+1))
				rf.setMatchIndex(idx, max(rf.getMatchIndex(idx), index))
				DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d", serverDetail, idx, index+1, index)
			}
			rf.mu.Unlock()
			return COMPLETE
		}
		// 接下来都是reply.Success = false

		if reply.Term > rf.getCurrentTerm() {
			DPrintf("[%v]Follower Term > My Term, Back To Follower\n", serverDetail)
			rf.turnToFollower(reply.Term, -1)
			rf.persist()
			rf.mu.Unlock()
			return ERROR
		}

		if reply.XTerm == -1 && reply.XIndex == -1 {
			// Follower日志比Leader短
			rf.setNextIndex(idx, reply.XLen)
		} else {
			index := max(int32(len(rf.log)-1), 1)
			for index > 1 && rf.log[index].Term > reply.XTerm {
				index--
			}
			if rf.log[index].Term == reply.XTerm {
				rf.setNextIndex(idx, index)
			} else {
				rf.setNextIndex(idx, reply.XIndex)
			}
		}
		DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n", serverDetail, idx, rf.getNextIndex(idx))
		rf.mu.Unlock()
	}
	return ERROR
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]New AppendEntries: %v", rf.getServerDetail(), args)

	term := rf.getCurrentTerm()
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		return
	}

	// 重置选举超时器
	rf.closeElectionTimer()

	if args.Term > term {
		rf.setCurrentTerm(args.Term)
		reply.Term = rf.getCurrentTerm()
		if rf.getRole() != FOLLOWER {
			rf.setRole(FOLLOWER)
			rf.setVoteFor(args.LeaderId)
		}
		rf.persist()
	}
	// args.Term == term
	if rf.getRole() == CANDIDATE {
		rf.setRole(FOLLOWER)
		rf.setVoteFor(args.LeaderId)
		rf.persist()
	}

	// 没有与prevLogIndex、prevLogTerm匹配的项
	// 返回false
	logSz := int32(len(rf.log))
	if args.PrevLogIndex >= logSz ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		log := fmt.Sprintf("logSz:%d", logSz)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = logSz

		// 返回属于冲突term的第一个条目的index
		conflictIdx := args.PrevLogIndex
		if conflictIdx < logSz {
			reply.XTerm = rf.log[conflictIdx].Term
			for conflictIdx > 0 &&
				rf.log[conflictIdx].Term == reply.XTerm {
				conflictIdx--
			}
			reply.XIndex = conflictIdx + 1
		}
		DPrintf("[%v]AppendEntries %v No Matched Log Entry:%v, %v", rf.getServerDetail(), args, reply, log)
		return
	}

	if len(args.Entries) != 0 {
		i, j := int(args.PrevLogIndex+1), 0
		for i < int(logSz) && j < len(args.Entries) {
			entry1, entry2 := rf.log[i], args.Entries[j]
			if entry1.Term != entry2.Term {
				// 日志发生冲突, 截断, 去掉rf.log[i]
				rf.log = rf.log[:i]
				break
			}
			i, j = i+1, j+1
		}
		// 截断args.Entries
		args.Entries = args.Entries[j:]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		DPrintf("[%v]Append %d Log, LogSz:%d", rf.getServerDetail(), len(args.Entries), len(rf.log))
	}

	// 更新CommitIndex, Follower同样要任期相同才提交
	newCommitIndex := min(args.LeaderCommit, int32(len(rf.log))-1)
	if newCommitIndex > rf.getCommitIndex() && args.Term == rf.log[newCommitIndex].Term {
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
		rf.setCommitIndex(newCommitIndex)
		DPrintf("[%v]Apply %d~%d Log To State Machine", rf.getServerDetail(), rf.lastApplied+1, newCommitIndex)
		rf.sendCommitedLogToTester()
	}

	reply.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) sendCommitedLogToTester() {
	msg := ApplyMsg{CommandValid: true}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		idx := rf.lastApplied
		msg.CommandIndex = int(idx)
		msg.Command = rf.log[idx].Command
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendHeartbeat() {
	DPrintf("[%v]Become Leader, Start Send Heartbeat", rf.getServerDetail())
	times := 0
	gap := time.Duration(100) * time.Millisecond
	// 每隔100ms检查，给100ms内没有发送数据的Follower发送心跳
	for !rf.killed() && rf.isLeader() {
		times++
		DPrintf("[%v]%dth Sending Heartbeat", rf.getServerDetail(), times)
		for idx := range rf.peers {
			if idx == int(rf.me) {
				continue
			}
			if rf.heartbeatTimeout(idx) {
				go rf.sendEntriesToFollower(idx, true)
			}
		}
		time.Sleep(gap)
	}
	DPrintf("[%v]Stop Sending Heartbeat, Killed:%v IsLeader:%v", rf.getServerDetail(), rf.killed(), rf.isLeader())
}
