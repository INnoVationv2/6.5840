package raft

import (
	"fmt"
	"time"
)

const (
	COMPLETE = iota
	TIMEOUT
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
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
	LogSz   int32
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
		args.Entries = rf.log[nextIdx:]
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

	jobChan := make(chan int)
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		idx := idx
		go func() {
			for !rf.killed() && rf.isLeader() && term == rf.getCurrentTerm() {
				status := rf.sendEntriesToFollower(idx, false)
				if status == TIMEOUT {
					continue
				} else {
					break
				}
			}
			jobChan <- 1
		}()
	}

	majority := rf.majority
	for majority > 0 && rf.isLeader() && !rf.killed() &&
		term == rf.getCurrentTerm() {
		majority--
		select {
		case _ = <-jobChan:
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v]Send Log Complete, Start Update CommitIndex", rf.getServerDetail())

	if majority != 0 || !rf.isLeader() || rf.killed() || term != rf.getCurrentTerm() {
		DPrintf("[%v]Sync Log Fail", rf.getServerDetail())
		return
	}

	//if rf.killed() || !rf.isLeader() || rf.getCurrentTerm() != term {
	//	DPrintf("[%v]Sync Log Fail", rf.getServerDetail())
	//	return
	//}
	//if failed == 0 {
	//	// 没有达成共识
	//	DPrintf("[%v]syncLogWithFollower Didn't Aggrement", rf.getServerDetail())
	//	return
	//}

	N := rf.findCommitIndex()
	DPrintf("[%v]N:%d", rf.getServerDetail(), N)
	if N > rf.getCommitIndex() && rf.log[N].Term == rf.getCurrentTerm() {
		rf.setCommitIndex(N)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), N)
	}

	commitIndex := rf.getCommitIndex()
	if commitIndex > rf.lastApplied {
		DPrintf("[%v]Apply %d~%d Tester\n", rf.getServerDetail(), rf.lastApplied+1, commitIndex)
		rf.sendCommitedLogToTester(rf.lastApplied+1, commitIndex)
		rf.lastApplied = commitIndex
	}
}

func (rf *Raft) sendEntriesToFollower(idx int, heartbeat bool) int {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for !rf.killed() {
		rf.mu.Lock()
		if rf.getRole() != LEADER {
			rf.mu.Unlock()
			return COMPLETE
		}
		rf.buildAppendEntriesArgs(args, idx, heartbeat)
		rf.resetHeartbeatTimer(idx)
		serverDetail := rf.getServerDetail()
		rf.mu.Unlock()

		DPrintf("[%v]Send AppendEntries RPC:%v to follower: %v\n", serverDetail, args, idx)
		ok := rf.peers[idx].Call("Raft.AcceptAppendEntries", args, reply)
		if !ok {
			DPrintf("[%v]Send AppendEntries RPC To %d Timeout", serverDetail, idx)
			return TIMEOUT
		}

		rf.mu.Lock()
		if rf.killed() || rf.getRole() != LEADER || rf.getCurrentTerm() != args.Term {
			DPrintf("[%v]killed:%v, Role:%v, Term:%d, CurrentTerm:%d", rf.getServerDetail(), rf.killed(), rf.getRoleStr(), args.Term, rf.getCurrentTerm())
			rf.mu.Unlock()
			return COMPLETE
		}

		if reply.Success {
			DPrintf("[%v]Success Send %d Log Entry To %d", serverDetail, reply.LogSz, idx)
			//if len(args.Entries) != 0 {
			if reply.LogSz != 0 {
				index := args.PrevLogIndex + reply.LogSz
				rf.setNextIndex(idx, index+1)
				rf.setMatchIndex(idx, index)
				DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d", serverDetail, idx, index+1, index)
			}
			rf.mu.Unlock()
			return COMPLETE
		}

		// 接下来都是reply.Success = false
		if reply.Term > rf.getCurrentTerm() {
			DPrintf("[%v]Follower Term > My Term, Back To Follower\n", serverDetail)
			rf.turnToFollower(reply.Term, int32(idx))
			rf.persist()
			rf.mu.Unlock()
			return COMPLETE
		}

		if reply.XTerm == -1 && reply.XIndex == -1 {
			// 日志太短
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
	return COMPLETE
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]AppendEntries From %d", rf.getServerDetail(), args.LeaderId)

	term := rf.getCurrentTerm()
	logSz := int32(len(rf.log))
	reply.Term = term
	reply.Success = false

	if args.Term < term {
		return
	}

	// 每条记录都用来更新Heartbeat
	rf.resetElectionTimer()

	if args.Term > term || rf.getRole() == CANDIDATE {
		DPrintf("[%s]AcceptAppendEntries Back To Follwer\n", rf.getServerDetail())
		rf.turnToFollower(args.Term, args.LeaderId)
		reply.Term = args.Term
		rf.persist()
	}

	// 本地没有与prevLogIndex、prevLogTerm匹配的项
	// 返回false
	if args.PrevLogIndex >= logSz || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = logSz

		conflictIdx := args.PrevLogIndex
		if conflictIdx < logSz {
			reply.XTerm = rf.log[conflictIdx].Term
			for conflictIdx > 0 &&
				rf.log[conflictIdx].Term == reply.XTerm {
				conflictIdx--
			}
			reply.XIndex = conflictIdx + 1
		}

		DPrintf("[%v]AppendEntries No Matched Log Entry:%v", rf.getServerDetail(), reply)
		return
	}
	reply.LogSz = int32(len(args.Entries))

	// 发来的日志和本地日志冲突(index相同,但term不同)
	// 删除冲突条目及后面所有的条目
	i, j := int(args.PrevLogIndex+1), 0
	for i < int(logSz) && j < len(args.Entries) {
		entry1 := rf.log[i]
		entry2 := args.Entries[j]
		if entry1.Term != entry2.Term {
			break
		}
		i, j = i+1, j+1
	}

	// rf.log[i]和args.Entries[j]发生冲突
	// 去除rf.log[i],保留args.Entries[j]
	rf.log = rf.log[:i]
	args.Entries = args.Entries[j:]

	// 将发来的日志项添加到日志中
	if len(args.Entries) != 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		DPrintf("[%v]Append %d Log, LogSz:%d", rf.getServerDetail(), len(args.Entries), len(rf.log))
	}
	logSz = int32(len(rf.log))

	// 更新CommitIndex
	if args.LeaderCommit > rf.getCommitIndex() {
		newCommitIndex := min(args.LeaderCommit, logSz-1)
		rf.setCommitIndex(newCommitIndex)
		DPrintf("[%v]AppendEntries Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	// 更新lastApplied
	commitIndex := rf.getCommitIndex()
	if commitIndex > rf.lastApplied {
		DPrintf("[%v]Apply %d~%d Log To State Machine", rf.getServerDetail(), rf.lastApplied+1, commitIndex)
		rf.sendCommitedLogToTester(rf.lastApplied+1, commitIndex)
		rf.lastApplied = commitIndex
	}

	reply.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) sendCommitedLogToTester(st, ed int32) {
	msg := ApplyMsg{CommandValid: true}
	for ; st <= ed; st++ {
		msg.CommandIndex = int(st)
		msg.Command = rf.log[st].Command
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
	DPrintf("[%v]Stop Sending Heartbeat", rf.getServerDetail())
}
