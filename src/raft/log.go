package raft

type LogEntry struct {
	Term    int32
	Command interface{}
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

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, serverIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.getCurrentTerm()
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	// 判断有没有需要发送的日志
	nextLogIdx := rf.getNextIndex(serverIdx)
	if rf.getLogSz() > nextLogIdx {
		prevLogIdx := nextLogIdx - 1
		args.PrevLogIndex = prevLogIdx
		args.PrevLogTerm = rf.log[prevLogIdx].Term
		args.Entries = rf.log[nextLogIdx:]
	}
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	if !rf.isLeader() || rf.killed() {
		isLeader = false
		return
	}
	isLeader = true

	rf.mu.Lock()
	index = len(rf.log)
	currentTerm := rf.getCurrentTerm()
	term = int(currentTerm)
	logEntry := rf.buildLogEntry(command, currentTerm)
	rf.log = append(rf.log, *logEntry)
	rf.mu.Unlock()

	go rf.syncLogWithFollower()

	return
}

// 发送日志到所有Server，达成一致后对日志提交
func (rf *Raft) syncLogWithFollower() {
	lastLogIdx := int32(len(rf.log) - 1)
	jobChan := make(chan int)

	for idx := 0; idx < len(rf.peers) && rf.isLeader(); idx++ {
		if int32(idx) == rf.me {
			continue
		}

		if lastLogIdx >= rf.getNextIndex(idx) {
			rf.setLastSendTime(idx)
			go func() {
				ok := rf.sendEntriesToFollower(idx)
				if !ok {
					jobChan <- -1
					return
				}
				jobChan <- idx
			}()
		}
	}

	majorityNum := len(rf.peers)/2 + 1
	N := int32(len(rf.log))
	for idx := 0; idx < majorityNum && rf.isLeader(); idx++ {
		select {
		case jobChan <- idx:
			if idx == -1 {
				return
			}
			N = min(N, rf.getMatchIndex(idx))
		}
	}

	rf.commitIndex = max(rf.commitIndex, N)
	// 执行lastApply~commitIndex之间的日志
}

func (rf *Raft) sendEntriesToFollower(idx int) bool {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for {
		if rf.getRole() != LEADER {
			return false
		}

		rf.buildAppendEntriesArgs(args, idx)
		ok := rf.peers[idx].Call("Raft.AcceptAppendEntries", args, reply)
		if !ok {
			DPrintf("[%v]Send AppendEntries RPC To %d Failed", rf.getServerDetail(), idx)
			return false
		}

		if reply.Success == true {
			rf.setMatchIndex(idx, args.PrevLogIndex+int32(len(args.Entries)))
			DPrintf("[%v]Success Send AppendEntries RPC To Follower %d\n", rf.getServerDetail(), idx)
			return true
		}

		if reply.Term > rf.getCurrentTerm() {
			rf.turnToFollower(reply.Term)
			return false
		}

		rf.addNextIndex(idx, -1)
	}
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.getCurrentTerm()
	reply.Term = term

	// 若term < CurrentTerm，返回false
	if args.Term < term {
		reply.Success = false
		return
	}

	if args.Term > term {
		rf.mu.Lock()
		rf.turnToFollower(args.Term)
		rf.mu.Unlock()
		reply.Term = args.Term
	}

	// 每条记录都用来更新Heartbeat
	rf.handleHeartbeat(args)
	if len(args.Entries) == 0 {
		// 如果没有Entries，证明只是Heartbeat
		reply.Success = true
		return
	}

	// 若日志中没有与`prevLogIndex`、`prevLogTerm`匹配的项，返回false
	entry := rf.log[args.PrevLogIndex]
	if entry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// 更新CommitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, int32(len(rf.log)-1))
		// apply log entry to state machine
	}

	if len(args.Entries) == 0 {
		// This RPC is Heartbeat
		reply.Success = true
		return
	}

	// 若Follower的日志和发来的日志冲突(比如日志条目的index相同，
	// 但term不同)，则删除现有的条目及后面所有的条目。
	i, j := int(args.PrevLogIndex+1), 0
	for i < len(rf.log) && j < len(args.Entries) {
		entry1, entry2 := rf.log[i], args.Entries[j]
		if entry1.Term != entry2.Term {
			break
		}
	}
	rf.log = append(rf.log[:i], args.Entries[j:]...)
	reply.Success = true
}
