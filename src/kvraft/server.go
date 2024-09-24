package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Command struct {
	ClientId int64
	Type     string
	Key      string
	Value    string
	Status   Err
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{%v %v:%v}", cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType string, clientId int64, str ...string) *Command {
	cmd := Command{Type: opType, Key: str[0], Status: OK, ClientId: clientId}
	if opType == PUT || opType == APPEND {
		cmd.Value = str[1]
	}
	return &cmd
}

type CommandWarp struct {
	Cmd      *Command
	ClientId int64
	CmdId    int32
}

func (w *CommandWarp) String() string {
	return fmt.Sprintf("{%v %v:%v}", w.Cmd, w.ClientId, w.CmdId)
}

func buildCommandWarp(cmd *Command, clientId int64, cmdId int32) *CommandWarp {
	return &CommandWarp{
		Cmd:      cmd,
		ClientId: clientId,
		CmdId:    cmdId,
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	killCh  chan bool
	dead    int32

	maxraftstate int

	raftTerm int32
	// 用于记录已经执行的最大的LogEntry的Index
	appliedLogIdx int32
	prevCmd       map[int64]*Command
	submitCmd     map[int]*CommandWarp

	db map[string]string

	//duplicateCheckMu sync.Mutex
	history map[int64]map[int32]string
	// 用于记录Server已经执行的最大的Command Index
	matchIndex map[int64]int32
}

func (kv *KVServer) checkIfCommandAlreadyExecuted(clientId int64, commandId int32) bool {
	if kv.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%v]Received Get RPC:%v", kv.getServerDetail(), args)
	clientId, cmdId := args.ClientId, args.CommandId

	// 如果是重复命令，直接返回之前的结果
	kv.mu.Lock()
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte Get Reuqest %v", kv.getServerDetail(), args)
		reply.Err = OK
		reply.Value = kv.history[clientId][cmdId]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := buildCommand(GET, args.ClientId, args.Key)
	cmdWarp := buildCommandWarp(cmd, clientId, cmdId)
	kv.submitCommand(cmdWarp)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%s]Submit Command %v Failed:%v", kv.getServerDetail(), args, reply.Err)
		return
	}
	reply.Value = cmd.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v]Received PUT RPC:%v", kv.getServerDetail(), args)
	kv.PutAppend(args, reply, PUT)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v]Received Append RPC:%v", kv.getServerDetail(), args)
	kv.PutAppend(args, reply, APPEND)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, op string) {
	clientId, cmdId := args.ClientId, args.CommandId

	// Check If Already Execute
	kv.mu.Lock()
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), op, args)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := buildCommand(op, args.ClientId, args.Key, args.Value)
	cmdWarp := buildCommandWarp(cmd, clientId, cmdId)
	kv.submitCommand(cmdWarp)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Err)
		return
	}
}

func (kv *KVServer) submitCommand(cmdWarp *CommandWarp) {
	DPrintf("[%v]SubmitCommand", kv.getServerDetail())
	kv.mu.Lock()
	cmd := cmdWarp.Cmd
	cmdIdx, term, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.setRaftTerm(int32(term))
	kv.submitCmd[cmdIdx] = cmdWarp
	kv.mu.Unlock()

	DPrintf("[%s]Submit Command %v To Raft, Index:%d, Term:%d", kv.getServerDetail(), cmdWarp, cmdIdx, term)
	for !kv.killed() && kv.getAppliedLogIdx() < int32(cmdIdx) && kv.getRaftTerm() == int32(term) {
	}

	if cmd.Status == LogNotMatch {
		DPrintf("[%s]Command %v Not Match, Need Re-Submit To KVServer", kv.getServerDetail(), cmd)
	}

	if kv.getRaftTerm() != int32(term) {
		cmd.Status = TermChanged
		DPrintf("[%s]Command %v Is Expired, SubmitTerm:%d, CurrentTerm:%d, Need Re-Submit To KVServer",
			kv.getServerDetail(), cmd, term, kv.getRaftTerm())
	}

	if kv.killed() {
		cmd.Status = Killed
	}
}

// check apply chan, Update appliedLogIdx
func (kv *KVServer) ticker() {
	DPrintf("[%s]Ticker Start", kv.getServerDetail())
	for {
		select {
		case msg := <-kv.applyCh:
			cmdIdx, cmd := msg.CommandIndex, msg.Command.(Command)
			DPrintf("[%s]Receive Command %d:%v From Raft applyChan", kv.getServerDetail(), cmdIdx, cmd)

			kv.applyCommand(cmdIdx, &cmd)
			kv.setAppliedLogIdx(int32(cmdIdx))
			kv.prevCmd[cmd.ClientId] = &cmd
		case <-kv.killCh:
			DPrintf("[%s]Sever Been Killed, Ticker End", kv.getServerDetail())
			return
		}
	}
}

func (kv *KVServer) applyCommand(cmdIdx int, cmd *Command) {
	prevCmd, ok := kv.prevCmd[cmd.ClientId]
	if !ok || !compareCommand(cmd, prevCmd) {
		switch cmd.Type {
		case PUT:
			kv.db[cmd.Key] = cmd.Value
		case APPEND:
			val, ok := kv.db[cmd.Key]
			if !ok {
				val = ""
			}
			kv.db[cmd.Key] = val + cmd.Value
		}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	cmdWarp, ok := kv.submitCmd[cmdIdx]
	if !ok {
		return
	}

	cmd2 := cmdWarp.Cmd
	if !compareCommand(cmd, cmd2) {
		DPrintf("[%v]Log At %d Not Match, Return", kv.getServerDetail(), cmdIdx)
		cmd2.Status = LogNotMatch
		return
	}
	kv.updateState(cmdWarp)
	delete(kv.submitCmd, cmdIdx)
}

func (kv *KVServer) updateState(cmdWarp *CommandWarp) {
	cmd, clientId, cmdId := cmdWarp.Cmd, cmdWarp.ClientId, cmdWarp.CmdId

	cmd.Status = OK
	kv.matchIndex[clientId] = max(kv.matchIndex[clientId], cmdId)
	DPrintf("[%v]Update Match Index To %d", kv.getServerDetail(), kv.matchIndex[clientId])

	if cmd.Type == GET {
		val, ok := kv.db[cmd.Key]
		if !ok {
			val = ""
		}
		cmd.Value = val
		if kv.history[clientId] == nil {
			kv.history[clientId] = make(map[int32]string)
		}
		kv.history[clientId][cmdId] = val
	}
}

func (kv *KVServer) monitorTerm() {
	for {
		term, _ := kv.rf.GetState()
		if int32(term) != kv.getRaftTerm() {
			DPrintf("[%s]Raft Term Change:%d-->%d", kv.getServerDetail(), kv.getRaftTerm(), term)
			kv.setRaftTerm(int32(term))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *KVServer) Kill() {
	kv.killCh <- true
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Report(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	clientID, cmdId := args.ClientId, args.CommandId
	DPrintf("[%v]Receive Report RPC %v, Delete History", kv.getServerDetail(), args)
	delete(kv.history[clientID], cmdId)
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[int32]string)
	kv.matchIndex = make(map[int64]int32)
	kv.submitCmd = make(map[int]*CommandWarp)
	kv.prevCmd = make(map[int64]*Command)

	go kv.ticker()
	go kv.monitorTerm()

	return kv
}
