package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	GET    = "GET"
	PUT    = "PUT"
	APPEND = "APPEND"
)

type Command struct {
	Type   string
	Key    string
	Value  string
	Status Err
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{%v %v:%v}", cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType string, str ...string) *Command {
	cmd := Command{Type: opType, Key: str[0], Status: Pending}
	if opType == PUT || opType == APPEND {
		cmd.Value = str[1]
	}
	return &cmd
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	killCh  chan bool
	dead    int32

	maxraftstate int

	raftTerm      int32
	appliedLogIdx int32
	submitCmd     map[int]*Command

	db map[string]string

	duplicateCheckMu sync.Mutex
	history          map[int64]map[int32]string
	matchIndex       map[int64]int32
}

func (kv *KVServer) checkIfCommandAlreadyExecuted(clientId int64, commandId int32) bool {
	if kv.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	clientId, cmdId := args.ClientId, args.CommandId

	// 如果是重复命令，直接返回之前的结果
	kv.duplicateCheckMu.Lock()
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte Get Reuqest %v", kv.getServerDetail(), args)
		reply.Err = OK
		reply.Value = kv.history[clientId][cmdId]
		kv.duplicateCheckMu.Unlock()
		return
	}
	kv.duplicateCheckMu.Unlock()

	cmd := buildCommand(GET, args.Key)
	kv.submitCommand(cmd)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%s]Submit Command %v Failed:%v", kv.getServerDetail(), args, reply.Err)
		return
	}

	reply.Value = cmd.Value

	kv.duplicateCheckMu.Lock()
	defer kv.duplicateCheckMu.Unlock()
	if kv.history[clientId] == nil {
		kv.history[clientId] = make(map[int32]string)
	}
	kv.history[clientId][cmdId] = cmd.Value
	kv.matchIndex[clientId] = max(cmdId, kv.matchIndex[clientId])
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply, PUT)
	if reply.Err != OK {
		return
	}

	kv.duplicateCheckMu.Lock()
	defer kv.duplicateCheckMu.Unlock()
	kv.matchIndex[args.ClientId] = max(args.CommandId, kv.matchIndex[args.ClientId])
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply, APPEND)
	if reply.Err != OK {
		return
	}

	kv.duplicateCheckMu.Lock()
	defer kv.duplicateCheckMu.Unlock()
	kv.matchIndex[args.ClientId] = max(args.CommandId, kv.matchIndex[args.ClientId])
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply, op string) {
	clientId, cmdId := args.ClientId, args.CommandId

	// Check If Already Execute
	kv.duplicateCheckMu.Lock()
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), op, args)
		reply.Err = OK
		kv.duplicateCheckMu.Unlock()
		return
	}
	kv.duplicateCheckMu.Unlock()

	cmd := buildCommand(op, args.Key, args.Value)
	kv.submitCommand(cmd)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Err)
		return
	}
}

func (kv *KVServer) submitCommand(cmd *Command) {
	kv.mu.Lock()
	cmdIdx, _, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.submitCmd[cmdIdx] = cmd
	kv.mu.Unlock()

	DPrintf("[%s]Submit Command %v To Raft, Index:%d", kv.getServerDetail(), cmd, cmdIdx)
	for !kv.killed() && kv.getAppliedLogIdx() < int32(cmdIdx) {
	}

	cmd.Status = OK

	if kv.killed() {
		return
	}

	kv.mu.Lock()
	delete(kv.submitCmd, cmdIdx)
	kv.mu.Unlock()
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
			atomic.StoreInt32(&kv.appliedLogIdx, int32(cmdIdx))
		case <-kv.killCh:
			DPrintf("[%s]Ticker End", kv.getServerDetail())
			return
		}
	}
}

func (kv *KVServer) applyCommand(cmdIdx int, cmd *Command) {
	switch cmd.Type {
	case PUT:
		kv.db[cmd.Key] = cmd.Value
	case APPEND:
		val, ok := kv.db[cmd.Key]
		if !ok {
			val = ""
		}
		kv.db[cmd.Key] = val + cmd.Value
	case GET:
	}

	kv.mu.Lock()
	cmd2, ok := kv.submitCmd[cmdIdx]
	kv.mu.Unlock()
	if !ok {
		return
	}
	cmd2.Status = LogNotMatch
	if compareCommand(cmd, cmd2) {
		cmd2.Status = OK
		if cmd.Type == GET {
			val, ok := kv.db[cmd.Key]
			if !ok {
				cmd2.Value = ""
			} else {
				cmd2.Value = val
			}
		}
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

	kv.duplicateCheckMu.Lock()
	delete(kv.history[clientID], cmdId)
	kv.duplicateCheckMu.Unlock()
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
	kv.submitCmd = make(map[int]*Command)

	go kv.ticker()

	return kv
}
