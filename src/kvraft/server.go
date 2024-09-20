package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	Type  string
	Key   string
	Value string
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{%v %v,%v}", cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType string, str ...string) *Command {
	cmd := Command{Type: opType, Key: str[0]}
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
	dead    int32

	maxraftstate int

	appliedLogIdx int32
	db            map[string]string

	history     map[int64]map[int32]string
	clientState map[int64]int32
}

func (kv *KVServer) checkIfCommandAlreadyExecute(clientId int64, commandId int32) bool {
	if kv.clientState[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[Server %v]Received Command %v", kv.me, args)
	clientId, cmdId := args.ClientId, args.CommandId
	kv.mu.Lock()
	if kv.checkIfCommandAlreadyExecute(clientId, cmdId) {
		reply.Err = OK
		reply.Value = kv.history[args.ClientId][args.CommandId]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := buildCommand(GET, args.Key)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command %v failed:%v", kv.me, args, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, ok := kv.db[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		result = ""
	}
	reply.Value = result

	kv.clientState[args.ClientId] = max(cmdId, kv.clientState[args.ClientId])

	if kv.history[clientId] == nil {
		kv.history[clientId] = make(map[int32]string)
	}
	kv.history[clientId][cmdId] = result
	DPrintf("[Server %d]Add Resut To History", kv.me)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[Server %v]Received Command %v", kv.me, args)
	clientId, cmdId := args.ClientId, args.CommandId

	kv.mu.Lock()
	if kv.checkIfCommandAlreadyExecute(clientId, cmdId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := buildCommand(PUT, args.Key, args.Value)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command %v failed:%v", kv.me, command, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[args.Key] = args.Value

	kv.clientState[clientId] = max(cmdId, kv.clientState[clientId])
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[Server %v]Received Command %v", kv.me, args)
	clientId, cmdId := args.ClientId, args.CommandId

	kv.mu.Lock()
	if kv.checkIfCommandAlreadyExecute(clientId, cmdId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := buildCommand(APPEND, args.Key, args.Value)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command %v failed:%v", kv.me, command, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.db[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		val = ""
	}
	kv.db[args.Key] = val + args.Value

	kv.clientState[args.ClientId] = max(cmdId, kv.clientState[args.ClientId])
}

func (kv *KVServer) submitCommand(cmd *Command) Err {
	logIdx, _, leader := kv.rf.Start(cmd)
	DPrintf("[Server %v]Send Command %v To Raft, Index:%d", kv.me, cmd, logIdx)
	if !leader {
		return ErrWrongLeader
	}

	for kv.getAppliedLogIdx() < int32(logIdx) && !kv.killed() {
		time.Sleep(time.Millisecond * 10)
	}

	if kv.killed() {
		return Killed
	}

	return OK
}

// check apply chan, Update appliedLogIdx
func (kv *KVServer) ticker() {
	DPrintf("[Server %v]Ticker Start", kv.me)

	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if int32(msg.CommandIndex) > kv.getAppliedLogIdx() {
				DPrintf("[Server %d]Update AppliedLogIdx To %d", kv.me, msg.CommandIndex)
				atomic.StoreInt32(&kv.appliedLogIdx, int32(msg.CommandIndex))
			}
			kv.mu.Unlock()
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}

	DPrintf("[Server %v]Ticker End", kv.me)
}

func (kv *KVServer) Kill() {
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
	DPrintf("[Server %d]Receive Report From %d, Delete Cmd %d From History", kv.me, clientID, cmdId)
	delete(kv.history[clientID], cmdId)
	//if len(kv.history[clientID]) == 0 {
	//	DPrintf("[Server %d]Erase History", kv.me)
	//	kv.history[clientID] = make(map[int32]string)
	//}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[int32]string)
	kv.clientState = make(map[int64]int32)

	go kv.ticker()

	return kv
}
