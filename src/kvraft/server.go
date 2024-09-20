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

	LEADER = iota
	FOLLOWER
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
	role    int32

	maxraftstate int

	raftTerm      int32
	appliedLogIdx int32
	db            map[string]string

	//submitLog  map[int32]*Command
	history    map[int64]map[int32]string
	matchIndex map[int64]int32
}

func (kv *KVServer) checkIfCommandAlreadyExecute(clientId int64, commandId int32) bool {
	if kv.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[Server %v]Received Command %v", kv.me, args)
	clientId, cmdId := args.ClientId, args.CommandId

	// 如果是重复命令，直接返回之前的结果
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
	kv.matchIndex[args.ClientId] = max(cmdId, kv.matchIndex[args.ClientId])

	if kv.history[clientId] == nil {
		kv.history[clientId] = make(map[int32]string)
	}
	kv.history[clientId][cmdId] = result
	DPrintf("[Server %d]Add Resut To History", kv.me)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db[args.Key] = args.Value

	kv.matchIndex[args.ClientId] = max(args.CommandId, kv.matchIndex[args.ClientId])
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.PutAppend(args, reply)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.db[args.Key]
	if !ok {
		val = ""
	}
	kv.db[args.Key] = val + args.Value

	kv.matchIndex[args.ClientId] = max(args.CommandId, kv.matchIndex[args.ClientId])
}

func (kv *KVServer) submitCommand(cmd *Command) Err {
	logIdx, _, isLeader := kv.rf.Start(cmd)
	DPrintf("[Server %v]Send Command %v To Raft, Index:%d", kv.me, cmd, logIdx)
	if !isLeader {
		kv.turnToFollower()
		return ErrWrongLeader
	}

	kv.turnToLeader()
	for kv.getAppliedLogIdx() < int32(logIdx) && !kv.killed() && kv.isLeader() {
	}

	if !kv.isLeader() {
		return ErrWrongLeader
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
		if kv.isLeader() {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.turnToFollower()
			}
		}
		select {
		case msg := <-kv.applyCh:
			if !kv.isLeader() {
				continue
			}
			kv.mu.Lock()
			DPrintf("[Server %d]Receive %d From Raft", kv.me, msg.CommandIndex)
			if int32(msg.CommandIndex) > kv.getAppliedLogIdx() {
				DPrintf("[Server %d]Update AppliedLogIdx To %d", kv.me, msg.CommandIndex)
				atomic.StoreInt32(&kv.appliedLogIdx, int32(msg.CommandIndex))
			}
			kv.mu.Unlock()
		default:
			continue
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
	kv.role = FOLLOWER
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[int32]string)
	kv.matchIndex = make(map[int64]int32)

	go kv.ticker()

	return kv
}
