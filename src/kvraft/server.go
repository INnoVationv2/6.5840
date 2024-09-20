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

func DPrintf(format string, a ...interface{}) (n int, err error) {
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
	val           map[string]string

	tickerStatus int32
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[Server %v]Received Command {Get %v}", kv.me, args.Key)
	command := buildCommand(GET, args.Key)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command {Get, %v} failed:%v", kv.me, args.Key, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, ok := kv.val[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		result = ""
	}
	reply.Value = result
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	command := buildCommand(PUT, args.Key, args.Value)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command %v failed:%v", kv.me, command, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.val[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	command := buildCommand(APPEND, args.Key, args.Value)
	reply.Err = kv.submitCommand(command)
	if reply.Err != OK {
		DPrintf("[Server %v]Command %v failed:%v", kv.me, command, reply.Err)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.val[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		val = ""
	}
	kv.val[args.Key] = val + args.Value
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

// check apply chan
func (kv *KVServer) ticker() {
	DPrintf("[Server %v]Ticker Start", kv.me)

	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if int32(msg.CommandIndex) > kv.getAppliedLogIdx() {
				DPrintf("Update AppliedLogIdx To %d", msg.CommandIndex)
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

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.val = make(map[string]string)
	go kv.ticker()

	return kv
}
