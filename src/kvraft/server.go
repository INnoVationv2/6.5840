package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type CmdType int

const (
	GET CmdType = iota
	PUT
	APPEND
)

func (c CmdType) String() string {
	switch c {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	ClientId int64
	CmdId    int32
	Type     CmdType
	Key      string
	Value    string
	Status   int32
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{ClientId:%d,CmdId:%d,Type:%v,Key:%v,Value:%v}", cmd.ClientId, cmd.CmdId, cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType CmdType, arg *Arg) *Command {
	return &Command{
		ClientId: arg.ClientId,
		CmdId:    arg.CommandId,
		Type:     opType,
		Status:   PENDING,
		Key:      arg.Key,
		Value:    arg.Value}
}

type KVServer struct {
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	shutdownCh chan struct{}
	dead       int32

	maxraftstate  int
	raftPersister *raft.Persister

	db DB

	mu sync.RWMutex
	// 记录历史结果,用于处理重复请求
	history map[int64]map[int32]string
	// 用于记录每个Client已执行的最大的Command Index
	matchIndex map[int64]int32
	submitCmd  map[int64]map[int32]Result

	// 用于记录已经执行的最大的LogEntry的Index
	appliedLogIdx int32
}

func (kv *KVServer) Get(arg *Arg, reply *Reply) {
	kv.submitCommand(GET, arg, reply)
}

func (kv *KVServer) Put(args *Arg, reply *Reply) {
	kv.submitCommand(PUT, args, reply)
}

func (kv *KVServer) Append(args *Arg, reply *Reply) {
	kv.submitCommand(APPEND, args, reply)
}

func (kv *KVServer) submitCommand(op CmdType, arg *Arg, reply *Reply) {
	DPrintf("[%v]Received %s RPC:%v", kv.getServerDetail(), op, arg)

	clientId, cmdId := arg.ClientId, arg.CommandId
	// 如果是重复请求，直接返回OK
	if val, ok := kv.getHistory(clientId, cmdId); ok {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), op, arg)
		reply.Status = OK
		reply.Value = val
		return
	}

	cmd := buildCommand(op, arg)
	kv.submitCmdToRaft(cmd)
	if reply.Status = cmd.Status; reply.Status != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Status)
	} else if op == GET {
		reply.Value = cmd.Value
	}
	DPrintf("[%v]%s Complete %v->%v", kv.getServerDetail(), op, arg, reply)
}

func (kv *KVServer) submitCmdToRaft(cmd *Command) {
	DPrintf("[%v]Submit Command %v To Raft", kv.getServerDetail(), cmd)
	defer DPrintf("[%v]Submit Command %v To Raft Complete", kv.getServerDetail(), cmd)

	// 用于Command执行完成时进行通知
	res := Result{Status: PENDING}
	kv.addSubmitCmd(cmd, res)

	cmdIdx, raftTerm, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		DPrintf("[%v]Not Leader", kv.getServerDetail())
		cmd.Status = ErrorNotLeader
		return
	}
	DPrintf("[%v]Success Submit Command %v To Raft, CmdIdx:%d", kv.getServerDetail(), cmd, cmdIdx)

	for !kv.killed() && kv.getRaftTerm() <= raftTerm && kv.getAppliedLogIdx() < int32(cmdIdx) {
	}

	cmd.Status = FAILED
	if res.Status == OK {
		cmd.Status = OK
		cmd.Value = res.Value
	}
}

// check apply chan, Update appliedLogIdx
func (kv *KVServer) ticker() {
	DPrintf("[%s]Start KVServer.Ticker", kv.getServerDetail())
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%s]Stop KVServer.Ticker", kv.getServerDetail())
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				cmdIdx, cmd := int32(msg.CommandIndex), msg.Command.(Command)
				DPrintf("[%s]Receive Command %v From Raft applyChan", kv.getServerDetail(), &cmd)
				kv.applyCommand(&cmd)
				kv.setAppliedLogIdx(cmdIdx)
				kv.checkSnapshot()
			} else {
				DPrintf("[%s]Receive Snapshot From Raft applyChan, LastIncludeIdx:%d", kv.getServerDetail(), msg.SnapshotIndex)
				kv.readSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		}
	}
}

func (kv *KVServer) applyCommand(cmd *Command) {
	DPrintf("[%v]Apply %v ", kv.getServerDetail(), cmd)
	defer DPrintf("[%v]Apply %v Complete", kv.getServerDetail(), cmd)

	if cmd.Type != GET && kv.getMatchIndex(cmd.ClientId) < cmd.CmdId {
		switch cmd.Type {
		case PUT:
			kv.db.set(cmd.Key, cmd.Value)
			DPrintf("[%v][ApplyCommand]Put %v->%v", kv.getServerDetail(), cmd, cmd.Value)
		case APPEND:
			value := kv.db.append(cmd.Key, cmd.Value)
			DPrintf("[%v][ApplyCommand]Append %v->%v", kv.getServerDetail(), cmd, value)
		}
	}

	value := ""
	if cmd.Type == GET {
		value = kv.db.get(cmd.Key)
		DPrintf("[%v][ApplyCommand]Get %v->%v", kv.getServerDetail(), cmd, value)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.setHistory(cmd.ClientId, cmd.CmdId, value)
	if cmd.CmdId > kv.matchIndex[cmd.ClientId] {
		kv.matchIndex[cmd.ClientId] = cmd.CmdId
	}
	if res, ok := kv.submitCmd[cmd.ClientId][cmd.CmdId]; ok {
		res.Status = OK
		res.Value = value
		delete(kv.submitCmd[cmd.ClientId], cmd.CmdId)
	}
}

func (kv *KVServer) readSnapshot(lastIncludeIndex int, snapshot []byte) {
	DPrintf("[%v]lastIncludeIndex:%d AppliedLogIdx:%d", kv.getServerDetail(), lastIncludeIndex, kv.appliedLogIdx)
	if int32(lastIncludeIndex) <= kv.getAppliedLogIdx() {
		return
	}

	kv.setAppliedLogIdx(int32(lastIncludeIndex))
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	db := make(map[string]string)
	if d.Decode(&db) != nil ||
		d.Decode(&kv.submitCmd) != nil ||
		d.Decode(&kv.history) != nil ||
		d.Decode(&kv.matchIndex) != nil {
		log.Fatalf("[%v]Decode Raft State Failed", kv.getServerDetail())
	}
	kv.db.setDB(db)
}

func (kv *KVServer) checkSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}
	DPrintf("[%v]Maxraftstate:%d RaftStateSize:%d", kv.getServerDetail(), kv.maxraftstate, kv.raftPersister.RaftStateSize())
	if kv.maxraftstate-kv.raftPersister.RaftStateSize() > 50 {
		return
	}
	DPrintf("[%v]Build Snapshot", kv.getServerDetail())
	// 大小接近，进行snapshot
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)

	kv.mu.RLock()
	if e.Encode(kv.db.export()) != nil ||
		e.Encode(kv.submitCmd) != nil ||
		e.Encode(kv.history) != nil ||
		e.Encode(kv.matchIndex) != nil {
		log.Fatalf("[%v]Encode KVServer State Failed", kv.getServerDetail())
	}
	kv.mu.RUnlock()

	kv.rf.Snapshot(int(kv.appliedLogIdx), buf.Bytes())
}

func (kv *KVServer) Kill() {
	close(kv.shutdownCh)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *KVServer) Report(args *Arg, reply *Reply) {
	DPrintf("[%v]Receive Report RPC %v, Delete History", kv.getServerDetail(), args)
	kv.deleteHistory(args.ClientId, args.CommandId)
	reply.Status = OK
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.raftPersister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shutdownCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = buildInMemoryDB()
	kv.history = make(map[int64]map[int32]string)
	kv.matchIndex = make(map[int64]int32)
	kv.submitCmd = make(map[int64]map[int32]Result)
	kv.appliedLogIdx = 0

	go kv.ticker()

	return kv
}
