package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	id         int64
	commandCnt int32
	servers    []*labrpc.ClientEnd
	leaderId   int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getCommandId() int32 {
	return atomic.AddInt32(&ck.commandCnt, 1)
}

func (ck *Clerk) Get(key string) string {
	args := ck.buildGetArg(key)
	reply := ck.CallServer("Get", args)
	DPrintf("[Client]Command Get Complete {%d %v}", args.CommandId, key)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	args := ck.buildPutAppendArg(key, value)
	ck.CallServer("Put", args)
	DPrintf("[Client]Command Put Complete {%d %v->%v}", args.CommandId, key, value)
}

func (ck *Clerk) Append(key string, value string) {
	args := ck.buildPutAppendArg(key, value)
	ck.CallServer("Append", args)
	DPrintf("[Client]Command Append Complete {%d %v->%v}", args.CommandId, key, value)
}

func (ck *Clerk) CallServer(op string, arg *Arg) (reply *Reply) {
	leaderId := atomic.LoadInt32(&ck.leaderId)
	serverNo := leaderId
	reply = &Reply{Status: FAILED}
	for {
		DPrintf("[Client]Send %s RPC %v To KvServer %d", op, arg, serverNo)
		ok := ck.servers[serverNo].Call("KVServer."+op, arg, reply)
		if ok && reply.Status == OK {
			go ck.Report(serverNo, arg)
			atomic.StoreInt32(&ck.leaderId, serverNo)
			DPrintf("[Client]Update LeaderId to %d", serverNo)
			return
		}
		DPrintf("[Client]Send %s RPC %v To KvServer %d Failed:%v, Retring...", op, arg, serverNo, reply.Status)
		if !ok || reply.Status == ErrorNotLeader {
			serverNo = (serverNo + 1) % int32(len(ck.servers))
			if serverNo == leaderId {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (ck *Clerk) Report(serverNo int32, arg *Arg) {
	cmdId := arg.CommandId
	args, reply := Arg{ClientId: ck.id, CommandId: cmdId}, Reply{}
	DPrintf("[Client]Command %d Is Complete, Send Report RPC To Server %d", cmdId, serverNo)
	for ok := false; !ok; {
		ok = ck.servers[serverNo].Call("KVServer.Report", &args, &reply)
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.servers = servers
	return ck
}
