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
	reply := &GetReply{}
	ck.CallServer("Get", args, reply)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("[Client]Put {%v,%v} Complete", key, value)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("[Client]Append {%v,%v} Complete", key, value)
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := ck.buildPutAppendArg(key, value)
	reply := &PutAppendReply{}
	ck.CallServer(op, args, reply)
}

func (ck *Clerk) CallServer(op string, args Args, reply Reply) {
	leaderId := atomic.LoadInt32(&ck.leaderId)
	serverNo := leaderId
	for {
		DPrintf("[Client]Send Command %v To KvServer %d", args, serverNo)
		ok := ck.servers[serverNo].Call("KVServer."+op, args, reply)
		if !ok || reply.getErr() == ErrWrongLeader {
			DPrintf("[Client]Send Command %v To KvServer %d failed:%v, Retring...", args, serverNo, reply.getErr())
			serverNo = (serverNo + 1) % int32(len(ck.servers))
			if serverNo == leaderId {
				DPrintf("[Client]No Leader, Wait Some Time Then Retry...")
				// 试了一圈都没有Leader，说明当前没有Leader，等一会儿再试
				time.Sleep(time.Millisecond * 150)
			}
			continue
		}
		break
	}
	go ck.Report(serverNo, args)
	atomic.StoreInt32(&ck.leaderId, serverNo)
	DPrintf("[Client]Update LeaderId to %d", serverNo)
}

func (ck *Clerk) Report(serverNo int32, arg Args) {
	args, reply := GetArgs{ClientId: ck.id, CommandId: arg.GetCommandId()}, GetReply{}
	DPrintf("[Client]Command %v Is Complete, Report To Server %d", args, serverNo)
	ck.servers[serverNo].Call("KVServer.Report", &args, &reply)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.servers = servers
	return ck
}
