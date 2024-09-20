package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

func (ck *Clerk) Get(key string) string {
	args, reply := &GetArgs{key}, &GetReply{}
	ck.CallServer("Get", args, reply)
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args, reply := &PutAppendArgs{key, value}, &PutAppendReply{}
	ck.CallServer(op, args, reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("[Client]Put {%v,%v} Complete", key, value)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("[Client]Append {%v,%v} Complete", key, value)
}

func (ck *Clerk) CallServer(op string, args Args, reply Reply) {
	leaderId := atomic.LoadInt32(&ck.leaderId)
	no := leaderId
	for {
		DPrintf("[Client]Call Server RPC %d Args: %v", no, args)
		ok := ck.servers[no].Call("KVServer."+op, args, reply)
		if !ok {
			DPrintf("Send Get Request To %d Timeout", no)
			continue
		}

		if reply.getErr() == ErrWrongLeader {
			DPrintf("[Client]Call Server %d: Arg: %v failed:%v", no, args, reply.getErr())
			no = (no + 1) % int32(len(ck.servers))
			if no == leaderId {
				DPrintf("No Leader")
				// 试了一圈都没有Leader，说明当前没有Leader，等一会儿再试
				time.Sleep(time.Millisecond * 150)
			}
			continue
		}

		atomic.StoreInt32(&ck.leaderId, no)
		DPrintf("[Client]Update LeaderId to %d", no)
		return
	}
}
