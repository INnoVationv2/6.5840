package kvraft

import (
	"6.5840/labrpc"
	"sync/atomic"
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
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Get(key string) string {
	args, reply := GetArgs{key}, GetReply{}
	no := ck.leaderId
	for {
		DPrintf("[Client]Call Server RPC %d, {Get %v}", no, key)
		ok := ck.servers[no].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("[Client]Call Server %d: {Get %v} Timeout", no, key)
			continue
		}

		if reply.Err == ErrWrongLeader {
			DPrintf("[Client]Call Server %d: {Get %v} failed:%v", no, key, reply.Err)
			no = (no + 1) % int32(len(ck.servers))
			continue
		}

		atomic.StoreInt32(&ck.leaderId, no)
		DPrintf("[Client]Update LeaderId to %d", no)
		return reply.Value
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args, reply := PutAppendArgs{key, value}, PutAppendReply{}
	no := ck.leaderId
	for {
		DPrintf("[Client]Call Server RPC %d {%v %v,%v}", no, op, key, value)
		ok := ck.servers[no].Call("KVServer."+op, &args, &reply)
		if !ok {
			DPrintf("Send Get Request To %d Timeout", no)
			continue
		}

		if reply.Err == ErrWrongLeader {
			DPrintf("[Client]Call Server %d, {%v %v,%v} failed:%v", no, op, key, value, reply.Err)
			no = (no + 1) % int32(len(ck.servers))
			continue
		}

		atomic.StoreInt32(&ck.leaderId, no)
		DPrintf("[Client]Update LeaderId to %d", no)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
	DPrintf("[Client]Put {%v,%v} Complete", key, value)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
	DPrintf("[Client]Append {%v,%v} Complete", key, value)
}
