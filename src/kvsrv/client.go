package kvsrv

import (
	"6.5840/labrpc"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd

	clientID int64
	rpcCnt   int32
}

func (ck *Clerk) getRpcId() int32 {
	id := atomic.AddInt32(&ck.rpcCnt, 1)
	return id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientID = nrand()
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{ClientID: ck.clientID, Key: key}
	reply := GetReply{}

	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}

	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{ClientID: ck.clientID, ID: ck.getRpcId(), Key: key, Value: value}
	reply := PutAppendReply{}

	ok := false
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}

	ck.Report(key)

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Report(key string) {
	args := ReportArgs{ClientID: ck.clientID, Key: key}
	reply := ReportReply{}

	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Report", &args, &reply)
	}
}
