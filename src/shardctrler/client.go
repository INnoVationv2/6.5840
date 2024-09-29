package shardctrler

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	id         int64
	commandCnt int32
	servers    []*labrpc.ClientEnd
	leaderId   int32
}

func (ck *Clerk) getCmdId() int32 {
	return atomic.AddInt32(&ck.commandCnt, 1)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.servers = servers
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Num:       num,
	}
	reply := &QueryReply{}
	ck.CallServer("Query", args, reply)
	DPrintf("[Client]Query RPC Complete %v:%v", args, reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Servers:   make(map[int][]string),
	}
	for key, val := range servers {
		args.Servers[key] = make([]string, len(val))
		copy(args.Servers[key], val)
	}
	reply := &JoinReply{}
	ck.CallServer("Join", args, reply)
	DPrintf("[Client]Join RPC Complete %v", args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		GIDs:      gids,
	}
	reply := &LeaveReply{}
	ck.CallServer("Leave", args, reply)
	DPrintf("[Client]Leave RPC Complete %v", args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Shard:     shard,
		GID:       gid,
	}
	reply := &MoveReply{}
	ck.CallServer("Move", args, reply)
	DPrintf("[Client]Move RPC Complete %v", args)
}

func (ck *Clerk) CallServer(op string, args Args, reply Reply) {
	leaderId := ck.leaderId
	serverNo := leaderId
	for {
		DPrintf("[Client]Send %s RPC %v To ShardCtrler %d", op, args, serverNo)
		ok := ck.servers[serverNo].Call("ShardCtrler."+op, args, reply)
		if ok && reply.getErr() == OK {
			go ck.Report(serverNo, args)
			atomic.StoreInt32(&ck.leaderId, serverNo)
			return
		}
		serverNo = (serverNo + 1) % int32(len(ck.servers))
		// 试了一圈，没有Leader，休息一段时间再试
		if serverNo == leaderId {
			time.Sleep(300 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Report(serverNo int32, arg Args) {
	cmdId := arg.GetCommandId()
	args, reply := QueryArgs{ClientId: ck.id, CommandId: cmdId}, QueryReply{}
	DPrintf("[Client]Command %d Is Complete, Send Report RPC To ShardCtrler %d", cmdId, serverNo)
	for ok := false; !ok; {
		ok = ck.servers[serverNo].Call("ShardCtrler.Report", &args, &reply)
	}
}
