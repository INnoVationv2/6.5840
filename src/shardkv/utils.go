package shardkv

import (
	"6.5840/labrpc"
	"6.5840/logger"
	"6.5840/shardctrler"
	"crypto/rand"
	"math/big"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Debug(format, a...)
	}
	return
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func Report(clientEnd *labrpc.ClientEnd, serverNo int32, args *BasicArgs) {
	reply := &Reply{}
	DPrintf("[Client]Command %d Is Complete, Send Report RPC To ShardCtrler %d", args.CmdId, serverNo)
	for ok := false; !ok; {
		ok = clientEnd.Call("ShardKV.Report", args, reply)
	}
}
