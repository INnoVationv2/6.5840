package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	rwLock sync.RWMutex

	db map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.rwLock.RLock()
	defer kv.rwLock.RUnlock()

	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	kv.db[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.rwLock.Lock()
	defer kv.rwLock.Unlock()

	value := kv.db[args.Key]
	reply.Value = value
	value += args.Value
	kv.db[args.Key] = value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string]string)
	return kv
}
