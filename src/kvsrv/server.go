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
	lock sync.Mutex

	db          map[string]string
	history     map[int64]map[string]string
	clientState map[int64]int32
}

func (kv *KVServer) getClientState(clientID int64) int32 {
	return kv.clientState[clientID]
}

func (kv *KVServer) setClientState(clientID int64, id int32) {
	kv.clientState[clientID] = id
}

func (kv *KVServer) checkClientState(clientID int64, id int32, key string) bool {
	curId := kv.getClientState(clientID)
	if id <= curId {
		return false
	}
	kv.setClientState(clientID, id)

	DPrintf("[KVServer]Client:%v ID:%v, NewId: %v\n", clientID, curId, id)
	return true
}

func (kv *KVServer) setHistory(clientID int64, key, value string) {
	val := kv.history[clientID]
	if val == nil {
		kv.history[clientID] = make(map[string]string)
	}
	kv.history[clientID][key] = value
}

func (kv *KVServer) getHistory(clientID int64, key string) string {
	return kv.history[clientID][key]
}

func (kv *KVServer) clearHistory(clientID int64, key string) {
	delete(kv.history[clientID], key)
	if len(kv.history[clientID]) == 0 {
		kv.history[clientID] = make(map[string]string)
	}
}

func (kv *KVServer) Report(args *ReportArgs, reply *ReportReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	clientID := args.ClientID
	key := args.Key

	kv.clearHistory(clientID, key)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	reply.Value = kv.db[args.Key]

	DPrintf("[KVServer]Get:%v->%v\n", (*args).string(), reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	clientID := args.ClientID
	key := args.Key
	value := args.Value

	ok := kv.checkClientState(clientID, args.ID, key)
	if ok {
		kv.db[key] = value
		kv.setHistory(clientID, key, value)
	}

	reply.Value = kv.getHistory(clientID, key)
	DPrintf("[KVServer]Put:%v->%v\n", (*args).string(), value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	clientID := args.ClientID
	key := args.Key
	value := args.Value

	ok := kv.checkClientState(args.ClientID, args.ID, key)
	if ok {
		kv.setHistory(clientID, key, kv.db[key])
		kv.db[key] += value
	}

	reply.Value = kv.getHistory(clientID, key)
	DPrintf("[KVServer]Append:%v->%v\n", (*args).string(), reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[string]string)
	kv.clientState = make(map[int64]int32)
	return kv
}
