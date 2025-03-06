package shardkv

import (
	"6.5840/shardctrler"
	"sync"
)

type ShardStatus int

const (
	Unavailable ShardStatus = iota
	Available
	WaitSend
	Sending
	Receiving
)

func (s ShardStatus) String() string {
	switch s {
	case Unavailable:
		return "Unavailable"
	case Available:
		return "Available"
	case WaitSend:
		return "WaitSend"
	case Sending:
		return "Sending"
	default:
		return "Unknown ShardNum Status"
	}
}

type ShardDetail struct {
	ShardNum int
	OwnerGid int
	ConfNum  int
	Status   ShardStatus
	Data     map[string]string
}

type InMemoryDB struct {
	mu     sync.RWMutex
	shards [shardctrler.NShards]*ShardDetail
}

func buildInMemoryDB() (db *InMemoryDB) {
	db = &InMemoryDB{}
	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		db.shards[shardNum] = &ShardDetail{ShardNum: shardNum}
	}
	return
}

func (db *InMemoryDB) get(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	shard := db.shards[key2shard(key)]
	if shard.Data == nil {
		return ""
	}
	return shard.Data[key]
}

func (db *InMemoryDB) put(key, val string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	shard := db.shards[key2shard(key)]
	if shard.Data == nil {
		shard.Data = make(map[string]string)
	}
	shard.Data[key] = val
}

func (db *InMemoryDB) append(key, val string) string {
	db.mu.Lock()
	defer db.mu.Unlock()

	shard := db.shards[key2shard(key)]
	if shard.Data == nil {
		shard.Data = make(map[string]string)
	}
	shard.Data[key] += val
	return shard.Data[key]
}

func (db *InMemoryDB) setShard(newShard *Shard) {
	db.mu.Lock()
	defer db.mu.Unlock()

	DPrintf("[DB]Set Shard[%d]", newShard.ShardNum)
	shard := db.shards[newShard.ShardNum]
	if shard.Status != Unavailable && shard.ConfNum >= newShard.ConfNum {
		DPrintf("Shard[%d] ShardStatus:%v, ShardConfNum:%d, NewShardConfNum:%d", shard.ShardNum, shard.Status, shard.ConfNum, newShard.ConfNum)
		return
	}

	shard.ConfNum = newShard.ConfNum
	shard.Data = newShard.Data
	shard.Status = Available
	DPrintf("Set Shard[%d],Data:%v", shard.ShardNum, shard.Data)
}

func (db *InMemoryDB) deleteShard(shardNum, confNum int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	DPrintf("[DB]Delete Shard[%d]", shardNum)
	shard := db.shards[shardNum]
	if shard.ConfNum > confNum {
		DPrintf("Shard ConfNum:%d > %d", shard.ConfNum, confNum)
		return
	}
	shard.ConfNum = confNum
	shard.Data = nil
	shard.Status = Unavailable
}

func (db *InMemoryDB) getShardDetail(shardNum int) (ShardStatus, int) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	shard := db.shards[shardNum]
	return shard.Status, shard.ConfNum
}

func (db *InMemoryDB) setShardStatus(shardNum int, newStatus ShardStatus) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.shards[shardNum].Status = newStatus
}

func (db *InMemoryDB) getShardStatus(shardNum int) ShardStatus {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.shards[shardNum].Status
}

func (db *InMemoryDB) compareAndSwapShardStatus(shardNum int, oldStatus, newStatus ShardStatus) bool {
	db.mu.Lock()
	defer db.mu.Unlock()

	shard := db.shards[shardNum]
	if shard.Status != oldStatus {
		DPrintf("[DB]Current Shard[%d] Status Is %v, Not %v", shardNum, shard.Status, oldStatus)
		return false
	}

	shard.Status = newStatus
	return true
}

func (db *InMemoryDB) setShardConfNum(shardNum, confNum int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	shard := db.shards[shardNum]
	if confNum > shard.ConfNum {
		shard.ConfNum = confNum
	}
}

func (db *InMemoryDB) getShardConfNum(shardNum int) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.shards[shardNum].ConfNum
}

func (db *InMemoryDB) getShardOwnerGid(shardNum int) int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.shards[shardNum].OwnerGid
}

func (db *InMemoryDB) setShardOwnerGid(shardNum, owner int) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.shards[shardNum].OwnerGid = owner
}

func (db *InMemoryDB) setDB(shards [shardctrler.NShards]*ShardDetail) {
	db.mu.Lock()
	defer db.mu.Unlock()

	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		db.shards[shardNum] = shards[shardNum]
	}
}

func (db *InMemoryDB) exportShard(shardNum int) *ShardDetail {
	db.mu.RLock()
	defer db.mu.RUnlock()

	DPrintf("[DB]Export Shard[%d]", shardNum)
	dbShard := db.shards[shardNum]
	exportShard := &ShardDetail{
		ShardNum: dbShard.ShardNum,
		OwnerGid: dbShard.OwnerGid,
		ConfNum:  dbShard.ConfNum,
	}
	exportShard.Data = make(map[string]string)
	if dbShard.Data != nil {
		for key, value := range dbShard.Data {
			exportShard.Data[key] = value
			DPrintf("[Shard[%d]]Key:%s,Value:%s", shardNum, key, value)
		}
	}
	return exportShard
}

func (db *InMemoryDB) exportAll() [shardctrler.NShards]*ShardDetail {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var exportShards [shardctrler.NShards]*ShardDetail
	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		exportShards[shardNum] = &ShardDetail{}
		dbShard, exportShard := db.shards[shardNum], exportShards[shardNum]
		exportShard.ShardNum = dbShard.ShardNum
		exportShard.OwnerGid = dbShard.OwnerGid
		exportShard.ConfNum = dbShard.ConfNum
		exportShard.Status = dbShard.Status
		exportShard.Data = make(map[string]string)
		for key, value := range dbShard.Data {
			exportShard.Data[key] = value
		}
	}
	return exportShards
}
