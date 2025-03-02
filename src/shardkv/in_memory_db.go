package shardkv

import "6.5840/shardctrler"

type ShardStatus int

const (
	Unavailable ShardStatus = iota
	Available
)

func (s ShardStatus) String() string {
	switch s {
	case Unavailable:
		return "Unavailable"
	case Available:
		return "Available"
	default:
		return "Unknown Shard Status"
	}
}

type InMemoryDB struct {
	shardStatus [shardctrler.NShards]ShardStatus
	dataMaps    [shardctrler.NShards]map[string]string
}

func buildInMemoryDB() *InMemoryDB {
	return &InMemoryDB{}
}

func (db *InMemoryDB) get(key string) string {
	shard := key2shard(key)

	dataMap := db.dataMaps[shard]
	if dataMap == nil {
		return ""
	}
	return dataMap[key]
}

func (db *InMemoryDB) set(key, val string) {
	shard := key2shard(key)

	if db.dataMaps[shard] == nil {
		db.dataMaps[shard] = make(map[string]string)
	}
	db.dataMaps[shard][key] = val
}

func (db *InMemoryDB) append(key, val string) {
	shard := key2shard(key)

	if db.dataMaps[shard] == nil {
		db.dataMaps[shard] = make(map[string]string)
	}
	value := db.dataMaps[shard][key]
	value += val
	db.dataMaps[shard][key] = value
}

func (db *InMemoryDB) setDB(shard int, val map[string]string) {
	if shard >= 0 && shard < shardctrler.NShards && val != nil {
		db.dataMaps[shard] = val
	}
	db.setShardStatus(shard, Available)
}

func (db *InMemoryDB) export(shard int) map[string]string {
	if db.dataMaps[shard] == nil {
		return nil
	}
	exportMap := make(map[string]string)
	for key, value := range db.dataMaps[shard] {
		exportMap[key] = value
	}
	return exportMap
}

func (db *InMemoryDB) getShardStatus(shard int) ShardStatus {
	return db.shardStatus[shard]
}

func (db *InMemoryDB) setShardStatus(shard int, newStatus ShardStatus) {
	if newStatus == Unavailable {
		db.dataMaps[shard] = nil
	}
	db.shardStatus[shard] = newStatus
}
