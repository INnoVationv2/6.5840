package shardctrler

import (
	"sync"
)

type InMemoryDB struct {
	mu      sync.RWMutex
	configs []Config
}

func buildInMemoryDB() *InMemoryDB {
	db := &InMemoryDB{
		configs: make([]Config, 1),
	}
	db.configs[0].Groups = map[int][]string{}
	return db
}

func (db *InMemoryDB) get(idx int) *Config {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if length := len(db.configs); idx == -1 || idx >= length {
		idx = length - 1
	}

	return &db.configs[idx]
}

func (db *InMemoryDB) append(conf *Config) {
	db.mu.Lock()
	defer db.mu.Unlock()

	DPrintf("Add New Config:%v\n", conf)
	db.configs = append(db.configs, *conf)
}

func (db *InMemoryDB) size() int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return len(db.configs)
}
