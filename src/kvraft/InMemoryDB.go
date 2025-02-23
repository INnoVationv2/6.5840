package kvraft

import "sync"

type InMemoryDB struct {
	mu    sync.RWMutex
	value map[string]string
}

func buildInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		value: make(map[string]string),
	}
}

func (db *InMemoryDB) get(key string) string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.value[key]
}

func (db *InMemoryDB) set(key, val string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.value[key] = val
}

func (db *InMemoryDB) append(key, val string) string {
	db.mu.Lock()
	defer db.mu.Unlock()

	val = db.value[key] + val
	db.value[key] = val
	return val
}

func (db *InMemoryDB) setDB(val map[string]string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.value = val
}

func (db *InMemoryDB) export() map[string]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	exportMap := make(map[string]string)
	for key, value := range db.value {
		exportMap[key] = value
	}
	return exportMap
}
