package shardkv

type InMemoryDB struct {
	value map[string]string
}

func buildInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		value: make(map[string]string),
	}
}

func (db *InMemoryDB) get(key string) string {
	return db.value[key]
}

func (db *InMemoryDB) set(key, val string) {
	db.value[key] = val
}

func (db *InMemoryDB) append(key, val string) string {
	val = db.value[key] + val
	db.value[key] = val
	return val
}

func (db *InMemoryDB) setDB(val map[string]string) {
	db.value = val
}

func (db *InMemoryDB) export() map[string]string {
	exportMap := make(map[string]string)
	for key, value := range db.value {
		exportMap[key] = value
	}
	return exportMap
}
