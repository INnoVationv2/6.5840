package shardctrler

type InMemoryDB struct {
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
	if length := len(db.configs); idx == -1 || idx >= length {
		idx = length - 1
	}
	return &db.configs[idx]
}

func (db *InMemoryDB) append(conf *Config) {
	DPrintf("Add New Config:%v\n", conf)
	db.configs = append(db.configs, *conf)
}

func (db *InMemoryDB) size() int {
	return len(db.configs)
}
