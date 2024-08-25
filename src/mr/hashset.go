package mr

import (
	"sync"
)

type HashSet struct {
	values map[string]Job
	lock   sync.RWMutex
}

func NewHashSet() *HashSet {
	return &HashSet{
		values: make(map[string]Job),
	}
}

func (hashSet *HashSet) Add(jobs ...Job) {
	hashSet.lock.Lock()
	defer hashSet.lock.Unlock()
	for _, job := range jobs {
		(*hashSet).values[job.JobName] = job
	}
}

func (hashSet *HashSet) Remove(job *Job) {
	hashSet.lock.Lock()
	defer hashSet.lock.Unlock()
	delete((*hashSet).values, job.JobName)
}

func (hashSet *HashSet) Contains(job *Job) bool {
	hashSet.lock.RLock()
	defer hashSet.lock.RUnlock()

	_, ok := (*hashSet).values[job.JobName]
	return ok
}

func (hashSet *HashSet) Size() int {
	hashSet.lock.RLock()
	defer hashSet.lock.RUnlock()

	return len((*hashSet).values)
}

func (hashSet *HashSet) IsEmpty() bool {
	hashSet.lock.RLock()
	defer hashSet.lock.RUnlock()

	empty := hashSet.Size() == 0
	return empty
}

func (hashSet *HashSet) GetFirstElem() *Job {
	hashSet.lock.RLock()
	defer hashSet.lock.RUnlock()

	var res Job
	for _, job := range hashSet.values {
		res = job
		break
	}

	return &res
}
