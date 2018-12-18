package provider

import (
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	"sync"
)

type Queue struct {
	lock sync.RWMutex
	entries []cid.Cid
}

func NewQueue() *Queue {
	return &Queue{
		lock: sync.RWMutex{},
		entries: make([]cid.Cid, 0),
	}
}

func (q *Queue) Enqueue(cid cid.Cid) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.entries = append(q.entries, cid)
}

func (q *Queue) Dequeue() cid.Cid {
	q.lock.Lock()
	defer q.lock.Unlock()

	entry := q.entries[0]
	q.entries = q.entries[1:]
	return entry
}

func (q *Queue) IsEmpty() bool {
	return q.Length() == 0
}

func (q *Queue) Length() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return len(q.entries)
}
