package provider

import (
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
)

type Queue struct {
	entries []cid.Cid
}

func NewQueue() *Queue {
	return &Queue{
		entries: make([]cid.Cid, 0),
	}
}

func (q *Queue) Enqueue(cid cid.Cid) {
	q.entries = append(q.entries, cid)
}

func (q *Queue) Dequeue() cid.Cid {
	entry := q.entries[0]
	q.entries = q.entries[1:]
	return entry
}

func (q *Queue) IsEmpty() bool {
	return len(q.entries) == 0
}
