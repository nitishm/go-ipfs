package provider

import (
	"errors"
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	ds "gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore"
	"gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore/query"
	"math"
	"strconv"
	"strings"
	"sync"
)

const providingQueuePrefix = "/provider/queue/"

type Queue struct {
	lock sync.RWMutex

	tail uint64
	head uint64

	datastore ds.Datastore
}

func NewQueue(datastore ds.Datastore) *Queue {
	q := &Queue{
		lock: sync.RWMutex{},
		tail: 0,
		head: math.MaxUint64,
		datastore: datastore,
	}
	q.prepare()
	return q
}

func (q *Queue) Enqueue(cid cid.Cid) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	nextKey := providingKey(q.tail)

	if err := q.datastore.Put(nextKey, cid.Bytes()); err != nil {
		return err
	}

	q.tail++
	return nil
}

func (q *Queue) Dequeue() (*cid.Cid, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.IsEmpty() {
		// TODO figure out how IPFS folks are doing custom errors and make this comply
		return nil, errors.New("queue is empty")
	}

	nextKey := providingKey(q.head)
	value, err := q.datastore.Get(nextKey)
	if err != nil {
		return nil, err
	}

	cid, err := cid.Parse(value)
	if err != nil {
		return nil, err
	}

	if err := q.datastore.Delete(nextKey); err != nil {
		return nil, err
	}

	q.head++
	return &cid, nil
}

func (q *Queue) IsEmpty() bool {
	return q.Length() == 0
}

func (q *Queue) Length() uint64 {
	return q.tail - q.head
}

func (q *Queue) prepare() error {
	q.lock.RLock()
	defer q.lock.RUnlock()

	query := query.Query{Prefix: providingQueuePrefix}
	results, err := q.datastore.Query(query)
	if err != nil {
		return err
	}

	// TODO: this is maybe dumb, maybe the entry id can be prepended to the bytes that make up the CID so that
	// we can pull the entry id out without having to parse the whole cid or some other Entry-like struct?
	for entry := range results.Next() {
		keyId := strings.TrimPrefix(entry.Key, providingQueuePrefix)
		id, err := strconv.ParseUint(keyId, 10, 64)
		if err != nil {
			return err
		}

		if id < q.head {
			q.head = id
		}

		if id > q.tail {
			q.tail = id
		}
	}

	if q.head == math.MaxUint64 {
		q.head = 0
	}

	return nil
}

func providingKey(id uint64) ds.Key {
	return ds.NewKey(providingQueuePrefix + strconv.FormatUint(id, 10))
}

