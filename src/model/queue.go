package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"log"
)

type EmptyQueueError struct{}

func (q EmptyQueueError) Error() string {
	return "Queue is Empty"
}

func NewQueue(id string, db fdb.Database) *Queue {
	return &Queue{
		db:  db,
		sub: subspace.Sub("Q").Sub(id),
	}
}

type Queue struct {
	db  fdb.Database
	sub subspace.Subspace
}

func (q *Queue) ClearAll() {
	q.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(q.sub.Sub())
		return nil, nil
	})
}

func (q *Queue) Dequeue() (interface{}, error) {
	ret, e := q.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		item, e := q.FirstItem()
		if e != nil {
			return nil, e
		}
		tr.Clear(item.(fdb.KeyValue).Key)
		return item.(fdb.KeyValue).Value, nil
	})

	return ret, e
}

func (q *Queue) Enqueue(value []byte) interface{} {
	ret, e := q.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		li := q.LastIndex() + 1
		tr.Set(q.sub.Pack(tuple.Tuple{li}), value)
		return tr.Get(q.sub.Pack(tuple.Tuple{li})).MustGet(), nil
	})

	if e != nil {
		log.Fatalf("Unable to enqueue (%v)", e)
	}

	return ret
}

func (q *Queue) FirstItem() (interface{}, error) {
	ret, e := q.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		pr, _ := fdb.PrefixRange(q.sub.Bytes())
		kvs, e := tr.Snapshot().GetRange(pr, fdb.RangeOptions{Limit: 1}).GetSliceWithError()

		if e != nil {
			log.Fatalf("Unable to read range: %v\n", e)
			return nil, e
		}

		for _, kv := range kvs {
			return kv, nil
		}

		return nil, EmptyQueueError{}
	})

	return ret, e
}

func (q *Queue) LastIndex() int64 {
	ret, e := q.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		pr, _ := fdb.PrefixRange(q.sub.Bytes())
		kvs, e := tr.Snapshot().GetRange(pr, fdb.RangeOptions{Reverse: true, Limit: 1}).GetSliceWithError()

		if e != nil {
			log.Fatalf("Unable to read range: %v\n", e)
			return int64(0), e
		}

		for _, kv := range kvs {
			tup, e := q.sub.Unpack(kv.Key)
			if e != nil {
				return int64(0), e
			}
			return tup[0], e

		}

		return int64(0), nil
	})

	if e != nil {
		log.Fatalf("Unable to perform FDB transaction (%v)", e)
	}

	return ret.(int64)
}
