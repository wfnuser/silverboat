package queue

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"log"
)

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

func (q *Queue) Dequeue() ([]byte, error) {
	ret, e := q.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		item, e := q.FirstItem()
		if e != nil {
			return nil, e
		}
		tr.Clear(item.Key)
		return item.Value, nil
	})

	return ret.([]byte), e
}

func (q *Queue) Enqueue(value []byte) []byte {
	ret, e := q.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		li := q.LastIndex() + 1
		tr.Set(q.sub.Pack(tuple.Tuple{li}), value)
		return tr.Get(q.sub.Pack(tuple.Tuple{li})).MustGet(), nil
	})

	if e != nil {
		log.Fatalf("Unable to enqueue (%v)", e)
	}

	return ret.([]byte)
}

func (q *Queue) FirstItem() (fdb.KeyValue, error) {
	ret, e := q.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		pr, _ := fdb.PrefixRange(q.sub.Bytes())
		kvs, e := tr.Snapshot().GetRange(pr, fdb.RangeOptions{Limit: 1}).GetSliceWithError()

		if e != nil {
			log.Fatalf("Unable to read range: %v\n", e)
			return fdb.KeyValue{}, e
		}

		for _, kv := range kvs {
			return kv, nil
		}

		return nil, nil
	})

	return ret.(fdb.KeyValue), e
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
