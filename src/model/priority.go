package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"math/rand"
	"time"
)

type PQ struct {
	db  fdb.Database
	sub subspace.Subspace
}

func NewPQ(id string, db fdb.Database) *PQ {
	return &PQ{
		db:  db,
		sub: subspace.Sub("PQ").Sub(id),
	}
}

func _pack(t interface{}) []byte {
	return tuple.Tuple{t}.Pack()
}

func _unpack(b []byte) tuple.Tuple {
	i, e := tuple.Unpack(b)
	if e != nil {
		return nil
	}
	return i
}

func (pq *PQ) Push(value interface{}, priority int) {
	pq.db.Transact(func(tr fdb.Transaction) (interface{}, error){
		tr.Set(pq.sub.Pack(tuple.Tuple{
			priority,
			-time.Now().UnixNano(),
			rand.Intn(20),
		}), _pack(value))
		return nil, nil
	})
}

func (pq *PQ) Pop(max bool) interface{} {
	res, _ := pq.db.Transact(func (tr fdb.Transaction) (interface{}, error){
		kvs, e := tr.Snapshot().GetRange(pq.sub, fdb.RangeOptions{Limit: 1, Reverse: max}).GetSliceWithError()
		tr.Snapshot().GetRange(pq.sub, fdb.RangeOptions{}).Iterator().Advance()
		if e != nil {
			return nil, e
		}
		if len(kvs) == 0 {
			return nil, nil
		}
		tr.Clear(kvs[0].Key)
		return _unpack(kvs[0].Value)[0], nil
	})
	return res
}