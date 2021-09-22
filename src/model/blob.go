package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func NewBlob(id string, db fdb.Database, chunk int) *Blob {
	return &Blob{
		db:  db,
		sub: subspace.Sub("B").Sub(id),
		chunk: chunk,
	}
}

type Blob struct {
	db  fdb.Database
	sub subspace.Subspace
	chunk int
}

func (blb *Blob) ClearAll() {
	blb.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(blb.sub.Sub())
		return nil, nil
	})
}

func (blb *Blob) WriteBlob(key string, blob []byte) (err error) {
	_, err = blb.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if len(blob) == 0 {
			return nil, nil
		}

		for i := 0; i < len(blob); i += blb.chunk {
			if i+blb.chunk <= len(blob) {
				tr.Set(blb.sub.Pack(tuple.Tuple{key, i}), blob[i:i+blb.chunk])
			} else {
				tr.Set(blb.sub.Pack(tuple.Tuple{key, i}), blob[i:])
			}
		}

		return nil, nil
	})

	return
}

func (blb *Blob) ReadBlob(key string) ([]byte, error) {

	data, err := blb.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		var blob []byte
		kr, e := fdb.PrefixRange(blb.sub.Pack(tuple.Tuple{key}))
		if e != nil {
			return nil, e
		}
		//fdb.PrefixRange(tbl.row.Pack(tuple.Tuple{row}))
		ri := rtr.GetRange(kr, fdb.RangeOptions{}).Iterator()

		for ri.Advance() {
			kv := ri.MustGet()
			blob = append(blob, rtr.Get(kv.Key).MustGet()...)
		}

		return blob, nil
	})

	if err != nil {
		return nil, err
	}

	return data.([]byte), nil
}