package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	utils "github.com/wfnuser/silverboat/test"
	"testing"
)

func BenchmarkTransactSet(b *testing.B) {
	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()
	b.ResetTimer()

	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.Set(fdb.Key(utils.RandStringBytesRmndr(10)), []byte(utils.RandStringBytesRmndr(10)))
				return nil, nil
			})
		}
	})
}

func BenchmarkTransactGet(b *testing.B) {
	fdb.MustAPIVersion(620)
	db := fdb.MustOpenDefault()
	key := utils.RandStringBytesRmndr(10)
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(fdb.Key(key), []byte(utils.RandStringBytesRmndr(10)))
		return nil, nil
	})
	b.ResetTimer()

	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.Get(fdb.Key(key))
				return nil, nil
			})
		}
	})
}