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
	for i:=0; i<b.N; i++ {
		db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(fdb.Key(utils.RandStringBytesRmndr(10)), []byte(utils.RandStringBytesRmndr(10)))
			return nil, nil
		})
	}
}