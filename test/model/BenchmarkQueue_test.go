package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	queue "github.com/wfnuser/silverboat/src/model"
	utils "github.com/wfnuser/silverboat/test"
	"testing"
)


func BenchmarkEnqueue(b *testing.B) {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(620)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()
	q := queue.NewQueue("benchmark_queue", db)
	b.ResetTimer()

	for i:=0; i<b.N; i++ {
		q.Enqueue([]byte(utils.RandStringBytesRmndr(10)))
	}
}