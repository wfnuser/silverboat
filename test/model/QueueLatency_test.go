package model

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	queue "github.com/wfnuser/silverboat/src/model"
	utils "github.com/wfnuser/silverboat/test"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestLatency(t *testing.T) {
	fdb.MustAPIVersion(620)
	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()
	utils.ClearSubspace(db, subspace.Sub("Q"))

	q := queue.NewQueue("benchmark_queue", db)

	var latencySum int64 = 0
	var cnt int64 = 1

	background := context.Background()
	ctx, _ := context.WithTimeout(background, 60*time.Second)
	var wg sync.WaitGroup

	wg.Add(1)
	// read
	fmt.Println("start reading")
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("read done")
				return
			default:
				timeBytes, e := q.Dequeue()
				if e == nil && timeBytes != nil {
					now := time.Now().UnixNano()
					timeStr := string(timeBytes.([]byte))
					t, e := strconv.ParseInt(timeStr, 10, 64)
					if e != nil {
						continue
					}
					latencySum = latencySum + now - t
					cnt++
				}
			}
		}
	}(ctx)

	wg.Add(1)
	// write
	fmt.Println("start writing")
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				fmt.Println("write done")
				return
			default:
				timeStr := strconv.FormatInt(time.Now().UnixNano(), 10)
				timeBytes := []byte(timeStr)
				q.Enqueue(timeBytes)
			}
		}
	}(ctx)

	wg.Wait()

	fmt.Printf("avg latency: %+v, total cnt: %+v\n", (latencySum+0.0)/cnt, cnt)
}
