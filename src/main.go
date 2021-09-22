package main

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	model "github.com/wfnuser/silverboat/src/model"
	"math/rand"
	"strconv"
)

func pq_run(db fdb.Database) {
	pq := model.NewPQ("testpq1", db)

	for i := 1; i <= 50; i++ {
		pq.Push([]byte("testpq"+ strconv.Itoa(i)), rand.Intn(3))
	}
	for i := 1; i <= 50; i++ {
		bytes := pq.Pop(true)
		fmt.Println(string(bytes.([]byte)))
	}
}

func queue_run(db fdb.Database) {
	q := model.NewQueue("testqueue1", db)

	fmt.Println("Enqueue 10 elements")
	for i := 1; i <= 10; i++ {
		q.Enqueue([]byte("test" + strconv.Itoa(i)))
	}
	fmt.Println("Dequeue 5 elements")
	for i := 1; i <= 5; i++ {
		bytes, e := q.Dequeue()
		if e != nil {
			fmt.Println(e)
		}
		fmt.Println(bytes.([]byte))
	}

	fmt.Println("Enqueue 10 elements")
	for i := 11; i <= 20; i++ {
		q.Enqueue([]byte("2test" + strconv.Itoa(i)))
	}

	fmt.Println("Dequeue 15 elements")
	for i := 1; i <= 15; i++ {
		bytes, e := q.Dequeue()
		if e != nil {
			fmt.Println(e)
		}
		fmt.Println(bytes.([]byte))
	}
}

func main() {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(620)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	//queue_run(db)
	pq_run(db)

	return
}
