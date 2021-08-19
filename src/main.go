package main

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	queue "github.com/wfnuser/silverboat/src/model"
	"strconv"
)

func main() {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(620)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	q := queue.NewQueue("testqueue1", db)

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
		fmt.Println(string(bytes))
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
		fmt.Println(string(bytes))
	}

	return
}
