package main

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"log"
)

func main() {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(710)

	// Open the default database from the system cluster
	db := fdb.MustOpenDefault()

	// Database reads and writes happen inside transactions
	ret, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(fdb.Key("hello"), []byte("world"))
		return tr.Get(fdb.Key("foo")).MustGet(), nil
		// db.Transact automatically commits (and if necessary,
		// retries) the transaction
	})
	if e != nil {
		log.Fatalf("Unable to perform FDB transaction (%v)", e)
	}

	fmt.Printf("hello is now world, foo was: %s\n", string(ret.([]byte)))
}
