package badger

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/google/uuid"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestTemp(t *testing.T) {
	go func() {
		fmt.Println("Starting http listen and serve")
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	ch := make(chan bool)

	db, err := Open(DefaultOptions("./b_test/").
		WithCompression(options.None).
		WithIndexCacheSize(256 << 20).
		WithBlockCacheSize(0))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		go func() {
			for {
				key := uuid.NewString()
				raw := make([]byte, 1024*64)

				txn := db.NewTransaction(true)
				txn.SetEntry(NewEntry([]byte(key), raw))
				txn.Commit()

				// txn := db.NewTransactionAt(uint64(time.Now().UnixNano()), true)
				// txn.SetEntry(badger.NewEntry([]byte(key), raw))
				// txn.Commit()
				<-time.After(1 * time.Millisecond)
			}
		}()
	}
	go func() {
		for {
			fmt.Println("cost:", (db.IndexCacheMetrics().CostAdded()-db.IndexCacheMetrics().CostEvicted())/1024.0/1024.0, "MB item:", db.IndexCacheMetrics().KeysAdded()-db.IndexCacheMetrics().KeysEvicted())
			<-time.After(1000 * time.Millisecond)
		}
	}()
	<-ch
}
