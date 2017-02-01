package table

import (
	"fmt"
	"io/ioutil"
	"sort"
	"testing"
)

func TestBuild(t *testing.T) {
	b := TableBuilder{}
	b.Reset()

	f, err := ioutil.TempFile("", "badger")
	if err != nil {
		t.Error(err)
		t.Fail()
	}
	keys := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("%04d-%04d", i/37, i)
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for idx, key := range keys {
		val := []byte(fmt.Sprintf("%d", idx))
		if err := b.Add([]byte(key), val); err != nil {
			fmt.Printf("Stopped adding more keys")
			// Stop adding.
			break
		}
	}
	f.Write(b.Finish())

	table := Table{
		offset: 0,
		fd:     f,
	}
	if err := table.ReadIndex(); err != nil {
		t.Error(err)
		t.Fail()
	}

	for _, i := range table.blockIndex {
		fmt.Printf("klen: %v. Key=%q. Offset: %v. Len: %v\n", len(i.key), i.key, i.offset, i.len)
	}

	seek := fmt.Sprintf("%04d-%04d", 1010/37, 1010)
	t.Logf("Seeking to: %q", seek)
	bi, err := table.BlockIteratorForKey([]byte(seek))
	if err != nil {
		t.Fatalf("While getting iterator: %v", err)
	}

	fn := func(k, v []byte) {
		fmt.Printf("ITERATOR key=%q. val=%q.\n", k, v)
	}

	for bi.Init(); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}
	fmt.Println("SEEKING")
	for bi.Seek([]byte(seek), 0); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}
}
