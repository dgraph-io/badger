package table

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sort"
	"testing"
)

func key(i int) []byte {
	return []byte(fmt.Sprintf("%04d-%04d", i/37, i))
}

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
		keys = append(keys, string(key(i)))
	}
	sort.Strings(keys)
	for idx, key := range keys {
		val := []byte(fmt.Sprintf("%d", idx))
		if err := b.Add([]byte(key), val); err != nil {
			t.Logf("Stopped adding more keys")
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

	seek := key(1010)
	t.Logf("Seeking to: %q", seek)
	bi, err := table.BlockIteratorForKey(seek)
	if err != nil {
		t.Fatalf("While getting iterator: %v", err)
	}

	fn := func(k, v []byte) {
		t.Logf("ITERATOR key=%q. val=%q.\n", k, v)
	}

	for bi.Init(); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}
	fmt.Println("SEEKING")
	for bi.Seek(seek, 0); bi.Valid(); bi.Next() {
		bi.KV(fn)
	}

	fmt.Println("SEEKING BACKWARDS")
	for bi.Seek(seek, 0); bi.Valid(); bi.Prev() {
		bi.KV(fn)
	}

	bi.Seek(seek, 0)
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, seek) != 0 {
			t.Error("Wrong seek. Wanted: %q. Got: %q", seek, k)
		}
	})

	bi.Prev()
	bi.Prev()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1008)) != 0 {
			t.Error("Wrong prev. Wanted: %q. Got: %q", key(1008), k)
		}
	})
	bi.Next()
	bi.Next()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, seek) != 0 {
			t.Error("Wrong next. Wanted: %q. Got: %q", seek, k)
		}
	})

	for bi.Seek(key(2000), 1); bi.Valid(); bi.Next() {
		t.Fatalf("This shouldn't be triggered.")
	}
	bi.Seek(key(1010), 0)
	for bi.Seek(key(2000), 1); bi.Valid(); bi.Prev() {
		t.Fatalf("This shouldn't be triggered.")
	}
	bi.Seek(key(2000), 0)
	bi.Prev()
	if !bi.Valid() {
		t.Fatalf("This should point to the last element in the block.")
	}
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1099)) != 0 {
			t.Errorf("Wrong prev. Wanted: %q. Got: %q", key(1099), k)
		}
	})

	bi.Reset()
	bi.Prev()
	bi.Next()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1000)) != 0 {
			t.Errorf("Wrong prev. Wanted: %q. Got: %q", key(1000), k)
		}
	})

	bi.Seek(key(1001), 0)
	bi.Prev()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1000)) != 0 {
			t.Errorf("Wrong prev. Wanted: %q. Got: %q", key(1000), k)
		}
	})
	bi.Prev()
	if bi.Valid() {
		t.Errorf("Shouldn't be valid")
	}
	bi.Next()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1000)) != 0 {
			t.Errorf("Wrong next. Wanted: %q. Got: %q", key(1000), k)
		}
	})
	bi.Prev()
	if bi.Valid() {
		t.Errorf("Shouldn't be valid")
	}
	bi.Next()
	bi.KV(func(k, v []byte) {
		if bytes.Compare(k, key(1000)) != 0 {
			t.Errorf("Wrong next. Wanted: %q. Got: %q", key(1000), k)
		}
	})
}
