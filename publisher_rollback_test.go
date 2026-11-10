/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
)

// A failed newSubscriber must not leave a subscriber in the map, orphaned
// trie entries, or a Closer whose WaitGroup is never Done. Otherwise the
// deferred cleanSubscribers in DB.Close hangs forever.
func TestNewSubscriberRollbackOnInvalidMatch(t *testing.T) {
	p := newPublisher()
	matches := []pb.Match{
		{Prefix: []byte("valid"), IgnoreBytes: ""},
		{Prefix: []byte("bad"), IgnoreBytes: "not-a-number"},
	}
	_, err := p.newSubscriber(z.NewCloser(1), matches)
	if err == nil {
		t.Fatal("expected an error for an invalid IgnoreBytes match")
	}
	id := p.nextID - 1
	if got := len(p.subscribers); got != 0 {
		t.Fatalf("leaked subscriber in map: got %d, want 0 (id=%d)", got, id)
	}
	if ids := p.indexer.Get([]byte("valid-anything")); len(ids) != 0 {
		t.Fatalf("orphaned trie entries for id=%d: %v", id, ids)
	}
}

func TestSubscribeInvalidMatchDoesNotDeadlockClose(t *testing.T) {
	db, err := Open(DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Subscribe(context.Background(), func(*pb.KVList) error { return nil },
		[]pb.Match{{Prefix: []byte("valid")}, {Prefix: []byte("bad"), IgnoreBytes: "not-a-number"}})
	if err == nil {
		t.Fatal("expected Subscribe to fail on an invalid match")
	}

	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case cerr := <-done:
		if cerr != nil {
			t.Fatalf("DB.Close returned: %v", cerr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("DB.Close deadlocked after a failed Subscribe: leaked subscriber closer was never Done")
	}
}
