// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package record

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type syncErrorFile struct {
	vfs.File
	err error
}

func (f syncErrorFile) Sync() error {
	return f.err
}

func TestSyncQueue(t *testing.T) {
	var q syncQueue
	var closed int32

	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()
		for {
			if atomic.LoadInt32(&closed) == 1 {
				return
			}
			head, tail := q.load()
			q.pop(head, tail, nil)
		}
	}()

	var commitMu sync.Mutex
	var doneWG sync.WaitGroup
	for i := 0; i < SyncConcurrency; i++ {
		doneWG.Add(1)
		go func(i int) {
			defer doneWG.Done()
			for j := 0; j < 1000; j++ {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				// syncQueue is a single-producer, single-consumer queue. We need to
				// provide mutual exclusion on the producer side.
				commitMu.Lock()
				q.push(wg, new(error))
				commitMu.Unlock()
				wg.Wait()
			}
		}(i)
	}
	doneWG.Wait()

	atomic.StoreInt32(&closed, 1)
	flusherWG.Wait()
}

func TestFlusherCond(t *testing.T) {
	var mu sync.Mutex
	var q syncQueue
	var c flusherCond
	var closed bool

	c.init(&mu, &q)

	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	go func() {
		defer flusherWG.Done()

		mu.Lock()
		defer mu.Unlock()

		for {
			for {
				if closed {
					return
				}
				if !q.empty() {
					break
				}
				c.Wait()
			}

			head, tail := q.load()
			q.pop(head, tail, nil)
		}
	}()

	var commitMu sync.Mutex
	var doneWG sync.WaitGroup
	// NB: we're testing with low concurrency here, because what we want to
	// stress is that signalling of the flusherCond works
	// correctly. Specifically, we want to make sure that a signal is "lost",
	// causing the test to wedge.
	for i := 0; i < 2; i++ {
		doneWG.Add(1)
		go func(i int) {
			defer doneWG.Done()
			for j := 0; j < 10000; j++ {
				wg := &sync.WaitGroup{}
				wg.Add(1)
				// syncQueue is a single-producer, single-consumer queue. We need to
				// provide mutual exclusion on the producer side.
				commitMu.Lock()
				q.push(wg, new(error))
				commitMu.Unlock()
				c.Signal()
				wg.Wait()
			}
		}(i)
	}
	doneWG.Wait()

	mu.Lock()
	closed = true
	c.Signal()
	mu.Unlock()
	flusherWG.Wait()
}

func TestSyncError(t *testing.T) {
	mem := vfs.NewMem()
	f, err := mem.Create("log")
	require.NoError(t, err)

	injectedErr := errors.New("injected error")
	w := NewLogWriter(syncErrorFile{f, injectedErr}, 0)

	syncRecord := func() {
		var syncErr error
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		_, err = w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		if injectedErr != syncErr {
			t.Fatalf("unexpected %v but found %v", injectedErr, syncErr)
		}
	}
	// First waiter receives error.
	syncRecord()
	// All subsequent waiters also receive the error.
	syncRecord()
	syncRecord()
}

type syncFile struct {
	writePos int64
	syncPos  int64
}

func (f *syncFile) Write(buf []byte) (int, error) {
	n := len(buf)
	atomic.AddInt64(&f.writePos, int64(n))
	return n, nil
}

func (f *syncFile) Sync() error {
	atomic.StoreInt64(&f.syncPos, atomic.LoadInt64(&f.writePos))
	return nil
}

func TestSyncRecord(t *testing.T) {
	f := &syncFile{}
	w := NewLogWriter(f, 0)

	var syncErr error
	for i := 0; i < 100000; i++ {
		var syncWG sync.WaitGroup
		syncWG.Add(1)
		offset, err := w.SyncRecord([]byte("hello"), &syncWG, &syncErr)
		require.NoError(t, err)
		syncWG.Wait()
		require.NoError(t, syncErr)
		if v := atomic.LoadInt64(&f.writePos); offset != v {
			t.Fatalf("expected write pos %d, but found %d", offset, v)
		}
		if v := atomic.LoadInt64(&f.syncPos); offset != v {
			t.Fatalf("expected sync pos %d, but found %d", offset, v)
		}
	}
}

type fakeTimer struct {
	f func()
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	return false
}

func (t *fakeTimer) Stop() bool {
	return false
}

func try(initialSleep, maxTotalSleep time.Duration, f func() error) error {
	totalSleep := time.Duration(0)
	for d := initialSleep; ; d *= 2 {
		time.Sleep(d)
		totalSleep += d
		if err := f(); err == nil || totalSleep >= maxTotalSleep {
			return err
		}
	}
}

func TestMinSyncInterval(t *testing.T) {
	const minSyncInterval = 100 * time.Millisecond

	f := &syncFile{}
	w := NewLogWriter(f, 0)
	w.SetMinSyncInterval(func() time.Duration {
		return minSyncInterval
	})

	var timer fakeTimer
	w.afterFunc = func(d time.Duration, f func()) syncTimer {
		if d != minSyncInterval {
			t.Fatalf("expected minSyncInterval %s, but found %s", minSyncInterval, d)
		}
		timer.f = f
		timer.Reset(d)
		return &timer
	}

	syncRecord := func(n int) *sync.WaitGroup {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err := w.SyncRecord(bytes.Repeat([]byte{'a'}, n), wg, new(error))
		require.NoError(t, err)
		return wg
	}

	// Sync one record which will cause the sync timer to kick in.
	syncRecord(1).Wait()

	startWritePos := atomic.LoadInt64(&f.writePos)
	startSyncPos := atomic.LoadInt64(&f.syncPos)

	// Write a bunch of large records. The sync position should not change
	// because we haven't triggered the timer. But note that the writes should
	// not block either even though syncing isn't being done.
	var wg *sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg = syncRecord(10000)
		if v := atomic.LoadInt64(&f.syncPos); startSyncPos != v {
			t.Fatalf("expected syncPos %d, but found %d", startSyncPos, v)
		}
		// NB: we can't use syncQueue.load() here as that will return 0,0 while the
		// syncQueue is blocked.
		head, tail := w.flusher.syncQ.unpack(atomic.LoadUint64(&w.flusher.syncQ.headTail))
		waiters := head - tail
		if waiters != uint32(i+1) {
			t.Fatalf("expected %d waiters, but found %d", i+1, waiters)
		}
	}

	err := try(time.Millisecond, 5*time.Second, func() error {
		v := atomic.LoadInt64(&f.writePos)
		if v > startWritePos {
			return nil
		}
		return errors.Errorf("expected writePos > %d, but found %d", startWritePos, v)
	})
	require.NoError(t, err)

	// Fire the timer, and then wait for the last record to sync.
	timer.f()
	wg.Wait()

	if w, s := atomic.LoadInt64(&f.writePos), atomic.LoadInt64(&f.syncPos); w != s {
		t.Fatalf("expected syncPos %d, but found %d", s, w)
	}
}

func TestMinSyncIntervalClose(t *testing.T) {
	const minSyncInterval = 100 * time.Millisecond

	f := &syncFile{}
	w := NewLogWriter(f, 0)
	w.SetMinSyncInterval(func() time.Duration {
		return minSyncInterval
	})

	var timer fakeTimer
	w.afterFunc = func(d time.Duration, f func()) syncTimer {
		if d != minSyncInterval {
			t.Fatalf("expected minSyncInterval %s, but found %s", minSyncInterval, d)
		}
		timer.f = f
		timer.Reset(d)
		return &timer
	}

	syncRecord := func(n int) *sync.WaitGroup {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err := w.SyncRecord(bytes.Repeat([]byte{'a'}, n), wg, new(error))
		require.NoError(t, err)
		return wg
	}

	// Sync one record which will cause the sync timer to kick in.
	syncRecord(1).Wait()

	// Syncing another record will not complete until the timer is fired OR the
	// writer is closed.
	wg := syncRecord(1)
	require.NoError(t, w.Close())
	wg.Wait()
}
