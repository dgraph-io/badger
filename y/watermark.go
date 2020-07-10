/*
 * Copyright 2016-2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"container/heap"
	"context"
	"sync"
)

type mark struct {
	index   uint64
	waiters []chan struct{}
	count   int
}

type markHeap []*mark

func (u markHeap) Len() int            { return len(u) }
func (u markHeap) Less(i, j int) bool  { return u[i].index < u[j].index }
func (u markHeap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *markHeap) Push(x interface{}) { *u = append(*u, x.(*mark)) }
func (u *markHeap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

// WaterMark is used to keep track of the minimum un-finished index.  Typically, an index k becomes
// finished or "done" according to a WaterMark once Done(k) has been called
//   1. as many times as Begin(k) has, AND
//   2. a positive number of times.
//
// An index may also become "done" by calling SetDoneUntil at a time such that it is not
// inter-mingled with Begin/Done calls.
//
// Since doneUntil and lastIndex addresses are passed to sync/atomic packages, we ensure that they
// are 64-bit aligned by putting them at the beginning of the structure.
type WaterMark struct {
	Name string

	m              sync.Mutex
	doneUntilValid bool
	doneUntil      uint64
	lastIndex      uint64
	pending        markHeap
	pendingMap     map[uint64]*mark
}

// Init initializes a WaterMark struct.
// Deprecated: provided for backwards compatibility only.
func (w *WaterMark) Init(closer *Closer) {
	closer.Done()
}

// Begin sets the last index to the given value.
func (w *WaterMark) Begin(index uint64) {
	w.m.Lock()
	defer w.m.Unlock()

	w.beginLocked(index)
}

func (w *WaterMark) beginLocked(index uint64) {
	// Do not allow the watermark to go back in time.
	AssertTrue(!w.doneUntilValid || w.doneUntil <= index)

	w.lastIndex = index

	m, ok := w.pendingMap[index]
	if !ok {
		m = &mark{
			index: index,
		}

		if w.pendingMap == nil {
			w.pendingMap = make(map[uint64]*mark)
		}
		w.pendingMap[index] = m

		heap.Push(&w.pending, m)
	}

	m.count++
}

// BeginMany works like Begin but accepts multiple indices.
func (w *WaterMark) BeginMany(indices []uint64) {
	w.m.Lock()
	defer w.m.Unlock()

	for _, index := range indices {
		w.beginLocked(index)
	}
}

// Done sets a single index as done.
func (w *WaterMark) Done(index uint64) {
	w.m.Lock()
	defer w.m.Unlock()

	w.doneLocked(index)
}

func (w *WaterMark) doneLocked(index uint64) {
	m, ok := w.pendingMap[index]
	// Done() should be called after a Begin():
	AssertTrue(ok)
	m.count--

	for len(w.pending) > 0 && w.pending[0].count == 0 {
		m := w.pending[0]
		for _, waitCh := range m.waiters {
			close(waitCh)
		}

		w.doneUntilValid = true
		w.doneUntil = m.index

		delete(w.pendingMap, m.index)
		heap.Pop(&w.pending)
	}
}

// DoneMany works like Done but accepts multiple indices.
func (w *WaterMark) DoneMany(indices []uint64) {
	w.m.Lock()
	defer w.m.Unlock()

	for _, index := range indices {
		w.doneLocked(index)
	}
}

// DoneUntil returns the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) DoneUntil() uint64 {
	w.m.Lock()
	defer w.m.Unlock()

	// TODO: it is not safe to return 0 here, because we cannot guarantee that
	// it is actually done (i.e. Begin(0) has been called at least one), so
	// we have no other solution than to panic.
	return w.doneUntil
}

// SetDoneUntil sets the maximum index that has the property that all indices
// less than or equal to it are done.
func (w *WaterMark) SetDoneUntil(val uint64) {
	w.m.Lock()
	defer w.m.Unlock()
	AssertTrue(len(w.pending) == 0)
	w.doneUntil = val
}

// LastIndex returns the last index for which Begin has been called.
func (w *WaterMark) LastIndex() uint64 {
	w.m.Lock()
	defer w.m.Unlock()
	return w.lastIndex
}

// WaitForMark waits until the given index is marked as done.
func (w *WaterMark) WaitForMark(ctx context.Context, index uint64) error {
	w.m.Lock()
	if len(w.pending) == 0 || w.pending[0].index < index {
		w.m.Unlock()
		return nil
	}

	m, ok := w.pendingMap[index]
	if !ok {
		m = &mark{
			index: index,
		}
		heap.Push(&w.pending, m)
	}

	waitCh := make(chan struct{})
	m.waiters = append(m.waiters, waitCh)
	w.m.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}
