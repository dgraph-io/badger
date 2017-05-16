/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
	"sync"
	"sync/atomic"

	"os"
	"syscall"
)

var EmptySlice = []byte{}

func OpenSyncedFile(filename string, sync bool) (*os.File, error) {
	flags := os.O_RDWR | os.O_CREATE
	if sync {
		flags |= syscall.O_DSYNC
	}
	return os.OpenFile(filename, flags, 0666)
}

func Safecopy(a []byte, src []byte) []byte {
	if cap(a) < len(src) {
		a = make([]byte, len(src))
	}
	a = a[:len(src)]
	copy(a, src)
	return a
}

type Slice struct {
	buf []byte
}

func (s *Slice) Resize(sz int) []byte {
	if cap(s.buf) < sz {
		s.buf = make([]byte, sz)
	}
	return s.buf[0:sz]
}

type LevelCloser struct {
	Name    string
	running int32
	nomore  int32
	closed  chan struct{}
	waiting sync.WaitGroup
}

type Closer struct {
	sync.RWMutex
	levels map[string]*LevelCloser
}

func NewCloser() *Closer {
	return &Closer{
		levels: make(map[string]*LevelCloser),
	}
}

func (c *Closer) Register(name string) *LevelCloser {
	c.Lock()
	defer c.Unlock()

	lc, has := c.levels[name]
	if !has {
		lc = &LevelCloser{Name: name, closed: make(chan struct{}, 10)}
		lc.waiting.Add(1)
		c.levels[name] = lc
	}

	AssertTruef(atomic.LoadInt32(&lc.nomore) == 0, "Can't register with closer after signal.")
	atomic.AddInt32(&lc.running, 1)
	return lc
}

func (c *Closer) Get(name string) *LevelCloser {
	c.RLock()
	defer c.RUnlock()

	lc, has := c.levels[name]
	if !has {
		Fatalf("%q not present in Closer", name)
		return nil
	}
	return lc
}

func (c *Closer) SignalAll() {
	c.RLock()
	defer c.RUnlock()

	for _, l := range c.levels {
		l.Signal()
	}
}

func (c *Closer) WaitForAll() {
	c.RLock()
	defer c.RUnlock()

	for _, l := range c.levels {
		l.Wait()
	}
}

func (lc *LevelCloser) NumRunning() int {
	return int(atomic.LoadInt32(&lc.running))
}

func (lc *LevelCloser) Signal() {
	if !atomic.CompareAndSwapInt32(&lc.nomore, 0, 1) {
		// fmt.Printf("Level %q already got signal\n", lc.Name)
		return
	}
	running := int(atomic.LoadInt32(&lc.running))
	// fmt.Printf("Sending signal to %d registered with name %q\n", running, lc.Name)
	for i := 0; i < running; i++ {
		lc.closed <- struct{}{}
	}
}

func (lc *LevelCloser) HasBeenClosed() <-chan struct{} {
	return lc.closed
}

func (lc *LevelCloser) GotSignal() bool {
	return atomic.LoadInt32(&lc.nomore) == 1
}

func (lc *LevelCloser) Done() {
	if atomic.LoadInt32(&lc.running) <= 0 {
		return
	}

	running := atomic.AddInt32(&lc.running, -1)
	if running == 0 {
		lc.waiting.Done()
	}
}

func (lc *LevelCloser) Wait() {
	lc.waiting.Wait()
}

func (lc *LevelCloser) SignalAndWait() {
	lc.Signal()
	lc.Wait()
}
