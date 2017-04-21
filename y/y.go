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
	"context"
	"fmt"
	"sync/atomic"

	"os"
	"syscall"

	"golang.org/x/net/trace"
)

var EmptySlice = []byte{}

func Trace(ctx context.Context, format string, args ...interface{}) {
	tr, ok := trace.FromContext(ctx)
	if !ok {
		return
	}
	tr.LazyPrintf(format, args...)
}

func OpenSyncedFile(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE|syscall.O_DSYNC, 0666)
}

type Closer struct {
	running int32
	nomore  int32
	closed  chan struct{}
	waiting chan struct{}
}

func NewCloser() *Closer {
	c := &Closer{
		closed:  make(chan struct{}, 10),
		waiting: make(chan struct{}),
	}
	return c
}

func (c *Closer) Register() {
	AssertTruef(atomic.LoadInt32(&c.nomore) == 0, "Can't register with closer after signal.")
	atomic.AddInt32(&c.running, 1)
}

func (c *Closer) HasBeenClosed() <-chan struct{} {
	return c.closed
}

func (c *Closer) Signal() {
	atomic.StoreInt32(&c.nomore, 1)
	running := int(atomic.LoadInt32(&c.running))
	fmt.Printf("Sending signal to %d registered\n", running)
	for i := 0; i < running; i++ {
		c.closed <- struct{}{}
	}
}

func (c *Closer) GotSignal() bool {
	return atomic.LoadInt32(&c.nomore) == 0
}

func (c *Closer) Done() {
	running := atomic.AddInt32(&c.running, -1)
	if running == 0 {
		c.waiting <- struct{}{}
	}
}

func (c *Closer) Wait() {
	<-c.waiting
}
