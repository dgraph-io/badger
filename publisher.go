/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
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
package badger

import (
	"strings"
	"sync"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

type publisher struct {
	sync.RWMutex
	subscribers map[string]chan<- *pb.KV
	batchSize   int
	opts        Options
}

type callback func(kv *pb.KV)

func newPublisher(opts Options) *publisher {
	return &publisher{
		subscribers: make(map[string]chan<- *pb.KV),
		batchSize:   opts.MaxPendingUpdates,
		opts:        opts,
	}
}

// runSubscriber spins two go rotuine, one for processing callback and another for batching the incomming
// updates. closer will first stop further incoming updates and wait for all the updates to get consumed
// by the subscriber's callback
// do we have to close the subscribers while closing db? or upto user to deal?
func (p *publisher) runSubscriber(prefix string, cb callback) *y.Closer {
	p.Lock()
	defer p.Unlock()

	c := y.NewCloser(2)
	listen := func(updateRecv chan *pb.KV) {
		defer c.Done()
		cbRunner := func() {
			// listen for incomming updates
			for kv := range updateRecv {
				cb(kv)
			}
			c.Done()
		}
		go cbRunner()
		<-c.HasBeenClosed()
		// delete the subscriber and close the channel
		p.deleteSubscriber(prefix)
		close(updateRecv)
	}

	updateCh := make(chan *pb.KV, p.batchSize)
	p.subscribers[prefix] = updateCh
	go listen(updateCh)
	return c
}

// publishUpdates send update to the listening subscriber
func (p *publisher) publishUpdates(reqs []*request) {
	p.Lock()
	defer p.Unlock()
	for _, req := range reqs {

		for _, e := range req.Entries {

			for prefix, sCh := range p.subscribers {

				// send update to the subscriber if prefix matches
				if strings.HasPrefix(string(e.Key), prefix) {
					k := y.SafeCopy(nil, e.Key)
					kv := &pb.KV{
						Key:       y.ParseKey(k),
						Value:     y.SafeCopy(nil, e.Value),
						Meta:      []byte{e.UserMeta},
						ExpiresAt: e.ExpiresAt,
						Version:   y.ParseTs(k),
					}
					select {
					case sCh <- kv:
					default:
						//buffer overflowed
					}
				}
			}
		}
	}
}

func (p *publisher) deleteSubscriber(prefix string) {
	p.Lock()
	defer p.Unlock()
	delete(p.subscribers, prefix)
}
