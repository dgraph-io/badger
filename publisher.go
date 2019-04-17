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
	subscribers map[string]chan<- *pb.KV
	sync.RWMutex
}

type callback func(kv *pb.KV)

func newPublisher() *publisher {
	return &publisher{
		subscribers: make(map[string]chan<- *pb.KV),
	}
}

func (p *publisher) addSubscriber(prefix string, cb callback) *y.Closer {
	p.Lock()
	defer p.Unlock()

	c := y.NewCloser(1)
	listen := func(updateRecv chan *pb.KV) {
		defer c.Done()
		cbCh := make(chan []*pb.KV)
		cbRunner := func() {
			for kvs := range cbCh {
				for _, kv := range kvs {
					cb(kv)
				}
			}
		}
		go cbRunner()

		var kvs []*pb.KV
		trySending := func() {
			select {
			case cbCh <- kvs:
				kvs = []*pb.KV{}
			}
		}

	smartbatch:
		for {
			select {
			case kv := <-updateRecv:
				kvs = append(kvs, kv)
				trySending()
			case <-c.HasBeenClosed():
				for {
					// we need to drain the channel before deleting subscriber
					// because after closing signal, publishUpdates may be invoked
					// and it'll try to push the update but no one is listening here so lock is never released.
					// That'll lead to deadlock when we try to aquire lock for deleting subscriber
					select {
					case kv := <-updateRecv:
						kvs = append(kvs, kv)
					default:
						// delete the subscribers to avoid further updates
						p.deleteSubscriber(prefix)
						if len(kvs) > 0 {
							// send pending updates
							cbCh <- kvs
						}
						// stop the callback runner
						close(cbCh)
						break smartbatch
					}
				}
			}
		}
	}
	updateCh := make(chan *pb.KV)
	p.subscribers[prefix] = updateCh
	go listen(updateCh)
	return c // do we have to close the subscribers while closing db?
}

func (p *publisher) publishUpdates(reqs []*request) {
	p.Lock()
	defer p.Unlock()
	for _, req := range reqs {

		for _, e := range req.Entries {

			for prefix, sCh := range p.subscribers {

				// send update to the subscriber if prefix matches
				if strings.HasPrefix(string(e.Key), prefix) {
					kv := pb.KV{}
					k := y.SafeCopy(nil, e.Key)
					kv.Key = y.ParseKey(k)
					kv.Value = y.SafeCopy(nil, e.Value)
					kv.Meta = []byte{e.UserMeta}
					kv.ExpiresAt = e.ExpiresAt
					kv.Version = y.ParseTs(k)
					sCh <- &kv
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
