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
	"bytes"
	"sync"

	"github.com/dgraph-io/badger/pb"
	"github.com/dgraph-io/badger/y"
)

type subscriber struct {
	prefixes  [][]byte
	sendCh    chan<- *pb.KVList
	subCloser *y.Closer
}

type publisher struct {
	sync.Mutex
	pubCh       chan requests
	subscribers map[uint64]subscriber
	nextID      uint64
}

func newPublisher() *publisher {
	return &publisher{
		pubCh:       make(chan requests, 1000000),
		subscribers: make(map[uint64]subscriber),
		nextID:      0,
	}
}

func (p *publisher) listenForUpdates(c *y.Closer) {
	defer func() {
		p.cleanSubscribers()
		c.Done()
	}()
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case reqs := <-p.pubCh:
			p.publishUpdates(reqs)
		}
	}
}

func (p *publisher) publishUpdates(reqs requests) {
	p.Lock()
	defer func() {
		p.Unlock()
		// release all the request
		reqs.DecrRef()
	}()
	for _, s := range p.subscribers {
		kvs := &pb.KVList{}
		for _, prefix := range s.prefixes {
			for _, req := range reqs {
				for _, e := range req.Entries {
					//TODO: use trie to find subscribers
					if bytes.HasPrefix(e.Key, prefix) {
						k := y.SafeCopy(nil, e.Key)
						kv := &pb.KV{
							Key:       y.ParseKey(k),
							Value:     y.SafeCopy(nil, e.Value),
							Meta:      []byte{e.UserMeta},
							ExpiresAt: e.ExpiresAt,
							Version:   y.ParseTs(k),
						}
						kvs.Kv = append(kvs.Kv, kv)
					}
				}
			}
		}
		if len(kvs.GetKv()) > 0 {
			select {
			case s.sendCh <- kvs:
			default:
			}
		}
	}
}

func (p *publisher) newSubscriber(c *y.Closer, prefixes ...[]byte) (<-chan *pb.KVList, uint64) {
	p.Lock()
	defer p.Unlock()
	ch := make(chan *pb.KVList, 1000)
	id := p.nextID
	// increment next ID
	p.nextID++
	p.subscribers[id] = subscriber{
		prefixes:  prefixes,
		sendCh:    ch,
		subCloser: c,
	}
	return ch, id
}

// cleanSubscribers stops all the subscribers. Ideally, It should be called while closing DB
func (p *publisher) cleanSubscribers() {
	p.Lock()
	defer p.Unlock()
	for id, s := range p.subscribers {
		delete(p.subscribers, id)
		s.subCloser.SignalAndWait()
	}
}

func (p *publisher) deleteSubscriber(id uint64) {
	p.Lock()
	defer p.Unlock()
	_, ok := p.subscribers[id]
	if !ok {
		return
	}
	delete(p.subscribers, id)
}

func (p *publisher) sendUpdates(reqs []*request) {
	select {
	case p.pubCh <- reqs:
	default:
	}
}
