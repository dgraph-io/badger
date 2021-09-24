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
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/trie"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
)

type subscriber struct {
	id uint64
	matches   []pb.Match
	sendCh    chan *pb.KVList
	subCloser *z.Closer
	// this will be atomic pointer which will be used to
	// track whether the subscriber is active or not
	active    *uint64
}

type publisher struct {
	sync.Mutex
	pubCh       chan requests
	subscribers map[uint64]subscriber
	nextID      uint64
	indexer     *trie.Trie
}

func newPublisher() *publisher {
	return &publisher{
		pubCh:       make(chan requests, 1000),
		subscribers: make(map[uint64]subscriber),
		nextID:      0,
		indexer:     trie.NewTrie(),
	}
}

func (p *publisher) listenForUpdates(c *z.Closer) {
	defer func() {
		p.cleanSubscribers()
		c.Done()
	}()
	slurp := func(batch requests) {
		for {
			select {
			case reqs := <-p.pubCh:
				batch = append(batch, reqs...)
			default:
				p.publishUpdates(batch)
				return
			}
		}
	}
	for {
		select {
		case <-c.HasBeenClosed():
			return
		case reqs := <-p.pubCh:
			slurp(reqs)
		}
	}
}

func (p *publisher) publishUpdates(reqs requests) {
	p.Lock()
	defer func() {
		p.Unlock()
		// Release all the request.
		reqs.DecrRef()
	}()
	batchedUpdates := make(map[uint64]*pb.KVList)
	for _, req := range reqs {
		for _, e := range req.Entries {
			ids := p.indexer.Get(e.Key)
			if len(ids) == 0 {
				continue
			}
			k := y.SafeCopy(nil, e.Key)
			kv := &pb.KV{
				Key:       y.ParseKey(k),
				Value:     y.SafeCopy(nil, e.Value),
				Meta:      []byte{e.UserMeta},
				ExpiresAt: e.ExpiresAt,
				Version:   y.ParseTs(k),
			}
			for id := range ids {
				if _, ok := batchedUpdates[id]; !ok {
					batchedUpdates[id] = &pb.KVList{}
				}
				batchedUpdates[id].Kv = append(batchedUpdates[id].Kv, kv)
			}
		}
	}

	for id, kvs := range batchedUpdates {
		if atomic.LoadUint64(p.subscribers[id].active) == 1 {
			p.subscribers[id].sendCh <- kvs
		}
	}
}

func (p *publisher) newSubscriber(c *z.Closer, matches []pb.Match) subscriber {
	p.Lock()
	defer p.Unlock()
	ch := make(chan *pb.KVList, 1000)
	id := p.nextID
	// Increment next ID.
	p.nextID++
	active := uint64(1)
	s := subscriber{
		active: &active,
		id: id,
		matches:   matches,
		sendCh:    ch,
		subCloser: c,
	}
	p.subscribers[id] = s
	for _, m := range matches {
		p.indexer.AddMatch(m, id)
	}
	return s
}

// cleanSubscribers stops all the subscribers. Ideally, It should be called while closing DB.
func (p *publisher) cleanSubscribers() {
	p.Lock()
	defer p.Unlock()
	for id, s := range p.subscribers {
		for _, m := range s.matches {
			p.indexer.DeleteMatch(m, id)
		}
		delete(p.subscribers, id)
		s.subCloser.SignalAndWait()
	}
}

func (p *publisher) deleteSubscriber(id uint64) {
	p.Lock()
	defer p.Unlock()
	if s, ok := p.subscribers[id]; ok {
		for _, m := range s.matches {
			p.indexer.DeleteMatch(m, id)
		}
	}
	delete(p.subscribers, id)
}

func (p *publisher) sendUpdates(reqs requests) {
	if p.noOfSubscribers() != 0 {
		reqs.IncrRef()
		p.pubCh <- reqs
	}
}

func (p *publisher) noOfSubscribers() int {
	p.Lock()
	defer p.Unlock()
	return len(p.subscribers)
}
