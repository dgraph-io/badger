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

	"github.com/dgraph-io/badger/v3/pb"
	"github.com/dgraph-io/badger/v3/trie"
	"github.com/dgraph-io/badger/v3/y"
	"github.com/dgraph-io/ristretto/z"
)

// so the problem is we have channels and mutex and both are locking in nature
type subscribers struct {
	sync.Mutex
	subs   map[uint64]*subscriber
	nextID uint64
}

func (s *subscribers) len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.subs)
}

func (s *subscribers) sendMessages(id uint64, kvs *pb.KVList) {
	s.Lock()
	defer s.Unlock()

	if sub, ok := s.subs[id]; ok && sub.active {
		sub.sendCh <- kvs
	}
}

func (s *subscribers) addNewSubscriber(c *z.Closer, matches []pb.Match) (uint64, <-chan *pb.KVList) {
	s.Lock()
	defer s.Unlock()

	id := s.nextID
	s.nextID++
	ch := make(chan *pb.KVList, 1000)
	s.subs[id] = &subscriber{
		matches:   matches,
		sendCh:    ch,
		subCloser: c,
	}

	return id, ch
}

func (s *subscribers) delete(id uint64) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.subs[id]; ok {
		delete(s.subs, id)
	}
}

func (s *subscribers) get(id uint64) *subscriber {
	s.Lock()
	defer s.Unlock()
	if sub, ok := s.subs[id]; ok {
		return sub
	}
	return nil
}

func (s *subscribers) clean() {
	s.Lock()
	defer s.Unlock()
	for id, sub := range s.subs {
		delete(s.subs, id)
		sub.subCloser.SignalAndWait()
	}
}

type subscriber struct {
	matches   []pb.Match
	sendCh    chan<- *pb.KVList
	subCloser *z.Closer
	active    bool
}

type publisher struct {
	sync.Mutex
	pubCh         chan requests
	subscribers   subscribers
	subMatcherMap map[uint64][]pb.Match
	inactiveSubs  map[uint64]struct{}
	indexer       *trie.Trie
}

func newPublisher() *publisher {
	return &publisher{
		pubCh: make(chan requests, 1000),
		subscribers: subscribers{
			Mutex: sync.Mutex{},
			subs:  make(map[uint64]*subscriber),
		},
		indexer:       trie.NewTrie(),
		subMatcherMap: make(map[uint64][]pb.Match),
	}
}

func (p *publisher) inactivateSubscription(id uint64) {
	p.Lock()
	defer p.Unlock()
	p.inactiveSubs[id] = struct{}{}
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
				// if its part of inactive subscription don't send anymore updates
				if _, ok := p.inactiveSubs[id]; ok {
					continue
				}
				if _, ok := batchedUpdates[id]; !ok {
					batchedUpdates[id] = &pb.KVList{}
				}
				batchedUpdates[id].Kv = append(batchedUpdates[id].Kv, kv)
			}
		}
	}

	// we don't need to lock to send updates to subscribers
	// subscribers have mutex and manage concurrency on their own
	p.Unlock()
	for id, kvs := range batchedUpdates {
		p.subscribers.sendMessages(id, kvs)
	}
}

func (p *publisher) newSubscriber(c *z.Closer, matches []pb.Match) (<-chan *pb.KVList, uint64) {
	id, ch := p.subscribers.addNewSubscriber(c, matches)
	p.Lock()
	defer p.Unlock()
	for _, m := range matches {
		p.indexer.AddMatch(m, id)
	}
	return ch, id
}

// cleanSubscribers stops all the subscribers. Ideally, It should be called while closing DB.
func (p *publisher) cleanSubscribers() {
	p.Lock()

	for subid, matches := range p.subMatcherMap {
		for _, m := range matches {
			p.indexer.DeleteMatch(m, subid)
		}
	}
	p.Unlock()
	p.subscribers.clean()
}

func (p *publisher) deleteSubscriber(id uint64) {
	p.Lock()
	get := p.subscribers.get(id)
	if get != nil {
		for _, m := range get.matches {
			p.indexer.DeleteMatch(m, id)
		}
	}
	p.Unlock()
	p.subscribers.delete(id)
}

func (p *publisher) sendUpdates(reqs requests) {
	if p.noOfSubscribers() != 0 {
		reqs.IncrRef()
		p.pubCh <- reqs
	}
}

func (p *publisher) noOfSubscribers() int {
	return p.subscribers.len()
}
