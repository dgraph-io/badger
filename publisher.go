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
type subscriber struct {
	sync.Mutex
	subs   map[uint64]*subscription
	nextID uint64
}

type subscription struct {
	id        uint64
	sendCh    chan *pb.KVList
	subCloser *z.Closer
}

func (s *subscriber) len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.subs)
}

func (s *subscriber) sendMessages(id uint64, kvs *pb.KVList) {
	s.Lock()
	defer s.Unlock()

	if sub, ok := s.subs[id]; ok {
		sub.sendCh <- kvs
	}
}

func (s *subscriber) addSubscription(c *z.Closer) (uint64, <-chan *pb.KVList) {
	s.Lock()
	defer s.Unlock()

	id := s.nextID
	s.nextID++
	ch := make(chan *pb.KVList, 1000)
	s.subs[id] = &subscription{
		id:        id,
		sendCh:    ch,
		subCloser: c,
	}
	return id, ch
}

func (s *subscriber) delete(id uint64) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.subs[id]; ok {
		delete(s.subs, id)
	}
}

func (s *subscriber) clean() {
	s.Lock()
	defer s.Unlock()
	for id, sub := range s.subs {
		delete(s.subs, id)
		sub.subCloser.SignalAndWait()
	}
}

type publisher struct {
	sync.Mutex
	subscribers *subscriber
	pubCh       chan requests
	matchersMap map[uint64][]pb.Match
	activeSubs  map[uint64]struct{}
	indexer     *trie.Trie
}

func newPublisher(sub *subscriber) *publisher {
	return &publisher{
		subscribers: sub,
		pubCh:       make(chan requests, 1000),
		indexer:     trie.NewTrie(),
		matchersMap: make(map[uint64][]pb.Match),
		activeSubs:  make(map[uint64]struct{}),
	}
}

// will mark the subscriber inactivate then then delete the matchers map and everything
func (p *publisher) inactivateSubscriber(id uint64) {
	p.Lock()
	defer p.Unlock()
	delete(p.activeSubs, id)
	if matches, ok := p.matchersMap[id]; ok {
		for _, m := range matches {
			p.indexer.DeleteMatch(m, id)
		}
	}
	delete(p.matchersMap, id)
}

func (p *publisher) listenForUpdates(c *z.Closer) {
	defer func() {
		p.cleanPublisher()
		p.subscribers.clean()
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
				if _, ok := p.activeSubs[id]; !ok {
					continue
				}
				if _, ok := batchedUpdates[id]; !ok {
					batchedUpdates[id] = &pb.KVList{}
				}
				batchedUpdates[id].Kv = append(batchedUpdates[id].Kv, kv)
			}
		}
	}

	 p.Unlock()
	// we don't need to lock to send updates to subscribers
	// subscribers have mutex and manage concurrency on their own
	for id, kvs := range batchedUpdates {
		p.subscribers.sendMessages(id, kvs)
	}
}

func (p *publisher) activateSubscriber(id uint64, matches []pb.Match) {
	p.Lock()
	defer p.Unlock()
	for _, m := range matches {
		p.indexer.AddMatch(m, id)
	}
	p.activeSubs[id] = struct{}{}
	p.matchersMap[id] = matches
}

// cleanSubscribers stops all the subscribers. Ideally, It should be called while closing DB.
func (p *publisher) cleanPublisher() {
	p.Lock()
	defer p.Unlock()
	for id, matches := range p.matchersMap {
		for _, m := range matches {
			p.indexer.DeleteMatch(m, id)
		}
		delete(p.activeSubs, id)
	}
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
	return len(p.activeSubs)
}
