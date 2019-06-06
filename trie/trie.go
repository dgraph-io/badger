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

package trie

import "sort"

type node struct {
	children map[byte]*node
	ids      []uint64
}

func newNode() *node {
	return &node{
		children: make(map[byte]*node),
		ids:      []uint64{},
	}
}

// Trie datastructure.
type Trie struct {
	root *node
}

// NewTrie returns Trie.
func NewTrie() *Trie {
	return &Trie{
		root: newNode(),
	}
}

// Add adds the id in the trie for the given prefix path.
func (t *Trie) Add(prefix []byte, id uint64) {
	curr := t.root
	for _, val := range prefix {
		n, ok := curr.children[val]
		if !ok {
			n = newNode()
			curr.children[val] = n
		}
		curr = n
	}
	curr.ids = append(curr.ids, id)
}

// Get returns prefix matched ids for the given key.
func (t *Trie) Get(key []byte) []uint64 {
	out := []uint64{}
	curr := t.root
	for _, val := range key {
		n, ok := curr.children[val]
		if !ok {
			break
		}
		out = append(out, n.ids...)
		curr = n
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	res := []uint64{}
	for i := 0; i < len(out); i++ {
		if len(res) != 0 && res[len(res)-1] == out[i] {
			continue
		}
		res = append(res, out[i])
	}
	return res
}

// Delete will delete the id if the id exist in the given index path.
func (t *Trie) Delete(index []byte, id uint64) {
	curr := t.root
	for _, val := range index {
		n, ok := curr.children[val]
		if !ok {
			return
		}
		curr = n
	}
	//NOTE: We're just removing the id not the hanging path.
	old := curr.ids
	out := []uint64{}
	for _, val := range old {
		if val != id {
			out = append(out, val)
		}
	}
	curr.ids = out
}
