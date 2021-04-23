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

package strie

import (
	"sort"

	"github.com/dgraph-io/badger/v3/y"
)

type node struct {
	v        byte
	children []*node
	maxId    int
	minId    int
}

func newNode(id int) *node {
	return &node{
		children: make([]*node, 0),
		maxId:    id,
		minId:    id,
	}
}

// Trie datastructure.
type Trie struct {
	root *node
}

// NewTrie returns Trie.
func NewTrie() *Trie {
	return &Trie{
		root: newNode(-1),
	}
}

// Add adds the id in the trie for the given prefix path.
func (t *Trie) Add(prefix []byte, id int) {
	node := t.root
	for _, val := range prefix {
		node.maxId = max(node.maxId, id)
		node.minId = min(node.minId, id)
		idx := node.search(val)
		if idx < len(node.children) && node.children[idx].v == val {
			// No need to create a new node.
			node = node.children[idx]
			continue
		}
		child := newNode(id)
		child.v = val
		node.children = append(node.children, child)
		node = child
	}
}

// Get returns the index for which the key[index] >= key.
func (t *Trie) Get(key []byte) int {
	node := t.root
	for i, val := range key {
		idx := node.search(val)
		if idx == len(node.children) {
			return node.maxId + 1
		}
		if node.children[idx].v != val {
			y.AssertTrue(node.children[idx].v > val)
			return node.children[idx].minId
		}
		node = node.children[idx]
		if i == len(key)-1 {
			return node.minId
		}
	}
	return 0
}

func (n *node) search(val byte) int {
	return sort.Search(len(n.children), func(i int) bool {
		return n.children[i].v >= val
	})
	// for i, child := range n.children {
	// 	if child.v >= val {
	// 		return i
	// 	}
	// }
	// return len(n.children)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
