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
	"testing"

	"github.com/stretchr/testify/require"
)

func b(s string) []byte {
	return []byte(s)
}

func TestGet(t *testing.T) {
	trie := NewTrie()
	keys := []string{"a", "aa", "ad", "adb", "adf", "adg", "ae", "b", "c"}
	for i, key := range keys {
		trie.Put(b(key), uint64(i))
	}

	require.Equal(t, 0, trie.Get(b("a")))
	require.Equal(t, 1, trie.Get(b("aa")))
	require.Equal(t, 2, trie.Get(b("ad")))
	require.Equal(t, 3, trie.Get(b("adb")))
	require.Equal(t, 4, trie.Get(b("adf")))
	require.Equal(t, 5, trie.Get(b("adg")))
	require.Equal(t, 6, trie.Get(b("ae")))
	require.Equal(t, 7, trie.Get(b("b")))
	require.Equal(t, 8, trie.Get(b("c")))

	require.Equal(t, 0, trie.Get(b("")))
	require.Equal(t, 2, trie.Get(b("aab")))
	require.Equal(t, 2, trie.Get(b("ac")))
	require.Equal(t, 3, trie.Get(b("ada")))
	require.Equal(t, 4, trie.Get(b("adbx")))
	require.Equal(t, 4, trie.Get(b("ade")))
	require.Equal(t, 8, trie.Get(b("bab")))
	require.Equal(t, 9, trie.Get(b("ddd")))
}

func TestEmptyTrie(t *testing.T) {
	trie := NewTrie()
	require.Equal(t, 0, trie.Get(b("abcd")))
}
