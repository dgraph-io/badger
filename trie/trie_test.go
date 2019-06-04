package trie

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	trie := NewTrie()
	trie.Add([]byte("hello"), 1)
	trie.Add([]byte("hello"), 3)
	trie.Add([]byte("hello"), 4)
	trie.Add([]byte("hel"), 20)
	trie.Add([]byte("badger"), 30)
	ids := trie.Get([]byte("hel"))
	if len(ids) != 1 {
		t.Errorf("expected number of ids 1 but got %d", len(ids))
		return
	}
	require.Equal(t, ids[0], uint64(20))
	ids = trie.Get([]byte("badger"))
	if len(ids) != 1 {
		t.Errorf("expected number of ids 1 but got %d", len(ids))
		return
	}
	require.Equal(t, ids[0], uint64(30))
	ids = trie.Get([]byte("hello"))
	if len(ids) != 4 {
		t.Errorf("expected number of ids 1 but got %d", len(ids))
		return
	}
	require.ElementsMatch(t, ids, []uint64{1, 3, 4, 20})
}

func TestTrieDelete(t *testing.T) {
	trie := NewTrie()
	trie.Add([]byte("hello"), 1)
	trie.Add([]byte("hello"), 3)
	trie.Add([]byte("hello"), 4)
	trie.Delete([]byte("hello"), 4)
	require.ElementsMatch(t, trie.Get([]byte("hello")), []uint64{1, 3})
}
