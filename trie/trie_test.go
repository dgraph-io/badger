package trie_test

import (
	"github.com/dgraph-io/badger/trie"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTrie(t *testing.T) {
	oneTrie := trie.New()
	oneTrie.Add([]byte("f"), 1)
	oneTrie.Add([]byte("foo"), 2)
	oneTrie.Add([]byte("foob"), 3)

	nodes := oneTrie.FindMatchingNodes([]byte("foobar"))
	require.Equal(t, 3, len(nodes), "the should be 3 nodes in the result")
	require.Equal(t, 1, nodes[0].Meta(), "the first node should have meta value 1")
	require.Equal(t, 2, nodes[1].Meta(), "the second node should have meta value 2")
	require.Equal(t, 3, nodes[2].Meta(), "the third node should have meta value 3")
	node1, ok1 := oneTrie.Find([]byte("f"))
	require.True(t, ok1, "the first node should be returned when finding with the key f")
	require.Equal(t, 1,  node1.Meta(), "the first node should have meta value 1")

	_, ok2 := oneTrie.Find([]byte("fo"))
	require.False(t, ok2, "no node should be found when search with the key fo")
}
