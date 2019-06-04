package trie

type node struct {
	childrens map[byte]*node
	items     []uint64
}

func newNode() *node {
	return &node{
		childrens: make(map[byte]*node),
		items:     []uint64{},
	}
}

// Trie datastructure
type Trie struct {
	root *node
}

// NewTrie returns trie
func NewTrie() *Trie {
	return &Trie{
		root: newNode(),
	}
}

// Add adds the item in the trie for the given prefix path
func (t *Trie) Add(prefix []byte, item uint64) {
	curr := t.root
	for _, val := range prefix {
		n, ok := curr.childrens[val]
		if !ok {
			n = newNode()
			curr.childrens[val] = n
		}
		curr = n
	}
	curr.items = append(curr.items, item)
}

// Get returns prefix matched items for the given key
func (t *Trie) Get(key []byte) []uint64 {
	o := []uint64{}
	curr := t.root
	for _, val := range key {
		n, ok := curr.childrens[val]
		if !ok {
			return o
		}
		o = append(o, n.items...)
		curr = n
	}
	return o
}

// Delete will delete the item if the item exist in the given index path
func (t *Trie) Delete(index []byte, item uint64) {
	curr := t.root
	for _, val := range index {
		n, ok := curr.childrens[val]
		if !ok {
			return
		}
		curr = n
	}
	//NOTE: we're just removing the item not the hanging path
	old := curr.items
	new := []uint64{}
	for _, val := range old {
		if val != item {
			new = append(new, val)
		}
	}
	curr.items = new
}
