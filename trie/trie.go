package trie

type Node struct {
	val byte
	meta interface{}
	term bool // whether the node represents a terminated prefix
	depth int // depth starts at 0
	children map[byte]*Node
}

func (n *Node) NewChild(val byte, meta interface{}, term bool) *Node {
	node := &Node {
		val : val,
		meta : meta,
		term: term,
		children: make(map[byte]*Node),
		depth: n.depth + 1,
	}
	n.children[val] = node
	return node
}

func (n *Node) SetMeta(meta interface{}) {
	n.meta = meta
}

func (n *Node) Meta() interface{} {
	return n.meta
}

type Trie struct {
	root *Node
}

func New() *Trie {
	return &Trie{
		root: &Node{children: make(map[byte]*Node)},
	}
}

const nul = 0x0

func (t *Trie) Add(key []byte, meta interface{}) *Node {
	node := t.root
	for _, k := range key {
		if n, ok := node.children[k]; ok {
			node = n
		} else {
			node = node.NewChild(k, nil, false)
		}
	}
	// we've arrived at a terminal node, store the meta here
	node.term = true
	node.meta = meta
	return node
}

type collectorFunc func(node *Node)

func (t *Trie) findNode(key []byte, collector collectorFunc) {
	node := t.root
	missing := false
	for _, k := range key {
		collector(node)
		if n, ok := node.children[k]; ok {
			node = n
		} else {
			missing = true
			break
		}
	}

	if !missing {
		collector(node)
	}
}

func (t *Trie) Find(key []byte) (*Node, bool) {
	var matchedNode *Node
	t.findNode(key, func(node *Node) {
		if node.term && node.depth == len(key) {
			matchedNode = node
		}
	})
	return matchedNode, matchedNode != nil
}

func (t *Trie) FindMatchingNodes(key []byte) []*Node {
	var nodes []*Node
	t.findNode(key, func(node *Node) {
		if node.term {
			nodes = append(nodes, node)
		}
	})
	return nodes
}
