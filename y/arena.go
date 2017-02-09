package y

// Very simple arena memory allocation. You can only add stuff.

const arenaBlockSize = 4096

type Arena struct {
	memUsage   int // Atomic.
	blocks     [][]byte
	allocSlice []byte
}

// Allocate returns a free slice of memory.
func (s *Arena) Allocate(n int) []byte {
	AssertTrue(n > 0)
	if n > len(s.allocSlice) {
		return s.allocateFallback(n)
	}
	out := s.allocSlice[:n]
	s.allocSlice = s.allocSlice[n:]
	return out
}

func (s *Arena) allocateFallback(n int) []byte {
	if n > arenaBlockSize/4 {
		// Object is more than a quarter of our block size.  Allocate it separately
		// to avoid wasting too much space in leftover bytes.
		return s.allocateNewBlock(n)
	}
	block := s.allocateNewBlock(arenaBlockSize)
	s.blocks = append(s.blocks, block)
	out := block[:n]
	s.allocSlice = block[n:]
	return out
}

func (s *Arena) allocateNewBlock(n int) []byte {
	out := make([]byte, n)
	s.memUsage += n
	return out
}

func (s *Arena) MemUsage() int { return s.memUsage }
