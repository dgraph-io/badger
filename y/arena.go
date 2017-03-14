/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
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
