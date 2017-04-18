package badger

//import (
//	"sync"

//	"github.com/dgraph-io/badger/skl"
//)

//type arenaList struct {
//	sync.Mutex // Guards used.

//	used []bool

//	arenas []*skl.Arena // Immutable.
//}

//func newArenaList(size int64, numArenas int) *arenaList {
//	out := &arenaList{
//		arenas: make([]*skl.Arena, numArenas),
//		used:   make([]bool, numArenas),
//	}
//	for i := 0; i < numArenas; i++ {
//		out.arenas[i] = skl.NewArena(size)
//	}
//	return out
//}

//func (s *arenaList) getUnusedArena() *skl.Arena {
//	s.Lock()
//	defer s.Unlock()
//	for i, arena := range s.arenas {
//		if !s.used[i] {
//			s.used[i] = true
//			return arena
//		}
//	}
//	return nil
//}

//func (s *arenaList) returnArena(arena *skl.Arena) bool {
//	// Do a simple loop. No need to do any hashing. There are very few arenas.
//	s.Lock()
//	defer s.Unlock()
//	for i, a := range s.arenas {
//		if arena == a {
//			s.used[i] = false
//			return true
//		}
//	}
//	return false
//}
