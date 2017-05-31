package badger

import (
	"bytes"
	"sync"

	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
)

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
}

var infRange = keyRange{inf: true}

func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.inf || dst.inf {
		return true
	}

	// If my left is greater than dst right, we have no overlap.
	if bytes.Compare(r.left, dst.right) > 0 {
		return false
	}
	// If my right is less than dst left, we have no overlap.
	if bytes.Compare(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}

func getKeyRange(tables []*table.Table) keyRange {
	y.AssertTrue(len(tables) > 0)
	smallest := tables[0].Smallest()
	biggest := tables[0].Biggest()
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i].Smallest(), smallest) < 0 {
			smallest = tables[i].Smallest()
		}
		if bytes.Compare(tables[i].Biggest(), biggest) > 0 {
			biggest = tables[i].Biggest()
		}
	}
	return keyRange{left: smallest, right: biggest}
}

type compactStatus struct {
	sync.Mutex
	ranges []keyRange
	// delSize int64 // TODO: Implement this.
}

func (cs *compactStatus) compareAndAdd(dst keyRange) bool {
	cs.Lock()
	defer cs.Unlock()

	for _, r := range cs.ranges {
		if r.overlapsWith(dst) {
			return false
		}
	}
	cs.ranges = append(cs.ranges, dst)
	return true
}

func (cs *compactStatus) delete(dst keyRange) {
	cs.Lock()
	defer cs.Unlock()

	var found bool
	final := cs.ranges[:0]
	for _, r := range cs.ranges {
		if bytes.Compare(r.left, dst.left) == 0 &&
			bytes.Compare(r.right, dst.right) == 0 &&
			r.inf == dst.inf {

			found = true
		} else {
			final = append(final, r)
		}
	}
	cs.ranges = final
	y.AssertTruef(found, "keyRange not found in compactStatus: %+v", dst)
}
