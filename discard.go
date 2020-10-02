/*
 * Copyright 2020 Dgraph Labs, Inc. and Contributors
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

package badger

import (
	"encoding/binary"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/dgraph-io/badger/v2/y"
	"github.com/dgraph-io/ristretto/z"
	"github.com/pkg/errors"
)

// discardStats keeps track of the amount of data that could be discarded for
// a given logfile.
type discardStats struct {
	sync.Mutex

	*z.MmapFile
	opt           Options
	nextEmptySlot int
}

const discardFname string = "vlog.discard"
const discardFsize int = 1 << 30
const maxSlot int = 64 << 20

func initDiscardStats(opt Options) (*discardStats, error) {
	fname := path.Join(opt.ValueDir, discardFname)

	// 1GB file can store 67M discard entries. Each entry is 16 bytes.
	mf, err := z.OpenMmapFile(fname, os.O_CREATE|os.O_RDWR, discardFsize)
	lf := &discardStats{
		MmapFile: mf,
		opt:      opt,
	}
	if err == z.NewFile {
		z.ZeroOut(mf.Data, 0, 1<<30)

	} else if err != nil {
		return nil, errors.Wrapf(err, "while opening file: %s\n", discardFname)
	}

	for slot := 0; slot < maxSlot; slot++ {
		if lf.get(16*slot) == 0 {
			lf.nextEmptySlot = slot
			break
		}
	}
	sort.Sort(lf)
	opt.Infof("Discard stats nextEmptySlot: %d\n", lf.nextEmptySlot)
	return lf, nil
}

func (lf *discardStats) Len() int {
	return lf.nextEmptySlot
}
func (lf *discardStats) Less(i, j int) bool {
	return lf.get(16*i) < lf.get(16*j)
}
func (lf *discardStats) Swap(i, j int) {
	left := lf.Data[16*i : 17*i]
	right := lf.Data[16*j : 17*j]
	var tmp [16]byte
	copy(tmp[:], left)
	copy(left, right)
	copy(right, tmp[:])
}

// offset is not slot.
func (lf *discardStats) get(offset int) uint64 {
	return binary.BigEndian.Uint64(lf.Data[offset : offset+8])
}
func (lf *discardStats) set(offset int, val uint64) {
	binary.BigEndian.PutUint64(lf.Data[offset:offset+8], val)
}

// Update would update the discard stats for the given file id. If discard is
// 0, it would return the current value of discard for the file. If discard is
// < 0, it would set the current value of discard to zero for the file.
func (lf *discardStats) Update(fidu uint32, discard int64) int64 {
	fid := uint64(fidu)
	lf.Lock()
	defer lf.Unlock()

	idx := sort.Search(lf.nextEmptySlot, func(slot int) bool {
		return lf.get(slot*16) >= fid
	})
	if idx < lf.nextEmptySlot && lf.get(idx*16) == fid {
		off := idx*16 + 8
		curDisc := lf.get(off)
		if discard == 0 {
			return int64(curDisc)
		}
		if discard < 0 {
			lf.set(off, 0)
			return 0
		}
		lf.set(off, curDisc+uint64(discard))
		return int64(curDisc + uint64(discard))
	}
	if discard <= 0 {
		// No need to add a new entry.
		return 0
	}

	// Could not find the fid. Add the entry.
	idx = lf.nextEmptySlot
	lf.set(idx*16, uint64(fid))
	lf.set(idx*16+8, uint64(discard))
	lf.nextEmptySlot++
	y.AssertTrue(lf.nextEmptySlot < maxSlot)

	sort.Sort(lf)
	return int64(discard)
}

// MaxDiscard returns the file id with maximum discard bytes.
func (lf *discardStats) MaxDiscard() (uint32, int64) {
	lf.Lock()
	defer lf.Unlock()

	var maxFid, maxVal uint64
	for slot := 0; slot < lf.nextEmptySlot; slot++ {
		if val := lf.get(slot*16 + 8); maxVal < val {
			maxVal = val
			maxFid = lf.get(slot * 16)
		}
	}
	return uint32(maxFid), int64(maxVal)
}
