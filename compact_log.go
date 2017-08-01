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

package badger

// Might consider moving this into a separate package.

import (
	"sync/atomic"
)

// compaction is our compaction in a easily serializable form.
type compaction struct {
	compactID uint64
	done      byte
	toDelete  []uint64
	toInsert  []uint64
}

// TODO: Remove this?
func (s *levelsController) buildCompactionLogEntry(def *compactDef) *compaction {
	var newIDMin, newIDMax uint64
	c := new(compaction)
	c.compactID = atomic.AddUint64(&s.maxCompactID, 1)

	var estSize int64
	for _, t := range def.top {
		c.toDelete = append(c.toDelete, t.ID())
		estSize += t.Size()
	}
	for _, t := range def.bot {
		c.toDelete = append(c.toDelete, t.ID())
		estSize += t.Size()
	}
	estNumTables := 1 + (estSize+s.kv.opt.MaxTableSize-1)/s.kv.opt.MaxTableSize
	newIDMin, newIDMax = s.reserveFileIDs(int(estNumTables))
	// TODO: Consider storing just two numbers for toInsert.
	for i := newIDMin; i < newIDMax; i++ {
		c.toInsert = append(c.toInsert, uint64(i))
	}

	return c
}
