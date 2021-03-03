/*
 * Copyright (C) 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package y

import (
	"expvar"
)

var (
	// lsmSize has size of the LSM in bytes
	lsmSize *expvar.Map
	// vlogSize has size of the value log in bytes
	vlogSize *expvar.Map
	// pendingWrites tracks the number of pending writes.
	pendingWrites *expvar.Map

	// These are cumulative

	// numReads has cumulative number of reads
	numReads *expvar.Int
	// numWrites has cumulative number of writes
	numWrites *expvar.Int
	// numBytesRead has cumulative number of bytes read
	numBytesRead *expvar.Int
	// numBytesWritten has cumulative number of bytes written
	numBytesWritten *expvar.Int
	// numLSMGets is number of LMS gets
	numLSMGets *expvar.Map
	// numLSMBloomHits is number of LMS bloom hits
	numLSMBloomHits *expvar.Map
	// numGets is number of gets
	numGets *expvar.Int
	// numPuts is number of puts
	numPuts *expvar.Int
	// numBlockedPuts is number of blocked puts
	numBlockedPuts *expvar.Int
	// numMemtableGets is number of memtable gets
	numMemtableGets *expvar.Int
	// numCompactionTables is the number of tables being compacted
	numCompactionTables *expvar.Int
)

// These variables are global and have cumulative values for all kv stores.
func init() {
	numReads = expvar.NewInt("badger_v3_disk_reads_total")
	numWrites = expvar.NewInt("badger_v3_disk_writes_total")
	numBytesRead = expvar.NewInt("badger_v3_read_bytes")
	numBytesWritten = expvar.NewInt("badger_v3_written_bytes")
	numLSMGets = expvar.NewMap("badger_v3_lsm_level_gets_total")
	numLSMBloomHits = expvar.NewMap("badger_v3_lsm_bloom_hits_total")
	numGets = expvar.NewInt("badger_v3_gets_total")
	numPuts = expvar.NewInt("badger_v3_puts_total")
	numBlockedPuts = expvar.NewInt("badger_v3_blocked_puts_total")
	numMemtableGets = expvar.NewInt("badger_v3_memtable_gets_total")
	lsmSize = expvar.NewMap("badger_v3_lsm_size_bytes")
	vlogSize = expvar.NewMap("badger_v3_vlog_size_bytes")
	pendingWrites = expvar.NewMap("badger_v3_pending_writes_total")
	numCompactionTables = expvar.NewInt("badger_v3_compactions_current")
}

func NumReadsAdd(enabled bool, val int64) {
	addInt(enabled, numReads, val)
}

func NumWritesAdd(enabled bool, val int64) {
	addInt(enabled, numWrites, val)
}

func NumBytesReadAdd(enabled bool, val int64) {
	addInt(enabled, numBytesRead, val)
}

func NumBytesWrittenAdd(enabled bool, val int64) {
	addInt(enabled, numBytesWritten, val)
}

func NumGetsAdd(enabled bool, val int64) {
	addInt(enabled, numGets, val)
}

func NumPutsAdd(enabled bool, val int64) {
	addInt(enabled, numPuts, val)
}

func NumBlockedPutsAdd(enabled bool, val int64) {
	addInt(enabled, numBlockedPuts, val)
}

func NumMemtableGetsAdd(enabled bool, val int64) {
	addInt(enabled, numMemtableGets, val)
}

func NumCompactionTablesAdd(enabled bool, val int64) {
	addInt(enabled, numCompactionTables, val)
}

func LSMSizeSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, lsmSize, key, val)
}

func VlogSizeSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, vlogSize, key, val)
}

func PendingWritesSet(enabled bool, key string, val expvar.Var) {
	storeToMap(enabled, pendingWrites, key, val)
}

func NumLSMBloomHitsAdd(enabled bool, key string, val int64) {
	addToMap(enabled, numLSMBloomHits, key, val)
}

func NumLSMGetsAdd(enabled bool, key string, val int64) {
	addToMap(enabled, numLSMGets, key, val)
}

func LSMSizeGet(enabled bool, key string) expvar.Var {
	return getFromMap(enabled, lsmSize, key)
}

func VlogSizeGet(enabled bool, key string) expvar.Var {
	return getFromMap(enabled, vlogSize, key)
}

func addInt(enabled bool, metric *expvar.Int, val int64) {
	if !enabled {
		return
	}

	metric.Add(val)
}

func addToMap(enabled bool, metric *expvar.Map, key string, val int64) {
	if !enabled {
		return
	}

	metric.Add(key, val)
}

func storeToMap(enabled bool, metric *expvar.Map, key string, val expvar.Var) {
	if !enabled {
		return
	}

	metric.Set(key, val)
}

func getFromMap(enabled bool, metric *expvar.Map, key string) expvar.Var {
	if !enabled {
		return nil
	}

	return metric.Get(key)
}