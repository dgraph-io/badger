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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/trace"
)

var (
	// These are cumulative
	NumReads        *expvar.Int
	NumWrites       *expvar.Int
	NumBytesRead    *expvar.Int
	NumBytesWritten *expvar.Int
	NumLSMGets      *expvar.Map
	NumLSMBloomHits *expvar.Map
)

type Metrics struct {
	NumGets         *expvar.Int
	NumPuts         *expvar.Int
	NumMemtableGets *expvar.Int
	Ticker          *time.Ticker

	lsmSize      *expvar.Int
	valueLogSize *expvar.Int
	dir          string
	valueDir     string
	elog         trace.EventLog
}

func (m *Metrics) totalSize(dir string, extension string) int64 {
	var size int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == extension {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		m.elog.Printf("Got error while calculating total size of directory: %s with ext: %s",
			dir, extension)
	}
	return size
}

func (m *Metrics) updateSize() {
	for range m.Ticker.C {
		m.lsmSize.Set(m.totalSize(m.dir, ".sst"))
		m.valueLogSize.Set(m.totalSize(m.valueDir, ".vlog"))
	}
}

// these variables are global and would have cummulative values for all kv stores.
func init() {
	NumReads = expvar.NewInt("badger_disk_reads_total")
	NumWrites = expvar.NewInt("badger_disk_writes_total")
	NumBytesRead = expvar.NewInt("badger_read_bytes")
	NumBytesWritten = expvar.NewInt("badger_written_bytes")
	NumLSMGets = expvar.NewMap("badger_lsm_level_gets_total")
	NumLSMBloomHits = expvar.NewMap("badger_lsm_bloom_hits_total")
}

func Init(elog trace.EventLog, dir, valueDir string) Metrics {
	var m Metrics
	m.NumGets = expvar.NewInt(fmt.Sprintf("badger_gets_total_%s", dir))
	m.NumPuts = expvar.NewInt(fmt.Sprintf("badger_puts_total_%s", dir))
	m.NumMemtableGets = expvar.NewInt(fmt.Sprintf("badger_memtable_gets_total_%s", dir))

	m.lsmSize = expvar.NewInt(fmt.Sprintf("badger_lsm_size_%s", dir))
	m.valueLogSize = expvar.NewInt(fmt.Sprintf("badger_value_log_size_%s", valueDir))
	m.elog = elog
	m.dir = dir
	m.valueDir = valueDir
	m.Ticker = time.NewTicker(5 * time.Second)

	go m.updateSize()
	return m
}
