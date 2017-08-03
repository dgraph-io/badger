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

import (
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/net/trace"
)

type metrics struct {
	NumGets         *expvar.Int
	NumPuts         *expvar.Int
	NumMemtableGets *expvar.Int
	Ticker          *time.Ticker

	lsmSize  *expvar.Int
	vlogSize *expvar.Int
	dir      string
	valueDir string
	elog     trace.EventLog
}

func (m *metrics) totalSize(dir string) (int64, int64) {
	var lsmSize, vlogSize int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ext := filepath.Ext(path)
		if ext == ".sst" {
			lsmSize += info.Size()
		} else if ext == ".vlog" {
			vlogSize += info.Size()
		}
		return nil
	})
	if err != nil {
		m.elog.Printf("Got error while calculating total size of directory: %s", dir)
	}
	return lsmSize, vlogSize
}

func (m *metrics) updateSize() {
	for range m.Ticker.C {
		lsmSize, vlogSize := m.totalSize(m.dir)
		m.lsmSize.Set(lsmSize)
		// If valueDir is different from dir, we'd have to do another walk.
		if m.valueDir != m.dir {
			_, vlogSize = m.totalSize(m.valueDir)
		}
		m.vlogSize.Set(vlogSize)
	}
}

// expvar panics if you try to set an already set variable. So we try get first else get new.
func getInt(k string) *expvar.Int {
	if val := expvar.Get(k); val != nil {
		return val.(*expvar.Int)
	}
	return expvar.NewInt(k)
}

func newMetrics(elog trace.EventLog, dir, valueDir string) *metrics {
	m := new(metrics)
	m.NumGets = getInt(fmt.Sprintf("badger_%s_gets_total", dir))
	m.NumPuts = getInt(fmt.Sprintf("badger_%s_puts_total", dir))
	m.NumMemtableGets = getInt(fmt.Sprintf("badger_%s_memtable_gets_total", dir))

	m.lsmSize = getInt(fmt.Sprintf("badger_%s_lsm_size", dir))
	m.vlogSize = getInt(fmt.Sprintf("badger_%s_value_log_size", valueDir))
	m.elog = elog
	m.dir = dir
	m.valueDir = valueDir
	m.Ticker = time.NewTicker(5 * time.Minute)

	go m.updateSize()
	return m
}
