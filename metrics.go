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
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/y"

	"golang.org/x/net/trace"
)

type metrics struct {
	ticker *time.Ticker

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

func getNewInt(val int64) *expvar.Int {
	v := new(expvar.Int)
	v.Add(val)
	return v
}

func (m *metrics) updateSize() {
	for range m.ticker.C {
		lsmSize, vlogSize := m.totalSize(m.dir)
		y.LSMSize.Set(m.dir, getNewInt(lsmSize))
		// If valueDir is different from dir, we'd have to do another walk.
		if m.valueDir != m.dir {
			_, vlogSize = m.totalSize(m.valueDir)
		}
		y.VlogSize.Set(m.dir, getNewInt(vlogSize))
	}
}

func newMetrics(elog trace.EventLog, dir, valueDir string) *metrics {
	m := new(metrics)
	m.elog = elog
	m.dir = dir
	m.valueDir = valueDir
	m.ticker = time.NewTicker(5 * time.Minute)
	go m.updateSize()
	return m
}
