// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph.io/badger/v3/internal/base"
	"github.com/dgraph.io/badger/v3/internal/datadriven"
)

func TestLevelIterator(t *testing.T) {
	var level LevelSlice
	datadriven.RunTest(t, "testdata/level_iterator",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				var files []*FileMetadata
				var startReslice int
				var endReslice int
				for _, metaStr := range strings.Split(d.Input, " ") {
					switch metaStr {
					case "[":
						startReslice = len(files)
						continue
					case "]":
						endReslice = len(files)
						continue
					case " ", "":
						continue
					default:
						parts := strings.Split(metaStr, "-")
						if len(parts) != 2 {
							t.Fatalf("malformed table spec: %q", metaStr)
						}
						m := &FileMetadata{
							FileNum:  base.FileNum(len(files) + 1),
							Smallest: base.ParseInternalKey(strings.TrimSpace(parts[0])),
							Largest:  base.ParseInternalKey(strings.TrimSpace(parts[1])),
						}
						m.SmallestSeqNum = m.Smallest.SeqNum()
						m.LargestSeqNum = m.Largest.SeqNum()
						files = append(files, m)
					}
				}
				level = NewLevelSliceKeySorted(base.DefaultComparer.Compare, files)
				level = level.Reslice(func(start, end *LevelIterator) {
					for i := 0; i < startReslice; i++ {
						start.Next()
					}
					for i := len(files); i > endReslice; i-- {
						end.Prev()
					}
				})
				return ""

			case "iter":
				iter := level.Iter()
				var buf bytes.Buffer
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					if len(parts) == 0 {
						continue
					}
					var m *FileMetadata
					switch parts[0] {
					case "first":
						m = iter.First()
					case "last":
						m = iter.Last()
					case "next":
						m = iter.Next()
					case "prev":
						m = iter.Prev()
					case "seek-ge":
						m = iter.SeekGE(base.DefaultComparer.Compare, []byte(parts[1]))
					case "seek-lt":
						m = iter.SeekLT(base.DefaultComparer.Compare, []byte(parts[1]))
					default:
						return fmt.Sprintf("unknown command %q", parts[0])
					}
					if m == nil {
						fmt.Fprintln(&buf, ".")
					} else {
						fmt.Fprintln(&buf, m)
					}
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command %q", d.Cmd)
			}
		})
}
