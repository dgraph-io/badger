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

package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Health info about Badger database.",
	Long: `
This command prints information about the badger key-value store.  It reads MANIFEST and prints its
info. It also prints info about missing/extra files, and general information about the value log
files (which are not referenced by the manifest).  Use this tool to report any issues about Badger
to the Dgraph team.
`,
	Run: func(cmd *cobra.Command, args []string) {
		err := printInfo(sstDir, vlogDir)
		if err != nil {
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
		err = tableInfo(sstDir, vlogDir)
		if err != nil {
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(infoCmd)
}

func bytes(sz int64) string {
	return humanize.Bytes(uint64(sz))
}

func dur(src, dst time.Time) string {
	return humanize.RelTime(dst, src, "earlier", "later")
}

func tableInfo(dir, valueDir string) error {
	// Open DB
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.ReadOnly = true

	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	tables := db.Tables()
	for _, t := range tables {
		lk, lv := y.ParseKey(t.Left), y.ParseTs(t.Left)
		rk, rv := y.ParseKey(t.Right), y.ParseTs(t.Right)
		fmt.Printf("SSTable [L%d, %03d] [%20X, v%-10d -> %20X, v%-10d]\n",
			t.Level, t.ID, lk, lv, rk, rv)
	}
	return nil
}

func printInfo(dir, valueDir string) error {
	if dir == "" {
		return fmt.Errorf("--dir not supplied")
	}
	if valueDir == "" {
		valueDir = dir
	}
	fp, err := os.Open(filepath.Join(dir, badger.ManifestFilename))
	if err != nil {
		return err
	}
	defer func() {
		if fp != nil {
			fp.Close()
		}
	}()
	manifest, truncOffset, err := badger.ReplayManifestFile(fp)
	if err != nil {
		return err
	}
	fp.Close()
	fp = nil

	fileinfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	fileinfoByName := make(map[string]os.FileInfo)
	fileinfoMarked := make(map[string]bool)
	for _, info := range fileinfos {
		fileinfoByName[info.Name()] = info
		fileinfoMarked[info.Name()] = false
	}

	fmt.Println()
	var baseTime time.Time
	// fmt.Print("\n[Manifest]\n")
	manifestTruncated := false
	manifestInfo, ok := fileinfoByName[badger.ManifestFilename]
	if ok {
		fileinfoMarked[badger.ManifestFilename] = true
		truncatedString := ""
		if truncOffset != manifestInfo.Size() {
			truncatedString = fmt.Sprintf(" [TRUNCATED to %d]", truncOffset)
			manifestTruncated = true
		}

		baseTime = manifestInfo.ModTime()
		fmt.Printf("[%25s] %-12s %6s MA%s\n", manifestInfo.ModTime().Format(time.RFC3339),
			manifestInfo.Name(), bytes(manifestInfo.Size()), truncatedString)
	} else {
		fmt.Printf("%s [MISSING]\n", manifestInfo.Name())
	}

	numMissing := 0
	numEmpty := 0

	levelSizes := make([]int64, len(manifest.Levels))
	for level, lm := range manifest.Levels {
		// fmt.Printf("\n[Level %d]\n", level)
		// We create a sorted list of table ID's so that output is in consistent order.
		tableIDs := make([]uint64, 0, len(lm.Tables))
		for id := range lm.Tables {
			tableIDs = append(tableIDs, id)
		}
		sort.Slice(tableIDs, func(i, j int) bool {
			return tableIDs[i] < tableIDs[j]
		})
		for _, tableID := range tableIDs {
			tableFile := table.IDToFilename(tableID)
			file, ok := fileinfoByName[tableFile]
			if ok {
				fileinfoMarked[tableFile] = true
				emptyString := ""
				fileSize := file.Size()
				if fileSize == 0 {
					emptyString = " [EMPTY]"
					numEmpty++
				}
				levelSizes[level] += fileSize
				// (Put level on every line to make easier to process with sed/perl.)
				fmt.Printf("[%25s] %-12s %6s L%d%s\n", dur(baseTime, file.ModTime()),
					tableFile, bytes(fileSize), level, emptyString)
			} else {
				fmt.Printf("%s [MISSING]\n", tableFile)
				numMissing++
			}
		}
	}

	valueDirFileinfos := fileinfos
	if valueDir != dir {
		valueDirFileinfos, err = ioutil.ReadDir(valueDir)
		if err != nil {
			return err
		}
	}

	// If valueDir is different from dir, holds extra files in the value dir.
	valueDirExtras := []os.FileInfo{}

	valueLogSize := int64(0)
	// fmt.Print("\n[Value Log]\n")
	for _, file := range valueDirFileinfos {
		if !strings.HasSuffix(file.Name(), ".vlog") {
			if valueDir != dir {
				valueDirExtras = append(valueDirExtras, file)
			}
			continue
		}

		fileSize := file.Size()
		emptyString := ""
		if fileSize == 0 {
			emptyString = " [EMPTY]"
			numEmpty++
		}
		valueLogSize += fileSize
		fmt.Printf("[%25s] %-12s %6s VL%s\n", dur(baseTime, file.ModTime()), file.Name(),
			bytes(fileSize), emptyString)

		fileinfoMarked[file.Name()] = true
	}

	numExtra := 0
	for _, file := range fileinfos {
		if fileinfoMarked[file.Name()] {
			continue
		}
		if numExtra == 0 {
			fmt.Print("\n[EXTRA]\n")
		}
		fmt.Printf("[%s] %-12s %6s\n", file.ModTime().Format(time.RFC3339),
			file.Name(), bytes(file.Size()))
		numExtra++
	}

	numValueDirExtra := 0
	for _, file := range valueDirExtras {
		if numValueDirExtra == 0 {
			fmt.Print("\n[ValueDir EXTRA]\n")
		}
		fmt.Printf("[%s] %-12s %6s\n", file.ModTime().Format(time.RFC3339),
			file.Name(), bytes(file.Size()))
		numValueDirExtra++
	}

	fmt.Print("\n[Summary]\n")
	totalIndexSize := int64(0)
	for i, sz := range levelSizes {
		fmt.Printf("Level %d size: %12s\n", i, bytes(sz))
		totalIndexSize += sz
	}

	fmt.Printf("Total index size: %8s\n", bytes(totalIndexSize))
	fmt.Printf("Value log size: %10s\n", bytes(valueLogSize))
	fmt.Println()
	totalExtra := numExtra + numValueDirExtra
	if totalExtra == 0 && numMissing == 0 && numEmpty == 0 && !manifestTruncated {
		fmt.Println("Abnormalities: None.")
	} else {
		fmt.Println("Abnormalities:")
	}
	fmt.Printf("%d extra %s.\n", totalExtra, pluralFiles(totalExtra))
	fmt.Printf("%d missing %s.\n", numMissing, pluralFiles(numMissing))
	fmt.Printf("%d empty %s.\n", numEmpty, pluralFiles(numEmpty))
	fmt.Printf("%d truncated %s.\n", boolToNum(manifestTruncated),
		pluralManifest(manifestTruncated))

	return nil
}

func boolToNum(x bool) int {
	if x {
		return 1
	}
	return 0
}

func pluralManifest(manifestTruncated bool) string {
	if manifestTruncated {
		return "manifest"
	}
	return "manifests"
}

func pluralFiles(count int) string {
	if count == 1 {
		return "file"
	}
	return "files"
}
