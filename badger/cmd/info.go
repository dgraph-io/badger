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
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	humanize "github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

type flagOptions struct {
	showTables    bool
	sizeHistogram bool
	dumpData      bool
	withPrefix    string
}

var (
	opt flagOptions
)

func init() {
	RootCmd.AddCommand(infoCmd)
	infoCmd.Flags().BoolVarP(&opt.showTables, "show-tables", "s", false,
		"If set to true, show tables as well.")
	infoCmd.Flags().BoolVar(&opt.sizeHistogram, "histogram", false,
		"Show a histogram of the key and value sizes.")
	infoCmd.Flags().BoolVarP(&opt.dumpData, "dump", "d", false, "Dump keys and values in Badger")
	infoCmd.Flags().StringVar(&opt.withPrefix, "with-prefix", "", "Consider only the keys with specified prefix")
}

var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Health info about Badger database.",
	Long: `
This command prints information about the badger key-value store.  It reads MANIFEST and prints its
info. It also prints info about missing/extra files, and general information about the value log
files (which are not referenced by the manifest).  Use this tool to report any issues about Badger
to the Dgraph team.
`,
	RunE: handleInfo,
}

func handleInfo(cmd *cobra.Command, args []string) error {
	err := printInfo(sstDir, vlogDir)
	if err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}

	// Open DB
	opts := badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = sstDir
	opts.ValueDir = vlogDir
	opts.ReadOnly = true

	db, err := badger.Open(opts)
	if err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
	defer db.Close()

	if opt.showTables {
		err = tableInfo(sstDir, vlogDir, db)
		if err != nil {
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
	}

	if opt.sizeHistogram {
		db.PrintHistogram([]byte(opt.withPrefix))
	}

	if opt.dumpData {
		if err := dumpData(db, opt.withPrefix); err != nil {
			return err
		}
	}
	return nil
}

func dumpData(db *badger.DB, prefix string) error {
	if len(prefix) > 0 {
		fmt.Printf("Only choosing keys with prefix: %s\n", prefix)
	}
	fmt.Printf("\nInformation about keys and values in the store\n")
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		totalKeys := 0
		for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
			item := it.Item()
			if item.IsDeletedOrExpired() {
				continue
			}
			var v []byte
			v, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			keyNoPrefix := item.Key()[len(prefix):]
			fmt.Printf("Key=%X  Value=%s  Version=%d  Metadata=%b\n", keyNoPrefix, v, item.Version(), item.UserMeta())
			totalKeys++
		}
		fmt.Print("\n[Summary]\n")
		fmt.Println("Total Number of keys:", totalKeys)
		return nil
	})
	return err
}

func hbytes(sz int64) string {
	return humanize.Bytes(uint64(sz))
}

func dur(src, dst time.Time) string {
	return humanize.RelTime(dst, src, "earlier", "later")
}

func tableInfo(dir, valueDir string, db *badger.DB) error {
	tables := db.Tables()
	fmt.Println()
	fmt.Println("SSTable [Li, Id, Total Keys including internal keys] [Left Key, Version -> Right Key, Version]")
	for _, t := range tables {
		lk, lt := y.ParseKey(t.Left), y.ParseTs(t.Left)
		rk, rt := y.ParseKey(t.Right), y.ParseTs(t.Right)

		fmt.Printf("SSTable [L%d, %03d, %07d] [%20X, v%d -> %20X, v%d]\n",
			t.Level, t.ID, t.KeyCount, lk, lt, rk, rt)
	}
	fmt.Println()
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
			manifestInfo.Name(), hbytes(manifestInfo.Size()), truncatedString)
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
			tm, ok1 := manifest.Tables[tableID]
			file, ok2 := fileinfoByName[tableFile]
			if ok1 && ok2 {
				fileinfoMarked[tableFile] = true
				emptyString := ""
				fileSize := file.Size()
				if fileSize == 0 {
					emptyString = " [EMPTY]"
					numEmpty++
				}
				levelSizes[level] += fileSize
				// (Put level on every line to make easier to process with sed/perl.)
				fmt.Printf("[%25s] %-12s %6s L%d %x%s\n", dur(baseTime, file.ModTime()),
					tableFile, hbytes(fileSize), level, tm.Checksum, emptyString)
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
			hbytes(fileSize), emptyString)

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
			file.Name(), hbytes(file.Size()))
		numExtra++
	}

	numValueDirExtra := 0
	for _, file := range valueDirExtras {
		if numValueDirExtra == 0 {
			fmt.Print("\n[ValueDir EXTRA]\n")
		}
		fmt.Printf("[%s] %-12s %6s\n", file.ModTime().Format(time.RFC3339),
			file.Name(), hbytes(file.Size()))
		numValueDirExtra++
	}

	fmt.Print("\n[Summary]\n")
	totalIndexSize := int64(0)
	for i, sz := range levelSizes {
		fmt.Printf("Level %d size: %12s\n", i, hbytes(sz))
		totalIndexSize += sz
	}

	fmt.Printf("Total index size: %8s\n", hbytes(totalIndexSize))
	fmt.Printf("Value log size: %10s\n", hbytes(valueLogSize))
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
