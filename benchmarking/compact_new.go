// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dgraph.io/badger/internal/base"
	"github.com/dgraph.io/badger/internal/manifest"
	"github.com/dgraph.io/badger/internal/record"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var compactNewConfig struct {
	deleteSourceFiles bool
}

var compactNewCmd = &cobra.Command{
	Use:   "new <data src> <workload dst>",
	Short: "construct a new bench compact workload from a Cockroach archive",
	Args:  cobra.ExactArgs(2),
	RunE:  runCompactNew,
}

func init() {
	compactNewCmd.Flags().BoolVar(&compactNewConfig.deleteSourceFiles,
		"delete", false, "delete source sstables as they're copied")
}

func runCompactNew(cmd *cobra.Command, args []string) error {
	src, workloadDst := args[0], args[1]
	archiveDir := filepath.Join(src, "archive")
	err := os.MkdirAll(workloadDst, os.ModePerm)
	if err != nil {
		return err
	}

	manifests, hist, err := replayManifests(src)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		err := vfs.Copy(vfs.Default, manifest, filepath.Join(workloadDst, filepath.Base(manifest)))
		if err != nil {
			return err
		}
	}

	for _, logItem := range hist {
		if logItem.compaction {
			continue
		}

		for _, f := range logItem.ve.NewFiles {
			// First look in the archive, because that's likely where it is.
			srcPath := base.MakeFilename(vfs.Default, archiveDir, base.FileTypeTable, f.Meta.FileNum)
			dstPath := base.MakeFilename(vfs.Default, workloadDst, base.FileTypeTable, f.Meta.FileNum)
			err := vfs.LinkOrCopy(vfs.Default, srcPath, dstPath)
			if oserror.IsNotExist(err) {
				// Maybe it's still in the data directory.
				srcPath = base.MakeFilename(vfs.Default, src, base.FileTypeTable, f.Meta.FileNum)
				err = vfs.LinkOrCopy(vfs.Default, srcPath, dstPath)
			}
			if err != nil {
				return errors.Wrap(err, filepath.Base(srcPath))
			}
			verbosef("Linked or copied %s to %s.\n", srcPath, dstPath)
			if compactNewConfig.deleteSourceFiles {
				err := os.Remove(srcPath)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type logItem struct {
	compaction bool
	flush      bool
	ve         manifest.VersionEdit
}

type fileEntry struct {
	level int
	meta  *manifest.FileMetadata
}

// replayManifests replays all manifests from the archive and the data
// directory in order, returning an in-order history of sstable additions,
// deletions and moves.
func replayManifests(srcPath string) ([]string, []logItem, error) {
	var history []logItem
	var manifests []string
	metas := make(map[base.FileNum]*manifest.FileMetadata)

	// If there's an archive directory in srcPath, look in the archive for
	// deleted manifests.
	archivePath := filepath.Join(srcPath, "archive")
	infos, err := ioutil.ReadDir(archivePath)
	if err != nil && !oserror.IsNotExist(err) {
		return nil, nil, err
	}
	for _, info := range infos {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || typ != base.FileTypeManifest {
			continue
		}
		path := filepath.Join(archivePath, info.Name())
		manifestLog, err := loadManifest(path, metas)
		if err != nil {
			return nil, nil, err
		}
		manifests = append(manifests, path)
		history = append(history, manifestLog...)
	}

	// Next look directly in srcPath.
	infos, err = ioutil.ReadDir(srcPath)
	if err != nil {
		return nil, nil, err
	}
	for _, info := range infos {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || typ != base.FileTypeManifest {
			continue
		}
		path := filepath.Join(srcPath, info.Name())
		manifestLog, err := loadManifest(path, metas)
		if err != nil {
			return nil, nil, err
		}
		manifests = append(manifests, path)
		history = append(history, manifestLog...)
	}

	return manifests, history, nil
}

func loadManifest(path string, metas map[base.FileNum]*manifest.FileMetadata) ([]logItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var log []logItem
	rr := record.NewReader(f, 0 /* logNum */)

	// A manifest's first record contains all the active files when the
	// manifest was created. We only care about edits to the version.
	if _, err = rr.Next(); err != nil {
		return nil, err
	}

	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			return nil, err
		}
		if len(ve.NewFiles) == 0 && len(ve.DeletedFiles) == 0 {
			continue
		}

		li := logItem{
			// If a version edit deletes files, we assume it's a compaction.
			// Only non-compaction added files will be replayed.
			compaction: len(ve.DeletedFiles) != 0,
			flush:      false,
			ve:         ve,
		}
		for _, nf := range ve.NewFiles {
			metas[nf.Meta.FileNum] = nf.Meta
			li.flush = !li.compaction && (li.flush || nf.Meta.SmallestSeqNum != nf.Meta.LargestSeqNum)
		}
		log = append(log, li)
	}
	return log, nil
}
