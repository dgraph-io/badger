/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	benchmarkRewriteExpiredKeyCount         = 8192
	benchmarkRewriteLiveKeyCount            = 4096
	benchmarkRewriteValueSize               = 2 << 10
	benchmarkRewriteValueLogFileSize        = 8 << 20
	benchmarkRewriteValueThreshold          = 128
	benchmarkRewriteEntryExpiry      uint64 = 1
)

func benchmarkValueGCRewriteOptions(dir string) Options {
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = benchmarkRewriteValueLogFileSize
	opt.BaseTableSize = 1 << 20
	opt.BaseLevelSize = 4 << 20
	opt.ValueThreshold = benchmarkRewriteValueThreshold
	opt.NumCompactors = 0
	opt.MetricsEnabled = false
	return opt
}

func benchmarkWriteEntries(b *testing.B, db *DB, prefix string, count int, value []byte, expiresAt uint64) {
	b.Helper()

	for i := 0; i < count; {
		txn := db.NewTransaction(true)
		for ; i < count; i++ {
			entry := NewEntry([]byte(fmt.Sprintf("%s-%08d", prefix, i)), value)
			entry.ExpiresAt = expiresAt

			err := txn.SetEntry(entry)
			if err == ErrTxnTooBig {
				require.NoError(b, txn.Commit())
				txn = nil
				break
			}
			require.NoError(b, err)
		}
		if txn != nil {
			require.NoError(b, txn.Commit())
		}
	}
}

func benchmarkPrepareRewriteFixture(b *testing.B, dir string) {
	b.Helper()

	db, err := Open(benchmarkValueGCRewriteOptions(dir))
	require.NoError(b, err)

	expiredValue := bytes.Repeat([]byte("e"), benchmarkRewriteValueSize)
	liveValue := bytes.Repeat([]byte("l"), benchmarkRewriteValueSize)

	// Fill the earliest vlog files with expired entries before appending live entries.
	benchmarkWriteEntries(b, db, "expired", benchmarkRewriteExpiredKeyCount, expiredValue, benchmarkRewriteEntryExpiry)
	benchmarkWriteEntries(b, db, "live", benchmarkRewriteLiveKeyCount, liveValue, 0)

	require.NoError(b, db.Close())
}

func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported file mode for %s", path)
		}

		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}

		dstFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			_ = srcFile.Close()
			return err
		}
		if _, err := io.Copy(dstFile, srcFile); err != nil {
			_ = srcFile.Close()
			_ = dstFile.Close()
			return err
		}
		if err := srcFile.Close(); err != nil {
			_ = dstFile.Close()
			return err
		}
		return dstFile.Close()
	})
}

func BenchmarkValueGCRewriteExpiredOnlyFile(b *testing.B) {
	rootDir := b.TempDir()
	fixtureDir := filepath.Join(rootDir, "fixture")
	benchmarkPrepareRewriteFixture(b, fixtureDir)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		runDir := filepath.Join(rootDir, fmt.Sprintf("run-%03d", i))
		require.NoError(b, copyDir(fixtureDir, runDir))

		db, err := Open(benchmarkValueGCRewriteOptions(runDir))
		require.NoError(b, err)

		fids := db.vlog.sortedFids()
		require.Greater(b, len(fids), 1)

		db.vlog.filesLock.RLock()
		lf := db.vlog.filesMap[fids[0]]
		db.vlog.filesLock.RUnlock()

		b.StartTimer()
		err = db.vlog.rewrite(lf)
		b.StopTimer()
		require.NoError(b, err)

		require.NoError(b, db.Close())
		removeDir(runDir)
	}
}
