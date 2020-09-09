# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Serialization Versioning](VERSIONING.md).

## [Unreleased]

## [1.6.2] - 2020-09-10

### Fixed
 - Fix Sequence generates duplicate values (#1281)
 - Ensure `bitValuePointer` flag is cleared for LSM entry values written to LSM (#1313)
 - Confirm `badgerMove` entry required before rewrite (#1302)
 - Drop move keys when its key prefix is dropped (#1331)
 - Compaction: Expired keys and delete markers are never purged (#1354)
 - Restore: Account for value size as well (#1358)
 - GC: Consider size of value while rewriting (#1357)
 - Rework DB.DropPrefix (#1381)
 - Update head while replaying value log (#1372)
 - Remove vlog file if bootstrap, syncDir or mmap fails (#1434)
 - Levels: Compaction incorrectly drops some delete markers (#1422)
 - Fix(replay) - Update head for LSM entries also (#1456)
 - Fix(Backup/Restore): Keep all versions (#1462)
 - Fix build on Plan 9 (#1451)

## [1.6.1] - 2020-03-26

### New APIs
 - Badger.DB
	- NewWriteBatchAt (#948)
 - Badger.Options
	- WithEventLogging (#1035)
	- WithVerifyValueChecksum (#1052)
	- WithBypassLockGuard (#1243)

### Features
 - Support checksum verification for values read from vlog (#1052)
 - Add EventLogging option (#1035)
 - Support WriteBatch API in managed mode (#948)
 - Add support for watching nil prefix in Subscribe API (#1246)

### Fixed
 - Initialize vlog before starting compactions in db.Open (#1226)
 - Fix int overflow for 32bit (#1216)
 - Remove the 'this entry should've caught' log from value.go (#1170)
 - Fix merge iterator duplicates issue (#1157)
 - Fix segmentation fault in vlog.Read (header.Decode) (#1150)
 - Fix VerifyValueChecksum checks (#1138)
 - Fix windows dataloss issue (#1134)
 - Fix request increment ref bug (#1121)
 - Limit manifest's change set size (#1119)
 - Fix deadlock in discard stats (#1070)
 - Acquire lock before unmapping vlog files (#1050)
 - Set move key's expiresAt for keys with TTL (#1006)
 - Fix deadlock when flushing discard stats. (#976)
 - Fix table.Smallest/Biggest and iterator Prefix bug (#997)
 - Fix boundaries on GC batch size (#987)
 - Lock log file before munmap (#949)
 - VlogSize to store correct directory name to expvar.Map (#956)
 - Fix transaction too big issue in restore (#957)
 - Fix race condition in updateDiscardStats (#973)
 - Cast results of len to uint32 to fix compilation in i386 arch. (#961)
 - Drop discard stats if we can't unmarshal it (#936)
 - Open all vlog files in RDWR mode (#923)
 - Fix race condition in flushDiscardStats function (#921)
 - Ensure rewrite in vlog is within transactional limits (#911)
 - Fix prefix bug in key iterator and allow all versions (#950)
 - Fix discard stats moved by GC bug (#929)

### Performance
 - Use fastRand instead of locked-rand in skiplist (#1173)
 - Fix checkOverlap in compaction (#1166)
 - Optimize createTable in stream_writer.go (#1132)
 - Add capacity to slice creation when capacity is known (#1103)
 - Introduce fast merge iterator (#1080)
 - Introduce StreamDone in Stream Writer (#1061)
 - Flush vlog buffer if it grows beyond threshold (#1067)
 - Binary search based table picker (#983)
 - Making the stream writer APIs goroutine-safe (#959)
 - Replace FarmHash with AESHash for Oracle conflicts (#952)
 - Change file picking strategy in compaction (#894)
 - Use trie for prefix matching (#851)
 - Fix busy-wait loop in Watermark (#920)


## [1.6.0] - 2019-07-01

This is a release including almost 200 commits, so expect many changes - some of them
not backward compatible.

Regarding backward compatibility in Badger versions, you might be interested on reading
[VERSIONING.md](VERSIONING.md).

_Note_: The hashes in parentheses correspond to the commits that impacted the given feature.

### New APIs

- badger.DB
  - DropPrefix (291295e)
  - Flatten (7e41bba)
  - KeySplits (4751ef1)
  - MaxBatchCount (b65e2a3)
  - MaxBatchSize (b65e2a3)
  - PrintKeyValueHistogram (fd59907)
  - Subscribe (26128a7)
  - Sync (851e462)

- badger.DefaultOptions() and badger.LSMOnlyOptions() (91ce687)
  - badger.Options.WithX methods

- badger.Entry (e9447c9)
  - NewEntry
  - WithMeta
  - WithDiscard
  - WithTTL 

- badger.Item
  - KeySize (fd59907)
  - ValueSize (5242a99)

- badger.IteratorOptions
  - PickTable (7d46029, 49a49e3)
  - Prefix (7d46029)

- badger.Logger (fbb2778)

- badger.Options
  - CompactL0OnClose (7e41bba)
  - Logger (3f66663)
  - LogRotatesToFlush (2237832)

- badger.Stream (14cbd89, 3258067)
- badger.StreamWriter (7116e16)
- badger.TableInfo.KeyCount (fd59907)
- badger.TableManifest (2017987)
- badger.Tx.NewKeyIterator (49a49e3)
- badger.WriteBatch (6daccf9, 7e78e80)

### Modified APIs

#### Breaking changes:

- badger.DefaultOptions and badger.LSMOnlyOptions are now functions rather than variables (91ce687)
- badger.Item.Value now receives a function that returns an error (439fd46)
- badger.Txn.Commit doesn't receive any params now (6daccf9)
- badger.DB.Tables now receives a boolean (76b5341)

#### Not breaking changes:

- badger.LSMOptions changed values (799c33f)
- badger.DB.NewIterator now allows multiple iterators per RO txn (41d9656)
- badger.Options.TableLoadingMode's new default is options.MemoryMap (6b97bac)

### Removed APIs

- badger.ManagedDB (d22c0e8)
- badger.Options.DoNotCompact (7e41bba)
- badger.Txn.SetWithX (e9447c9)

### Tools:

- badger bank disect (13db058)
- badger bank test (13db058) --mmap (03870e3)
- badger fill (7e41bba)
- badger flatten (7e41bba)
- badger info --histogram (fd59907) --history --lookup --show-keys --show-meta --with-prefix (09e9b63) --show-internal (fb2eed9)
- badger benchmark read (239041e)
- badger benchmark write (6d3b67d)

## [1.5.5] - 2019-06-20

* Introduce support for Go Modules

## [1.5.3] - 2018-07-11
Bug Fixes:
* Fix a panic caused due to item.vptr not copying over vs.Value, when looking
    for a move key.

## [1.5.2] - 2018-06-19
Bug Fixes:
* Fix the way move key gets generated.
* If a transaction has unclosed, or multiple iterators running simultaneously,
    throw a panic. Every iterator must be properly closed. At any point in time,
    only one iterator per transaction can be running. This is to avoid bugs in a
    transaction data structure which is thread unsafe.

* *Warning: This change might cause panics in user code. Fix is to properly
    close your iterators, and only have one running at a time per transaction.*

## [1.5.1] - 2018-06-04
Bug Fixes:
* Fix for infinite yieldItemValue recursion. #503
* Fix recursive addition of `badgerMove` prefix. https://github.com/dgraph-io/badger/commit/2e3a32f0ccac3066fb4206b28deb39c210c5266f
* Use file size based window size for sampling, instead of fixing it to 10MB. #501

Cleanup:
* Clarify comments and documentation.
* Move badger tool one directory level up.

## [1.5.0] - 2018-05-08
* Introduce `NumVersionsToKeep` option. This option is used to discard many
  versions of the same key, which saves space.
* Add a new `SetWithDiscard` method, which would indicate that all the older
  versions of the key are now invalid. Those versions would be discarded during
  compactions.
* Value log GC moves are now bound to another keyspace to ensure latest versions
  of data are always at the top in LSM tree.
* Introduce `ValueLogMaxEntries` to restrict the number of key-value pairs per
  value log file. This helps bound the time it takes to garbage collect one
  file.

## [1.4.0] - 2018-05-04
* Make mmap-ing of value log optional.
* Run GC multiple times, based on recorded discard statistics.
* Add MergeOperator.
* Force compact L0 on clsoe (#439).
* Add truncate option to warn about data loss (#452).
* Discard key versions during compaction (#464).
* Introduce new `LSMOnlyOptions`, to make Badger act like a typical LSM based DB.

Bug fix:
* (Temporary) Check max version across all tables in Get (removed in next
  release).
* Update commit and read ts while loading from backup.
* Ensure all transaction entries are part of the same value log file.
* On commit, run unlock callbacks before doing writes (#413).
* Wait for goroutines to finish before closing iterators (#421).

## [1.3.0] - 2017-12-12
* Add `DB.NextSequence()` method to generate monotonically increasing integer
  sequences.
* Add `DB.Size()` method to return the size of LSM and value log files.
* Tweaked mmap code to make Windows 32-bit builds work.
* Tweaked build tags on some files to make iOS builds work.
* Fix `DB.PurgeOlderVersions()` to not violate some constraints.

## [1.2.0] - 2017-11-30
* Expose a `Txn.SetEntry()` method to allow setting the key-value pair
  and all the metadata at the same time.

## [1.1.1] - 2017-11-28
* Fix bug where txn.Get was returing key deleted in same transaction.
* Fix race condition while decrementing reference in oracle.
* Update doneCommit in the callback for CommitAsync.
* Iterator see writes of current txn.

## [1.1.0] - 2017-11-13
* Create Badger directory if it does not exist when `badger.Open` is called.
* Added `Item.ValueCopy()` to avoid deadlocks in long-running iterations
* Fixed 64-bit alignment issues to make Badger run on Arm v7

## [1.0.1] - 2017-11-06
* Fix an uint16 overflow when resizing key slice

[Unreleased]: https://github.com/dgraph-io/badger/compare/v1.6.2...HEAD
[1.6.2]: https://github.com/dgraph-io/badger/compare/v1.6.1...v1.6.2
[1.6.1]: https://github.com/dgraph-io/badger/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/dgraph-io/badger/compare/v1.5.5...v1.6.0
[1.5.5]: https://github.com/dgraph-io/badger/compare/v1.5.3...v1.5.5
[1.5.3]: https://github.com/dgraph-io/badger/compare/v1.5.2...v1.5.3
[1.5.2]: https://github.com/dgraph-io/badger/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/dgraph-io/badger/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/dgraph-io/badger/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/dgraph-io/badger/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/dgraph-io/badger/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/dgraph-io/badger/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/dgraph-io/badger/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/dgraph-io/badger/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/dgraph-io/badger/compare/v1.0.0...v1.0.1
