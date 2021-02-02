package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime/trace"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3/internal/base"
	"github.com/dgraph-io/badger/v3/internal/humanize"
	"github.com/dgraph-io/badger/v3/internal/manifest"
	"github.com/dgraph-io/badger/v3/internal/replay"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/codahale/hdrhistogram"
	"github.com/spf13/cobra"
)

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "compaction benchmarks",
}

var compactRunConfig struct {
	trace   bool
	profile bool
}

var compactRunCmd = &cobra.Command{
	Use:   "run <workload dir>",
	Short: "run a compaction benchmark through ingesting sstables",
	Args:  cobra.ExactArgs(1),
	RunE:  runReplay,
}

func init() {
	compactRunCmd.Flags().BoolVar(&compactRunConfig.trace,
		"trace", false, "collect traces throughout the benchmark run")
	compactRunCmd.Flags().BoolVar(&compactRunConfig.profile,
		"profile", false, "collect pprof profiles throughout the benchmark run")
}

const numLevels = 7

type compactionTracker struct {
	mu         sync.Mutex
	activeJobs map[int]bool
	waiter     chan struct{}
}

func (t *compactionTracker) countActive() (int, chan struct{}) {
	ch := make(chan struct{})
	t.mu.Lock()
	count := len(t.activeJobs)
	t.waiter = ch
	t.mu.Unlock()
	return count, ch
}

func (t *compactionTracker) apply(jobID int) {
	t.mu.Lock()
	if t.activeJobs[jobID] {
		delete(t.activeJobs, jobID)
	} else {
		t.activeJobs[jobID] = true
	}
	if t.waiter != nil {
		close(t.waiter)
		t.waiter = nil
	}
	t.mu.Unlock()
}

// eventListener returns a Pebble event listener that listens for compaction
// events and appends them to the queue.
func (t *compactionTracker) eventListener() pebble.EventListener {
	return pebble.EventListener{
		CompactionBegin: func(info pebble.CompactionInfo) {
			t.apply(info.JobID)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			t.apply(info.JobID)
		},
	}
}

func open(dir string, listener pebble.EventListener) (*replay.DB, error) {
	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    mvccComparer,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    2,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       400,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10,
		}},
		Merger: fauxMVCCMerger,
	}
	opts.EnsureDefaults()

	opts.EventListener = listener
	if verbose {
		opts.EventListener = teeEventListener(opts.EventListener,
			pebble.MakeLoggingEventListener(nil))
	}
	rd, err := replay.Open(dir, opts)
	return rd, err
}

// hardLinkWorkload makes a hardlink of every manifest and sstable file from
// src inside dst. Pebble will remove ingested sstables from their original
// path.  Making a hardlink of all the sstables ensures that we can run the
// same workload multiple times.
func hardLinkWorkload(src, dst string) error {
	ls, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}
	for _, info := range ls {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || (typ != base.FileTypeManifest && typ != base.FileTypeTable) {
			continue
		}
		err := os.Link(filepath.Join(src, info.Name()), filepath.Join(dst, info.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func startSamplingRAmp(d *replay.DB) func() *hdrhistogram.Histogram {
	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	hist := hdrhistogram.New(0, 100, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer ticker.Stop()
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				ramp := d.Metrics().ReadAmp()
				if err := hist.RecordValue(int64(ramp)); err != nil {
					panic(err)
				}
			case <-done:
				return
			}
		}
	}()
	return func() *hdrhistogram.Histogram {
		close(done)
		wg.Wait()
		return hist
	}
}

func runReplay(cmd *cobra.Command, args []string) error {
	// Make hard links of all the files in the workload so ingestion doesn't
	// delete our original copy of a workload.
	workloadDir, err := ioutil.TempDir(args[0], "pebble-bench-workload")
	if err != nil {
		return err
	}
	defer removeAll(workloadDir)
	if err := hardLinkWorkload(args[0], workloadDir); err != nil {
		return err
	}

	_, hist, err := replayManifests(workloadDir)
	if err != nil {
		return err
	}
	var workloadSize int64
	var workloadTableCount int
	for _, li := range hist {
		if !li.compaction {
			for _, f := range li.ve.NewFiles {
				workloadSize += int64(f.Meta.Size)
				workloadTableCount++
			}
		}
	}
	verbosef("Workload contains %s across %d sstables to replay.\n",
		humanize.Int64(workloadSize), workloadTableCount)
	if workloadTableCount == 0 {
		return errors.New("empty workload")
	}

	dir, err := ioutil.TempDir(args[0], "pebble-bench-data")
	if err != nil {
		return err
	}
	defer removeAll(dir)
	verbosef("Opening database in %q.\n", dir)

	compactions := compactionTracker{activeJobs: map[int]bool{}}
	rd, err := open(dir, compactions.eventListener())
	if err != nil {
		return err
	}
	defer rd.Close()

	m := rd.Metrics()
	start := time.Now()

	if compactRunConfig.profile {
		stopProf := startCPUProfile()
		defer stopProf()
	}
	if compactRunConfig.trace {
		stopTrace := startRecording("trace.%04d.out", trace.Start, trace.Stop)
		defer stopTrace()
	}

	stopSamplingRAmp := startSamplingRAmp(rd)

	var replayedCount int
	var sizeSum, sizeCount int64
	var ref *manifest.Version
	for _, li := range hist {
		// Maintain ref as the reference database's version for calculating
		// read amplification.
		var bve manifest.BulkVersionEdit
		bve.Accumulate(&li.ve)
		ref, _, err = bve.Apply(ref, mvccCompare, nil, 0, 0)
		if err != nil {
			return err
		}

		// Ignore manifest changes due to compactions.
		if li.compaction {
			continue
		}

		// We keep the current database's L0 read amplification equal to the
		// reference database's L0 read amplification at the same point in its
		// execution. If we've exceeded it, wait for compactions to catch up.
		var skipCh <-chan time.Time
		for skip := false; !skip; {
			refReadAmplification := ref.L0Sublevels.ReadAmplification()

			activeCompactions, waiterCh := compactions.countActive()
			m = rd.Metrics()
			if int(m.Levels[0].Sublevels) <= refReadAmplification {
				break
			}

			verbosef("L0 read amplification %d > reference L0 read amplification %d; pausing to let compaction catch up.\n",
				m.Levels[0].Sublevels, refReadAmplification)

			// If the current database's read amplification has exceeded the
			// reference by 1 and there are no active compactions, allow it to
			// proceed after 10 milliseconds, as long as no compaction starts.
			if activeCompactions == 0 && (int(m.Levels[0].Sublevels)-refReadAmplification) == 1 {
				skipCh = time.After(10 * time.Millisecond)
			}
			select {
			case <-waiterCh:
				// A compaction event was processed, continue to check the
				// read amplfication.
				skipCh = nil
				continue
			case <-skipCh:
				skip = true
				verbosef("No compaction in 10 milliseconds. Proceeding anyways.\n")
			}
		}

		// Track the average database size.
		sizeCount++
		for _, l := range m.Levels {
			sizeSum += l.Size
		}

		if li.flush {
			for _, f := range li.ve.NewFiles {
				name := fmt.Sprintf("%s.sst", f.Meta.FileNum)
				tablePath := filepath.Join(workloadDir, name)
				replayedCount++
				verbosef("Flushing table %d/%d %s (%s, seqnums %d-%d)\n", replayedCount, workloadTableCount,
					name, humanize.Int64(int64(f.Meta.Size)), f.Meta.SmallestSeqNum, f.Meta.LargestSeqNum)
				if err := rd.FlushExternal(replay.Table{Path: tablePath, FileMetadata: f.Meta}); err != nil {
					return err
				}
			}
		} else {
			var tables []replay.Table
			for _, f := range li.ve.NewFiles {
				name := fmt.Sprintf("%s.sst", f.Meta.FileNum)
				tablePath := filepath.Join(workloadDir, name)
				replayedCount++
				verbosef("Ingesting %d tables: table %d/%d %s (%s)\n", len(li.ve.NewFiles), replayedCount,
					workloadTableCount, name, humanize.Int64(int64(f.Meta.Size)))
				tables = append(tables, replay.Table{
					Path:         tablePath,
					FileMetadata: f.Meta,
				})
			}
			if err := rd.Ingest(tables); err != nil {
				return err
			}
		}
	}

	rampHist := stopSamplingRAmp()

	verbosef("Replayed all %d tables.\n", replayedCount)
	d := rd.Done()

	m = d.Metrics()
	fmt.Println(m)
	fmt.Println("Waiting for background compactions to complete.")

	for quiesced := false; !quiesced; {
		activeCompactions, waiterCh := compactions.countActive()
		if activeCompactions > 0 {
			<-waiterCh
			continue
		}

		// There are no active compactions, but one might be initiated by
		// the completion of a previous one. Wait a second, and break out
		// only if no new compactions are initiated.
		select {
		case <-time.After(time.Second):
			quiesced = true
		case <-waiterCh:
			continue
		}
	}

	fmt.Println("Background compactions finished.")
	m = d.Metrics()
	fmt.Println(m)

	fmt.Println("Manually compacting entire key space to calculate space amplification.")
	beforeSize := totalSize(m)
	iter := d.NewIter(nil)
	if err != nil {
		return err
	}
	var first, last []byte
	if iter.First() {
		first = append(first, iter.Key()...)
	}
	if iter.Last() {
		last = append(last, iter.Key()...)
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if err := d.Compact(first, last); err != nil {
		return err
	}
	afterSize := totalSize(d.Metrics())
	fmt.Println()
	fmt.Printf("__elapsed__compacts__w-amp__r-amp(_p50__p95__p99__max_)__space(_____amp__stable___final__average__)\n")
	fmt.Printf("  %6.2fm  %8d  %5.2f         %3d  %3d  %3d  %3d         %9.2f  %6s  %6s  %7s\n",
		time.Since(start).Minutes(),
		m.Compact.Count,
		totalWriteAmp(m),
		rampHist.ValueAtQuantile(50),
		rampHist.ValueAtQuantile(95),
		rampHist.ValueAtQuantile(99),
		rampHist.ValueAtQuantile(100),
		float64(beforeSize)/float64(afterSize),
		humanize.Int64(int64(beforeSize)),
		humanize.Int64(int64(afterSize)),
		humanize.Int64(sizeSum/sizeCount))
	return nil
}

func totalWriteAmp(m *pebble.Metrics) float64 {
	var total pebble.LevelMetrics
	for _, lm := range m.Levels {
		total.Add(&lm)
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested
	total.BytesIn = m.WAL.BytesWritten + total.BytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.BytesFlushed += total.BytesIn
	return total.WriteAmp()
}

func totalSize(m *pebble.Metrics) int64 {
	sz := int64(m.WAL.Size)
	// fmt.Printf("%d ----Sourav----", sz)
	for _, lm := range m.Levels {
		sz += lm.Size
	}
	return sz
}

func verbosef(fmtstr string, args ...interface{}) {
	if verbose {
		fmt.Printf(fmtstr, args...)
	}
}

func removeAll(dir string) {
	verbosef("Removing %q.\n", dir)
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// teeEventListener wraps two event listeners, forwarding all events to both.
func teeEventListener(a, b pebble.EventListener) pebble.EventListener {
	a.EnsureDefaults(nil)
	b.EnsureDefaults(nil)
	return pebble.EventListener{
		BackgroundError: func(err error) {
			a.BackgroundError(err)
			b.BackgroundError(err)
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			a.CompactionBegin(info)
			b.CompactionBegin(info)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			a.CompactionEnd(info)
			b.CompactionEnd(info)
		},
		FlushBegin: func(info pebble.FlushInfo) {
			a.FlushBegin(info)
			b.FlushBegin(info)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			a.FlushEnd(info)
			b.FlushEnd(info)
		},
		ManifestCreated: func(info pebble.ManifestCreateInfo) {
			a.ManifestCreated(info)
			b.ManifestCreated(info)
		},
		ManifestDeleted: func(info pebble.ManifestDeleteInfo) {
			a.ManifestDeleted(info)
			b.ManifestDeleted(info)
		},
		TableCreated: func(info pebble.TableCreateInfo) {
			a.TableCreated(info)
			b.TableCreated(info)
		},
		TableDeleted: func(info pebble.TableDeleteInfo) {
			a.TableDeleted(info)
			b.TableDeleted(info)
		},
		TableIngested: func(info pebble.TableIngestInfo) {
			a.TableIngested(info)
			b.TableIngested(info)
		},
		WALCreated: func(info pebble.WALCreateInfo) {
			a.WALCreated(info)
			b.WALCreated(info)
		},
		WALDeleted: func(info pebble.WALDeleteInfo) {
			a.WALDeleted(info)
			b.WALDeleted(info)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			a.WriteStallBegin(info)
			b.WriteStallBegin(info)
		},
		WriteStallEnd: func() {
			a.WriteStallEnd()
			b.WriteStallEnd()
		},
	}
}
