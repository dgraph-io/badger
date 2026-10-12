/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

// lsmbench is a bulk-load LSM benchmark used to detect compaction
// performance regressions (issue #2327). It recreates the reproducer from
// that issue: ~300M small entries written through db.Update in batches of
// 5000 against default options, keys spread across several top-level
// prefixes.
//
// It intentionally uses only badger's public API so the same harness can be
// compiled against any badger version (the module's replace directive points
// at the working tree; CI repoints it at a baseline checkout to compare the
// two on identical hardware).
//
// Beyond wall time, it tracks the structural signature of the #2327
// regression, which is hardware-independent:
//
//   - the base level must not collapse to L1/L2 once the tree is large
//     (healthy: L3+; broken: pinned at L1 from ~170M entries onward), and
//   - lifetime L0 write stalls must stay near zero (healthy: 0s; broken:
//     ~1-2 minutes at this scale).
//
// Exit code is non-zero if any enabled gate fails. The final line of output
// is machine-readable:
//
//	LSMBENCH_RESULT wall_ms=... stall_ms=... min_base=... final_base=... lsm_mb=... entries=...
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

type config struct {
	dir           string
	rows          int
	minBase       int           // fail if base level drops below this on a big tree; 0 disables
	maxStall      time.Duration // fail if lifetime L0 stall exceeds this; 0 disables
	maxShallowOcc int           // fail if more than this many big-tree checkpoints had tables in L1/L2; -1 disables
}

type results struct {
	wall      time.Duration
	stall     time.Duration
	minBase   int // lowest base level seen while LSM > bigTreeBytes; -1 if tree never got big
	finalBase int
	lsmBytes  int64
	entries   int

	// Checkpoint-sampled counters, only counted while LSM > bigTreeBytes.
	// Checkpoints are entry-indexed (every checkpointEvery entries), so these
	// are comparable across machines of different speeds.
	bigCheckpoints  int // checkpoints taken on a big tree
	collapsedCp     int // ... where the base level was L2 or shallower
	shallowOccCp    int // ... where L1 or L2 held any tables
	slowCompactures map[string]int
}

const (
	numFields       = 300
	batchSize       = 5000
	checkpointEvery = 10_000_000

	// The base-level gate only applies once the tree is large enough that the
	// size-ratio target is deep: at 2GB the level targets are L6=2GB,
	// L5=200MB, L4=20MB, L3=10MB, so a healthy base level is L3 or deeper.
	bigTreeBytes = 2 << 30
)

// stallLogger wraps badger's default logger and records the "Lifetime L0
// stalled for: <duration>" line that levelsController prints on Close. Log
// scraping is deliberate: it works on every badger version, old and new,
// which is what allows baseline-vs-head comparison with one harness.
type stallLogger struct {
	badger.Logger
	mu       sync.Mutex
	stall    time.Duration
	compacts map[string]int // slow (>2s) compactions by "src->dst"
}

var (
	stallRe = regexp.MustCompile(`Lifetime L0 stalled for: (\S+)`)
	// Note: badger only logs compactions that took >2s, so these counts are
	// diagnostics for humans, not a gate — fast hardware can complete even a
	// pathological 0->1 compaction under the logging threshold.
	compactRe = regexp.MustCompile(`LOG Compact (\d+)->(\d+)`)
)

func (l *stallLogger) Infof(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if m := stallRe.FindStringSubmatch(line); m != nil {
		if d, err := time.ParseDuration(m[1]); err == nil {
			l.mu.Lock()
			l.stall = d
			l.mu.Unlock()
		}
	}
	if m := compactRe.FindStringSubmatch(line); m != nil {
		l.mu.Lock()
		if l.compacts == nil {
			l.compacts = map[string]int{}
		}
		l.compacts[m[1]+"->"+m[2]]++
		l.mu.Unlock()
	}
	l.Logger.Infof(format, args...)
}

func baseLevel(db *badger.DB) int {
	for _, li := range db.Levels() {
		if li.IsBaseLevel {
			return li.Level
		}
	}
	return -1
}

func run(cfg config) (results, error) {
	res := results{minBase: -1}

	opts := badger.DefaultOptions(cfg.dir)
	logger := &stallLogger{Logger: opts.Logger}
	opts = opts.WithLogger(logger)

	db, err := badger.Open(opts)
	if err != nil {
		return res, fmt.Errorf("open: %w", err)
	}

	typePrefixes := []string{"type1", "type2", "type3", "type4", "type5"}
	fieldNames := make([]string, numFields)
	for f := 0; f < numFields; f++ {
		fieldNames[f] = fmt.Sprintf("FIELD_%03d", f)
	}

	rng := rand.New(rand.NewSource(42))
	value := func() []byte {
		buf := make([]byte, 0, 24)
		now := uint64(time.Now().UnixNano())
		for i := 0; i < 8; i++ {
			buf = append(buf, byte(now>>(8*i)))
		}
		return fmt.Appendf(buf, "%d.%d", rng.Int63n(1_000_000), rng.Int63n(1000))
	}

	batch := make([]*badger.Entry, 0, batchSize+10)
	commit := func() error {
		err := db.Update(func(txn *badger.Txn) error {
			for _, e := range batch {
				if err := txn.SetEntry(e); err != nil {
					return err
				}
			}
			return nil
		})
		batch = batch[:0]
		return err
	}

	start := time.Now()
	total := 0

	checkpoint := func() {
		lsm, _ := db.Size()
		levels := db.Levels()
		base, shallowTables := -1, 0
		for _, li := range levels {
			if li.IsBaseLevel {
				base = li.Level
			}
			if li.Level == 1 || li.Level == 2 {
				shallowTables += li.NumTables
			}
		}
		fmt.Printf("checkpoint entries=%d elapsed=%s lsm_mb=%d base=L%d l1l2_tables=%d\n",
			total, time.Since(start).Round(time.Second), lsm/(1<<20), base, shallowTables)
		if lsm > bigTreeBytes {
			res.bigCheckpoints++
			if base >= 0 && (res.minBase == -1 || base < res.minBase) {
				res.minBase = base
			}
			if base >= 0 && base <= 2 {
				res.collapsedCp++
			}
			if shallowTables > 0 {
				res.shallowOccCp++
			}
		}
	}

	for row := 0; row < cfg.rows; row++ {
		secID := fmt.Sprintf("SEC%08d 12345678", row)
		prefixes := []string{
			"unknown_id:" + secID + ":",
			typePrefixes[row%len(typePrefixes)] + ":" + secID + ":",
		}
		for _, prefix := range prefixes {
			for f := 0; f < numFields; f++ {
				batch = append(batch, badger.NewEntry([]byte(prefix+fieldNames[f]), value()))
				total++
				if len(batch) >= batchSize {
					if err := commit(); err != nil {
						return res, fmt.Errorf("commit: %w", err)
					}
				}
				if total%checkpointEvery == 0 {
					checkpoint()
				}
			}
		}
		for i := 0; i < 4; i++ {
			suffix := fmt.Sprintf(":%010d:ID_%s", rng.Int63n(10_000_000_000), typePrefixes[0])
			batch = append(batch,
				badger.NewEntry([]byte(typePrefixes[(row+i+1)%len(typePrefixes)]+suffix), []byte(secID)))
			total++
			if len(batch) >= batchSize {
				if err := commit(); err != nil {
					return res, fmt.Errorf("commit: %w", err)
				}
			}
		}
	}
	if len(batch) > 0 {
		if err := commit(); err != nil {
			return res, fmt.Errorf("commit: %w", err)
		}
	}

	res.wall = time.Since(start)
	res.entries = total
	res.lsmBytes, _ = db.Size()
	res.finalBase = baseLevel(db)
	if res.lsmBytes > bigTreeBytes && res.finalBase >= 0 &&
		(res.minBase == -1 || res.finalBase < res.minBase) {
		res.minBase = res.finalBase
	}

	// Close prints the lifetime L0 stall line, which stallLogger captures.
	if err := db.Close(); err != nil {
		return res, fmt.Errorf("close: %w", err)
	}
	logger.mu.Lock()
	res.stall = logger.stall
	res.slowCompactures = logger.compacts
	logger.mu.Unlock()

	return res, nil
}

// gate applies the enabled gates and returns an error listing every failure.
func gate(cfg config, res results) error {
	var failures []string
	if cfg.minBase > 0 && res.minBase != -1 && res.minBase < cfg.minBase {
		failures = append(failures, fmt.Sprintf(
			"base level collapsed: min base on a >2GB tree was L%d, want >= L%d (issue #2327 signature)",
			res.minBase, cfg.minBase))
	}
	if cfg.maxStall > 0 && res.stall > cfg.maxStall {
		failures = append(failures, fmt.Sprintf(
			"lifetime L0 stall %s exceeds budget %s (issue #2327 signature)",
			res.stall, cfg.maxStall))
	}
	if cfg.maxShallowOcc >= 0 && res.shallowOccCp > cfg.maxShallowOcc {
		failures = append(failures, fmt.Sprintf(
			"tables stranded in L1/L2 at %d of %d big-tree checkpoints, want <= %d "+
				"(healthy bulk loads never place data in L1/L2)",
			res.shallowOccCp, res.bigCheckpoints, cfg.maxShallowOcc))
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d gate(s) failed:\n  - %s", len(failures), strings.Join(failures, "\n  - "))
	}
	return nil
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.dir, "dir", "", "data directory (required; must be empty)")
	flag.IntVar(&cfg.rows, "rows", 500_000, "rows to load (~604 entries per row)")
	flag.IntVar(&cfg.minBase, "min-base", 0, "fail if base level on a >2GB tree drops below this level (0 = disabled)")
	flag.DurationVar(&cfg.maxStall, "max-stall", 0, "fail if lifetime L0 stall exceeds this (0 = disabled)")
	flag.IntVar(&cfg.maxShallowOcc, "max-shallow-occupied", -1,
		"fail if more than this many big-tree checkpoints had tables in L1/L2 (-1 = disabled)")
	flag.Parse()

	if cfg.dir == "" {
		log.Fatal("-dir is required")
	}

	res, err := run(cfg)
	if err != nil {
		log.Fatalf("lsmbench: %v", err)
	}

	fmt.Printf("slow compactions (>2s, by level pair): %v\n", res.slowCompactures)
	fmt.Printf("LSMBENCH_RESULT wall_ms=%d stall_ms=%d min_base=%d final_base=%d "+
		"collapsed_cp=%d shallow_occ_cp=%d big_cp=%d lsm_mb=%d entries=%d\n",
		res.wall.Milliseconds(), res.stall.Milliseconds(), res.minBase, res.finalBase,
		res.collapsedCp, res.shallowOccCp, res.bigCheckpoints,
		res.lsmBytes/(1<<20), res.entries)

	if err := gate(cfg, res); err != nil {
		log.Fatalf("lsmbench: %v", err)
	}
	fmt.Println("lsmbench: all enabled gates passed")
	_ = os.Stdout.Sync()
}
