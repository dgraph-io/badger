//go:build integration

/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestBulkLoadNoBaseLevelCollapse is the integration-test entry point for the
// #2327 regression benchmark. It is excluded from normal unit-test runs by
// the "integration" build tag; run it explicitly with:
//
//	go test -v -tags=integration -timeout 60m ./integration/lsmbench/
//
// The full run writes ~302M entries (~10GB on disk) and takes ~4 minutes on a
// 12-core workstation, longer on CI runners. Knobs:
//
//	BADGER_LSMBENCH_ROWS        rows to load (default 500000)
//	BADGER_LSMBENCH_WALL_LIMIT  optional wall-clock budget, e.g. "10m"
//	                            (default: disabled — absolute wall time is
//	                            hardware-dependent; the CI workflow gates on
//	                            a baseline-vs-head ratio instead)
//
// The structural gates (base level, L0 stall) are always on: they are the
// hardware-independent signature of the #2327 collapse.
func TestBulkLoadNoBaseLevelCollapse(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running integration benchmark")
	}

	rows := 500_000
	if v := os.Getenv("BADGER_LSMBENCH_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("bad BADGER_LSMBENCH_ROWS %q", v)
		}
		rows = n
	}

	cfg := config{
		dir:           t.TempDir(),
		rows:          rows,
		minBase:       3,
		maxStall:      30 * time.Second,
		maxShallowOcc: 0,
	}

	res, err := run(cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Logf("wall=%s stall=%s min_base=%d final_base=%d collapsed_cp=%d shallow_occ_cp=%d lsm_mb=%d entries=%d",
		res.wall.Round(time.Second), res.stall, res.minBase, res.finalBase,
		res.collapsedCp, res.shallowOccCp, res.lsmBytes/(1<<20), res.entries)

	if err := gate(cfg, res); err != nil {
		t.Error(err)
	}

	if v := os.Getenv("BADGER_LSMBENCH_WALL_LIMIT"); v != "" {
		limit, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("bad BADGER_LSMBENCH_WALL_LIMIT %q", v)
		}
		if res.wall > limit {
			t.Errorf("bulk load took %s, exceeds budget %s", res.wall.Round(time.Second), limit)
		}
	}
}
