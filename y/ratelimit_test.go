/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRateLimiterDisabled verifies that a non-positive rate yields a nil
// (disabled) limiter whose methods are safe no-ops.
func TestRateLimiterDisabled(t *testing.T) {
	rl := NewRateLimiter(0, 4096)
	require.Nil(t, rl)

	// Nil limiter: Request and Close must not panic and must return instantly.
	done := make(chan struct{})
	go func() {
		rl.Request(1 << 30)
		rl.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("nil limiter Request/Close blocked")
	}
}

// TestRateLimiterRate verifies that requesting N bytes at rate R takes roughly
// N/R seconds. We pick N considerably larger than the burst so that the
// throttle (not the initial full bucket) dominates the timing.
func TestRateLimiterRate(t *testing.T) {
	const rate = 1 << 20 // 1 MiB/s
	rl := NewRateLimiter(rate, 4096)
	require.NotNil(t, rl)
	defer rl.Close()

	const total = 4 << 20 // 4 MiB => expect ~ (total-burst)/rate ~ 4s, allow slack.
	start := time.Now()
	// Charge in chunks to mimic per-record charging.
	const chunk = 64 << 10
	for charged := 0; charged < total; charged += chunk {
		rl.Request(chunk)
	}
	elapsed := time.Since(start)

	// Expected lower bound: (total - burst) / rate. burst = rate/10 ~ 0.1 MiB.
	secs := float64(total-rate/10) / float64(rate)
	minExpected := time.Duration(secs * float64(time.Second))
	// Allow generous slack for ticker granularity and scheduling.
	require.GreaterOrEqual(t, elapsed, time.Duration(float64(minExpected)*0.7),
		"throttle released bytes too fast: %v < %v", elapsed, minExpected)
	require.LessOrEqual(t, elapsed, minExpected+3*time.Second,
		"throttle was far too slow: %v", elapsed)
}

// TestRateLimiterBurstDeadlock verifies that a single Request for far more than
// the burst capacity is satisfiable (loop-draining) and never deadlocks.
func TestRateLimiterBurstDeadlock(t *testing.T) {
	const rate = 8 << 20 // 8 MiB/s; burst = rate/10 ~ 0.8 MiB.
	rl := NewRateLimiter(rate, 4096)
	require.NotNil(t, rl)
	defer rl.Close()

	// n is several times the burst; with a single bucket-sized wait this would
	// deadlock forever. Loop-draining must satisfy it.
	n := int(rate) // 8 MiB, 10x the burst.
	done := make(chan struct{})
	go func() {
		rl.Request(n)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Request(%d) with n >> burst deadlocked", n)
	}
}

// TestRateLimiterCloseUnblocks verifies Close wakes a blocked waiter and stops
// the refill goroutine without leaking (run with -race).
func TestRateLimiterCloseUnblocks(t *testing.T) {
	const rate = 1024 // tiny rate so the waiter blocks.
	rl := NewRateLimiter(rate, 4096)
	require.NotNil(t, rl)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rl.Request(1 << 30) // huge; will block essentially forever.
	}()

	// Give the goroutine time to block on the empty bucket.
	time.Sleep(200 * time.Millisecond)
	rl.Close()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock a waiting Request")
	}

	// Close is idempotent / safe to call again on a closed limiter? It uses
	// SignalAndWait which would double-Wait; ensure we don't call it twice.
}

// TestRateLimiterConcurrent verifies multiple concurrent requesters all make
// progress and none deadlock (fairness is best-effort for the MVP).
func TestRateLimiterConcurrent(t *testing.T) {
	const rate = 4 << 20
	rl := NewRateLimiter(rate, 4096)
	require.NotNil(t, rl)
	defer rl.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 16; j++ {
				rl.Request(32 << 10)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("concurrent requesters did not all complete")
	}
}
