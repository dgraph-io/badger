/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
)

// RateLimiter is a byte-rate IO limiter implemented as a token bucket. It is
// used to pace compaction (and optionally value-log GC) so that a compaction
// or GC storm cannot saturate disk IO and starve foreground reads.
//
// A background goroutine refills the bucket every refillPeriod, adding
// ratePerSec*refillPeriod worth of byte-tokens (with sub-tick fractional
// carry so that low rates are not rounded down to zero). The bucket is capped
// at burst bytes. Callers consume tokens via Request(n), which blocks until n
// byte-tokens have been granted.
//
// # Burst / deadlock edge
//
// A single Request(n) may ask for more bytes than the burst capacity (e.g. a
// large value during compaction). To avoid deadlocking forever waiting for
// availableBytes >= n (which can never happen when n > burst), Request drains
// the bucket in a loop: each iteration it waits for at least one available
// token, then grants min(available, remaining). This makes any n satisfiable
// regardless of burst size, and cannot livelock as long as the refill
// goroutine keeps adding tokens. (Confirmed against GPT-5.)
//
// A nil *RateLimiter is a valid, disabled limiter: Request and Close are
// no-ops with zero overhead. This is the path taken when the feature is
// turned off (CompactionBytesPerSec == 0).
//
// # v2: auto-tuning (NOT implemented)
//
// The MVP is a fixed-rate, single-FIFO token bucket. A future v2 would make
// the rate adaptive using a drain-ratio controller:
//
//   - Each refill interval, record whether the bucket fully drained (i.e. a
//     waiter was blocked on an empty bucket during the interval).
//   - Maintain the drain ratio = fraction of recent intervals that fully
//     drained.
//   - If drainRatio >= 0.9 (compaction is consistently IO-bound and starved),
//     increase the rate: rate *= kInc, capped at a configured maximum.
//   - If drainRatio < 0.5 (compaction is not pressuring IO), decrease the
//     rate: rate /= kDec, floored at maxRate/100.
//
// This lets the limiter back off when foreground load is light and tighten
// when compaction would otherwise dominate, without an operator hand-tuning
// CompactionBytesPerSec. It is deliberately out of scope for the MVP.
type RateLimiter struct {
	sync.Mutex
	cond *sync.Cond

	availableBytes int64 // current tokens in the bucket, in bytes; <= burst.
	ratePerSec     int64 // refill rate in bytes/sec.
	burst          int64 // bucket capacity in bytes; also caps a single grant slice.
	refillPeriod   time.Duration

	closer *z.Closer
	closed bool
}

const rateLimiterRefillPeriod = 100 * time.Millisecond

// NewRateLimiter creates a token-bucket rate limiter that grants ratePerSec
// bytes per second. oneBlock is the size of a single SSTable block (or any
// sensible minimum unit) and is used as a floor for the burst capacity so that
// the bucket can always hold at least one block worth of tokens.
//
// If ratePerSec <= 0 the limiter is disabled and a nil *RateLimiter is
// returned; callers may use it directly (Request/Close are no-ops on nil).
func NewRateLimiter(ratePerSec int64, oneBlock int64) *RateLimiter {
	if ratePerSec <= 0 {
		return nil
	}
	if oneBlock <= 0 {
		oneBlock = 4 * 1024
	}
	// Burst = max(ratePerSec/10, oneBlock). Loop-draining in Request means the
	// burst does NOT need to be >= the largest record; it only bounds how much
	// IO can be issued in a single burst.
	burst := ratePerSec / 10
	if burst < oneBlock {
		burst = oneBlock
	}
	rl := &RateLimiter{
		availableBytes: burst, // start full so the first request is not delayed.
		ratePerSec:     ratePerSec,
		burst:          burst,
		refillPeriod:   rateLimiterRefillPeriod,
		closer:         z.NewCloser(1),
	}
	rl.cond = sync.NewCond(&rl.Mutex)
	go rl.refillLoop()
	return rl
}

func (rl *RateLimiter) refillLoop() {
	defer rl.closer.Done()
	ticker := time.NewTicker(rl.refillPeriod)
	defer ticker.Stop()

	// perTick is the whole-byte tokens added each tick; carry accumulates the
	// fractional remainder so low rates are not rounded down to zero.
	periodNanos := rl.refillPeriod.Nanoseconds()
	var carryNum int64 // numerator of carried fractional tokens, over 1e9.
	for {
		select {
		case <-rl.closer.HasBeenClosed():
			rl.Lock()
			rl.closed = true
			rl.cond.Broadcast() // wake any waiters so they can return.
			rl.Unlock()
			return
		case <-ticker.C:
			// tokens = ratePerSec * periodNanos / 1e9, carrying the remainder.
			total := rl.ratePerSec*periodNanos + carryNum
			add := total / 1e9
			carryNum = total % 1e9

			rl.Lock()
			rl.availableBytes += add
			if rl.availableBytes > rl.burst {
				rl.availableBytes = rl.burst
			}
			// Broadcast on every refill: simple and correct for the MVP. Any
			// waiter blocked on an empty bucket gets a chance to make progress.
			rl.cond.Broadcast()
			rl.Unlock()
		}
	}
}

// Request blocks until n byte-tokens have been granted, then returns. It
// charges n as actual bytes. A nil limiter (disabled) returns immediately.
//
// Request drains the bucket in a loop so that any n is satisfiable regardless
// of burst capacity (see the type doc for the deadlock rationale). If the
// limiter is closed while waiting, Request returns early with whatever it has
// already consumed; correctness of the caller is unaffected because the
// limiter only paces IO, it does not gate it.
func (rl *RateLimiter) Request(n int) {
	if rl == nil || n <= 0 {
		return
	}
	remaining := int64(n)
	rl.Lock()
	defer rl.Unlock()
	for remaining > 0 {
		for rl.availableBytes <= 0 && !rl.closed {
			rl.cond.Wait()
		}
		if rl.closed {
			return
		}
		grant := rl.availableBytes
		if grant > remaining {
			grant = remaining
		}
		rl.availableBytes -= grant
		remaining -= grant
	}
}

// Close stops the refill goroutine and wakes any blocked callers. It is safe
// to call on a nil limiter.
func (rl *RateLimiter) Close() {
	if rl == nil {
		return
	}
	rl.closer.SignalAndWait()
}
