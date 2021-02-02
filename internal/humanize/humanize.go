// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package humanize

import (
	"fmt"
	"math"
)

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

func humanate(s uint64, base float64, suffixes []string) string {
	if s < 10 {
		return fmt.Sprintf("%d%s", s, suffixes[0])
	}
	e := math.Floor(logn(float64(s), base))
	suffix := suffixes[int(e)]
	val := math.Floor(float64(s)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}

	return fmt.Sprintf(f, val, suffix)
}

type config struct {
	base   float64
	suffix []string
}

// IEC produces human readable representations of integer values in IEC units.
var IEC = config{1024, []string{" B", " K", " M", " G", " T", " P", " E"}}

// SI produces human readable representations of integer values in SI units.
var SI = config{1000, []string{"", " K", " M", " G", " T", " P", " E"}}

// Int64 produces a human readable representation of the value.
func (c *config) Int64(s int64) string {
	if s < 0 {
		return "-" + humanate(uint64(-s), c.base, c.suffix)
	}
	return humanate(uint64(s), c.base, c.suffix)
}

// Uint64 produces a human readable representation of the value.
func (c *config) Uint64(s uint64) string {
	return humanate(s, c.base, c.suffix)
}

// Int64 produces a human readable representation of the value in IEC units
// (base 1024).
func Int64(s int64) string {
	return IEC.Int64(s)
}

// Uint64 produces a human readable representation of the value in IEC units
// (base 1024).
func Uint64(s uint64) string {
	return IEC.Uint64(s)
}
