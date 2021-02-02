// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package randvar

import (
	"sync/atomic"

	"golang.org/x/exp/rand"
)

// Uniform is a random number generator that generates draws from a uniform
// distribution.
type Uniform struct {
	min uint64
	max uint64
}

// NewUniform constructs a new Uniform generator with the given
// parameters. Returns an error if the parameters are outside the accepted
// range.
func NewUniform(min, max uint64) *Uniform {
	return &Uniform{min: min, max: max}
}

// IncMax increments max.
func (g *Uniform) IncMax(delta int) {
	atomic.AddUint64(&g.max, uint64(delta))
}

// Uint64 returns a random Uint64 between min and max, drawn from a uniform
// distribution.
func (g *Uniform) Uint64(rng *rand.Rand) uint64 {
	max := atomic.LoadUint64(&g.max)
	return rng.Uint64n(max-g.min+1) + g.min
}
