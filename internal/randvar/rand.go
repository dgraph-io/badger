package randvar

import (
	"time"

	"golang.org/x/exp/rand"
)

// NewRand creates a new random number generator seeded with the current time.
func NewRand() *rand.Rand {
	return rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
}

func ensureRand(rng *rand.Rand) *rand.Rand {
	if rng != nil {
		return rng
	}
	return NewRand()
}
