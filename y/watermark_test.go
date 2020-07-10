package y

import (
	"sync"
	"testing"
)

func BenchmarkWatermark(b *testing.B) {
	w := &WaterMark{}
	var c Closer
	c.AddRunning(1)
	w.Init(&c)

	var wg sync.WaitGroup
	for k := 0; k < 32; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				w.Begin(1)
				w.Done(1)
			}
		}()
	}

	wg.Wait()
}
