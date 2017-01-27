package value

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkReadWrite(b *testing.B) {
	rwRatio := []float32{
		0.1, 0.2, 0.5, 1.0,
	}
	valueSize := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}

	for _, vsz := range valueSize {
		for _, rw := range rwRatio {
			b.Run(fmt.Sprintf("%3.1f,%04d", rw, vsz), func(b *testing.B) {
				var vl Log
				vl.Open("vlog")
				defer os.Remove("vlog")
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {
					var e Entry
					e.Key = make([]byte, 16)
					e.Value = make([]byte, vsz)

					var ptrs []Pointer

					pt, err := vl.Write([]Entry{e})
					if err != nil {
						b.Fatalf("Benchmark Write: ", err)
					}
					ptrs = append(ptrs, pt...)

					for pb.Next() {
						f := rand.Float32()
						if f < rw {
							pt, err := vl.Write([]Entry{e})
							if err != nil {
								b.Fatalf("Benchmark Write: ", err)
							}
							ptrs = append(ptrs, pt...)

						} else {
							ln := len(ptrs)
							if ln == 0 {
								b.Fatalf("Zero length of ptrs")
							}
							idx := rand.Intn(ln)
							p := ptrs[idx]
							if err := vl.Read(p.Offset, p.Len, func(e Entry) {
								if len(e.Key) != 16 {
									b.Fatalf("Key is invalid")
								}
								if len(e.Value) != vsz {
									b.Fatalf("Value is invalid")
								}
							}); err != nil {
								b.Fatalf("Benchmark Read:", err)
							}
						}
					}
				})
			})
		}
	}
}
