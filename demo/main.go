package main

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type LearnedFilter struct {
	Slope     float64
	Intercept float64
	Keys      []uint64
}

// BloomFilter implementation
type BloomFilter struct {
	bits   []bool
	size   int
	hashes int // number of hash functions
}

func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	// Optimal size: m = -n*ln(p) / (ln(2))^2
	size := int(-float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2))

	// Optimal hash functions: k = (m/n) * ln(2)
	hashes := int(math.Ceil(float64(size) / float64(n) * math.Ln2))

	return &BloomFilter{
		bits:   make([]bool, size),
		size:   size,
		hashes: hashes,
	}
}

func (bf *BloomFilter) hash(key uint64, seed int) int {
	// Fast double hashing: h1(x) + i*h2(x)
	h1 := key * 0x9e3779b97f4a7c15 // Multiply by large prime
	h2 := key * 0xc6a4a7935bd1e995

	hash := h1 + uint64(seed)*h2
	return int(hash % uint64(bf.size))
}

func (bf *BloomFilter) Add(key uint64) {
	for i := 0; i < bf.hashes; i++ {
		pos := bf.hash(key, i)
		bf.bits[pos] = true
	}
}

func (bf *BloomFilter) MayContain(key uint64) bool {
	for i := 0; i < bf.hashes; i++ {
		pos := bf.hash(key, i)
		if !bf.bits[pos] {
			return false
		}
	}
	return true
}

func Train(keys []uint64) *LearnedFilter {
	n := len(keys)
	var sumX, sumY, sumXY, sumX2 float64

	for i, key := range keys {
		x := float64(key)
		y := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	nf := float64(n)
	slope := (nf*sumXY - sumX*sumY) / (nf*sumX2 - sumX*sumX)
	intercept := (sumY - slope*sumX) / nf

	return &LearnedFilter{
		Slope:     slope,
		Intercept: intercept,
		Keys:      keys,
	}
}

func (lf *LearnedFilter) Search(key uint64, window int) (int, bool) {
	pred := lf.Slope*float64(key) + lf.Intercept
	pos := int(math.Round(pred))

	if pos < 0 {
		pos = 0
	}
	if pos >= len(lf.Keys) {
		pos = len(lf.Keys) - 1
	}

	start := pos - window
	end := pos + window
	if start < 0 {
		start = 0
	}
	if end >= len(lf.Keys) {
		end = len(lf.Keys) - 1
	}

	for i := start; i <= end; i++ {
		if lf.Keys[i] == key {
			return i, true
		}
	}

	return -1, false
}

// Binary Search implementation
func BinarySearch(keys []uint64, target uint64) (int, bool) {
	left, right := 0, len(keys)-1

	for left <= right {
		mid := left + (right-left)/2

		if keys[mid] == target {
			return mid, true
		} else if keys[mid] < target {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return -1, false
}

func runBenchmark(name string, numKeys int) {
	fmt.Printf("\n╔════════════════════════════════════════╗\n")
	fmt.Printf("║  Dataset Size: %d keys%-14s║\n", numKeys, "")
	fmt.Printf("╚════════════════════════════════════════╝\n")

	// Test with sequential keys
	keys := make([]uint64, numKeys)
	for i := range keys {
		keys[i] = uint64(i * 100) // 0, 100, 200, ...
	}

	// Train learned index
	lf := Train(keys)

	// Build bloom filter (1% false positive rate)
	bf := NewBloomFilter(numKeys, 0.01)
	for _, key := range keys {
		bf.Add(key)
	}

	fmt.Println("\n=== LEARNED INDEX ===")
	// Benchmark learned index
	start := time.Now()
	hits := 0
	for i := 0; i < numKeys; i++ {
		_, found := lf.Search(uint64(i*100), 50)
		if found {
			hits++
		}
	}
	learnedTime := time.Since(start)

	fmt.Printf("Found %d/%d keys in %v\n", hits, numKeys, learnedTime)
	fmt.Printf("Avg lookup: %v\n", learnedTime/time.Duration(numKeys))
	fmt.Printf("Model: y = %.6fx + %.2f\n", lf.Slope, lf.Intercept)

	fmt.Println("\n=== BINARY SEARCH ===")
	// Benchmark binary search
	start = time.Now()
	hits = 0
	for i := 0; i < numKeys; i++ {
		_, found := BinarySearch(keys, uint64(i*100))
		if found {
			hits++
		}
	}
	binaryTime := time.Since(start)

	fmt.Printf("Found %d/%d keys in %v\n", hits, numKeys, binaryTime)
	fmt.Printf("Avg lookup: %v\n", binaryTime/time.Duration(numKeys))

	fmt.Println("\n=== BLOOM FILTER ===")
	// Benchmark bloom filter (just membership test)
	start = time.Now()
	hits = 0
	for i := 0; i < numKeys; i++ {
		if bf.MayContain(uint64(i * 100)) {
			hits++
		}
	}
	bloomTime := time.Since(start)

	fmt.Printf("Found %d/%d keys in %v\n", hits, numKeys, bloomTime)
	fmt.Printf("Avg lookup: %v\n", bloomTime/time.Duration(numKeys))
	fmt.Printf("Filter size: %.2f MB (%d bits, %d hash functions)\n",
		float64(bf.size)/(8*1024*1024), bf.size, bf.hashes)

	fmt.Println("\n=== COMPARISON ===")
	fmt.Printf("Learned Index:  %v (%.2fx vs Bloom)\n", learnedTime, float64(bloomTime)/float64(learnedTime))
	fmt.Printf("Binary Search:  %v (%.2fx vs Bloom)\n", binaryTime, float64(bloomTime)/float64(binaryTime))
	fmt.Printf("Bloom Filter:   %v (baseline)\n", bloomTime)

	// Show complexity
	fmt.Printf("\nComplexity Analysis:\n")
	fmt.Printf("  Binary Search: O(log n) = ~%.0f comparisons\n", math.Log2(float64(len(keys))))
	fmt.Printf("  Learned Index: O(1) prediction + O(window) = ~%d comparisons\n", 100)
	fmt.Printf("  Bloom Filter:  O(k) = %d hash computations\n", bf.hashes)
}

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════╗")
	fmt.Println("║  LEARNED INDEX vs BINARY SEARCH vs BLOOM FILTER      ║")
	fmt.Println("╚═══════════════════════════════════════════════════════╝")

	// Test with different dataset sizes
	runBenchmark("Small", 10000)
	runBenchmark("Large", 10000000)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("KEY INSIGHTS:")
	fmt.Println("- Bloom Filter: FASTEST but probabilistic (false positives)")
	fmt.Println("- Learned Index: Fast & exact for sequential data")
	fmt.Println("- Binary Search: Exact but slower as data grows")
	fmt.Println("\nUse Cases:")
	fmt.Println("  • Bloom Filter: Quick negative lookups (\"key NOT here\")")
	fmt.Println("  • Learned Index: Point queries on sorted data")
	fmt.Println("  • Binary Search: General-purpose exact search")
	fmt.Println(strings.Repeat("=", 60))
}
