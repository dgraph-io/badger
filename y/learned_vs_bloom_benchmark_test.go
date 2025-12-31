/*
 * Benchmark Comparison: Learned Index vs Bloom Filter
 *
 * This file contains benchmarks and tests to compare the performance
 * characteristics of learned indexes (linear regression) vs traditional
 * Bloom filters for SSTable key lookups.
 *
 * Run with: go test -v -run TestCompare ./y/
 * Run benchmarks: go test -bench=BenchmarkCompare -benchmem ./y/
 */

package y

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// ============================================================================
// COMPARISON TEST - Run this to see the differences
// ============================================================================

// TestCompareLearnedIndexVsBloomFilter demonstrates the key differences
// between learned indexes and Bloom filters.
//
// Run with: go test -v -run TestCompareLearnedIndexVsBloomFilter ./y/
func TestCompareLearnedIndexVsBloomFilter(t *testing.T) {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("  LEARNED INDEX vs BLOOM FILTER COMPARISON")
	fmt.Println(strings.Repeat("=", 70))

	// Test configurations
	keyCounts := []int{1000, 10000, 100000}
	numBlocks := 100 // Simulating 100 blocks per SSTable

	for _, keyCount := range keyCounts {
		fmt.Printf("\n%s\n", strings.Repeat("-", 70))
		fmt.Printf("  TEST WITH %d KEYS (%d keys per block)\n", keyCount, keyCount/numBlocks)
		fmt.Printf("%s\n", strings.Repeat("-", 70))

		// Generate test data (simulating sorted SSTable keys)
		keys := generateSortedKeys(keyCount)
		hashes := make([]uint32, keyCount)
		blockIndices := make([]uint32, keyCount)
		keysPerBlock := keyCount / numBlocks

		for i, key := range keys {
			hashes[i] = Hash(key)
			blockIndices[i] = uint32(i / keysPerBlock)
			if blockIndices[i] >= uint32(numBlocks) {
				blockIndices[i] = uint32(numBlocks - 1)
			}
		}

		// ========== BUILD TIME COMPARISON ==========
		fmt.Println("\n  📊 BUILD TIME:")

		// Bloom Filter build time
		bloomStart := time.Now()
		bitsPerKey := BloomBitsPerKey(keyCount, 0.01) // 1% false positive rate
		bloomFilter := NewFilter(hashes, bitsPerKey)
		bloomBuildTime := time.Since(bloomStart)

		// Learned Index build time
		learnedStart := time.Now()
		learnedIndex := TrainLearnedIndex(hashes, blockIndices, numBlocks)
		learnedBuildTime := time.Since(learnedStart)

		fmt.Printf("     Bloom Filter:  %v\n", bloomBuildTime)
		fmt.Printf("     Learned Index: %v\n", learnedBuildTime)
		if learnedBuildTime < bloomBuildTime {
			fmt.Printf("     ✅ Learned Index is %.2fx FASTER to build\n",
				float64(bloomBuildTime)/float64(learnedBuildTime))
		} else {
			fmt.Printf("     ⚠️  Bloom Filter is %.2fx faster to build\n",
				float64(learnedBuildTime)/float64(bloomBuildTime))
		}

		// ========== STORAGE SIZE COMPARISON ==========
		fmt.Println("\n  💾 STORAGE SIZE:")
		bloomSize := len(bloomFilter)
		learnedSize := LearnedIndexSize // 32 bytes

		fmt.Printf("     Bloom Filter:  %d bytes\n", bloomSize)
		fmt.Printf("     Learned Index: %d bytes\n", learnedSize)
		fmt.Printf("     ✅ Learned Index is %.1fx SMALLER\n",
			float64(bloomSize)/float64(learnedSize))

		// ========== LOOKUP TIME COMPARISON ==========
		fmt.Println("\n  🔍 LOOKUP TIME (1000 lookups):")

		numLookups := 1000
		lookupHashes := make([]uint32, numLookups)
		for i := 0; i < numLookups; i++ {
			lookupHashes[i] = hashes[rand.Intn(len(hashes))]
		}

		// Bloom Filter lookup time
		bloomLookupStart := time.Now()
		for _, h := range lookupHashes {
			_ = Filter(bloomFilter).MayContain(h)
		}
		bloomLookupTime := time.Since(bloomLookupStart)

		// Learned Index lookup time
		learnedLookupStart := time.Now()
		for _, h := range lookupHashes {
			_, _, _ = learnedIndex.Predict(h)
		}
		learnedLookupTime := time.Since(learnedLookupStart)

		fmt.Printf("     Bloom Filter:  %v (%.0f ns/lookup)\n",
			bloomLookupTime, float64(bloomLookupTime.Nanoseconds())/float64(numLookups))
		fmt.Printf("     Learned Index: %v (%.0f ns/lookup)\n",
			learnedLookupTime, float64(learnedLookupTime.Nanoseconds())/float64(numLookups))

		// ========== ACCURACY COMPARISON ==========
		fmt.Println("\n  🎯 ACCURACY:")

		// Bloom Filter: Check false positive rate
		falsePositives := 0
		nonExistentChecks := 1000
		for i := 0; i < nonExistentChecks; i++ {
			fakeHash := rand.Uint32()
			if Filter(bloomFilter).MayContain(fakeHash) {
				falsePositives++
			}
		}
		bloomFPRate := float64(falsePositives) / float64(nonExistentChecks) * 100

		// Learned Index: Check prediction accuracy
		totalError := 0
		correctPredictions := 0
		for i := 0; i < len(hashes); i++ {
			_, minB, maxB := learnedIndex.Predict(hashes[i])
			actualBlock := int(blockIndices[i])
			if actualBlock >= minB && actualBlock <= maxB {
				correctPredictions++
			}
			// Calculate how far off the prediction was
			searchRange := maxB - minB + 1
			totalError += searchRange
		}
		avgSearchRange := float64(totalError) / float64(len(hashes))
		accuracy := float64(correctPredictions) / float64(len(hashes)) * 100

		fmt.Printf("     Bloom Filter:\n")
		fmt.Printf("       - False Positive Rate: %.1f%% (target: 1%%)\n", bloomFPRate)
		fmt.Printf("       - Can filter out: %.1f%% of non-existent keys\n", 100-bloomFPRate)
		fmt.Printf("     Learned Index:\n")
		fmt.Printf("       - Prediction Accuracy: %.1f%%\n", accuracy)
		fmt.Printf("       - Average Search Range: %.1f blocks (out of %d)\n",
			avgSearchRange, numBlocks)
		fmt.Printf("       - Search Space Reduction: %.1f%%\n",
			(1-avgSearchRange/float64(numBlocks))*100)

		// ========== SUMMARY ==========
		fmt.Println("\n  📋 SUMMARY:")
		fmt.Printf("     Storage Savings:     %.0fx smaller\n",
			float64(bloomSize)/float64(learnedSize))
		fmt.Printf("     Search Optimization: Narrows to %.1f%% of blocks\n",
			avgSearchRange/float64(numBlocks)*100)
	}

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("  KEY INSIGHTS:")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("")
	fmt.Println("  1. STORAGE: Learned Index uses only 32 bytes vs hundreds/thousands for Bloom")
	fmt.Println("")
	fmt.Println("  2. SEMANTICS DIFFERENCE:")
	fmt.Println("     - Bloom Filter: \"Key is DEFINITELY NOT here\" or \"Key MIGHT be here\"")
	fmt.Println("     - Learned Index: \"Key is probably around block X, search range [A,B]\"")
	fmt.Println("")
	fmt.Println("  3. USE CASE:")
	fmt.Println("     - Bloom Filter: Filters out tables that don't have a key")
	fmt.Println("     - Learned Index: Speeds up search WITHIN a table by predicting position")
	fmt.Println("")
	fmt.Println("  4. TRADE-OFF:")
	fmt.Println("     - Bloom Filter can skip entire tables (saves I/O)")
	fmt.Println("     - Learned Index reduces binary search range (faster in-table lookup)")
	fmt.Println("")
}

// ============================================================================
// DETAILED BENCHMARKS
// ============================================================================

// BenchmarkCompareBloomBuild measures Bloom filter construction time
func BenchmarkCompareBloomBuild(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		hashes := make([]uint32, size)
		for i := 0; i < size; i++ {
			hashes[i] = rand.Uint32()
		}
		bitsPerKey := BloomBitsPerKey(size, 0.01)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				NewFilter(hashes, bitsPerKey)
			}
		})
	}
}

// BenchmarkCompareLearnedBuild measures Learned Index construction time
func BenchmarkCompareLearnedBuild(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	numBlocks := 100

	for _, size := range sizes {
		hashes := make([]uint32, size)
		blocks := make([]uint32, size)
		keysPerBlock := size / numBlocks
		for i := 0; i < size; i++ {
			hashes[i] = rand.Uint32()
			blocks[i] = uint32(i / keysPerBlock)
		}

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				TrainLearnedIndex(hashes, blocks, numBlocks)
			}
		})
	}
}

// BenchmarkCompareBloomLookup measures Bloom filter lookup time
func BenchmarkCompareBloomLookup(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		hashes := make([]uint32, size)
		for i := 0; i < size; i++ {
			hashes[i] = rand.Uint32()
		}
		bitsPerKey := BloomBitsPerKey(size, 0.01)
		filter := NewFilter(hashes, bitsPerKey)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = Filter(filter).MayContain(rand.Uint32())
			}
		})
	}
}

// BenchmarkCompareLearnedLookup measures Learned Index lookup time
func BenchmarkCompareLearnedLookup(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	numBlocks := 100

	for _, size := range sizes {
		hashes := make([]uint32, size)
		blocks := make([]uint32, size)
		keysPerBlock := size / numBlocks
		for i := 0; i < size; i++ {
			hashes[i] = rand.Uint32()
			blocks[i] = uint32(i / keysPerBlock)
		}
		li := TrainLearnedIndex(hashes, blocks, numBlocks)

		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				li.Predict(rand.Uint32())
			}
		})
	}
}

// BenchmarkCompareStorageSize measures memory usage
func BenchmarkCompareStorageSize(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	numBlocks := 100

	fmt.Println("\n📦 Storage Size Comparison:")
	fmt.Println(strings.Repeat("-", 50))
	fmt.Printf("%-15s %-15s %-15s %-10s\n", "Keys", "Bloom Size", "Learned Size", "Ratio")
	fmt.Println(strings.Repeat("-", 50))

	for _, size := range sizes {
		hashes := make([]uint32, size)
		blocks := make([]uint32, size)
		keysPerBlock := size / numBlocks
		for i := 0; i < size; i++ {
			hashes[i] = rand.Uint32()
			blocks[i] = uint32(i / keysPerBlock)
		}

		bitsPerKey := BloomBitsPerKey(size, 0.01)
		bloomFilter := NewFilter(hashes, bitsPerKey)
		bloomSize := len(bloomFilter)

		learnedSize := LearnedIndexSize

		fmt.Printf("%-15d %-15d %-15d %-10.1fx\n",
			size, bloomSize, learnedSize, float64(bloomSize)/float64(learnedSize))
	}
	fmt.Println(strings.Repeat("-", 50))
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// generateSortedKeys creates a slice of sorted byte keys
func generateSortedKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		// Generate lexicographically sorted keys
		keys[i] = []byte(fmt.Sprintf("key_%010d", i))
	}
	return keys
}

// TestPrintLearnedIndexDetails shows detailed learned index statistics
func TestPrintLearnedIndexDetails(t *testing.T) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("  LEARNED INDEX MODEL DETAILS")
	fmt.Println(strings.Repeat("=", 60))

	keyCount := 10000
	numBlocks := 100
	keysPerBlock := keyCount / numBlocks

	keys := generateSortedKeys(keyCount)
	hashes := make([]uint32, keyCount)
	blockIndices := make([]uint32, keyCount)

	for i, key := range keys {
		hashes[i] = Hash(key)
		blockIndices[i] = uint32(i / keysPerBlock)
	}

	li := TrainLearnedIndex(hashes, blockIndices, numBlocks)

	fmt.Printf("\n  Model Parameters:\n")
	fmt.Printf("    Slope:     %e\n", li.Slope)
	fmt.Printf("    Intercept: %f\n", li.Intercept)
	fmt.Printf("    Min Error: %d blocks\n", li.MinErr)
	fmt.Printf("    Max Error: %d blocks\n", li.MaxErr)
	fmt.Printf("    Key Count: %d\n", li.KeyCount)
	fmt.Printf("    Max Block: %d\n", li.MaxPos)

	fmt.Printf("\n  Interpretation:\n")
	fmt.Printf("    - Linear model: block = %.2e × hash + %.2f\n", li.Slope, li.Intercept)
	fmt.Printf("    - Search range: predicted_block + [%d, %d]\n", li.MinErr, li.MaxErr)
	fmt.Printf("    - Total range:  %d blocks to search\n", li.MaxErr-li.MinErr+1)
	fmt.Printf("    - Reduction:    Search %.1f%% of table instead of 100%%\n",
		float64(li.MaxErr-li.MinErr+1)/float64(numBlocks)*100)

	// Show sample predictions
	fmt.Printf("\n  Sample Predictions:\n")
	fmt.Printf("    %-20s %-10s %-10s %-15s\n", "Key", "Actual", "Predicted", "Range")
	fmt.Printf("    %s\n", strings.Repeat("-", 55))

	samples := []int{0, keyCount / 4, keyCount / 2, 3 * keyCount / 4, keyCount - 1}
	for _, idx := range samples {
		pred, minB, maxB := li.Predict(hashes[idx])
		fmt.Printf("    %-20s %-10d %-10d [%d, %d]\n",
			string(keys[idx]), blockIndices[idx], pred, minB, maxB)
	}
}
