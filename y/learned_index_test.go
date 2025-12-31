/*
 * Tests for the Learned Index implementation
 */

package y

import (
	"math/rand"
	"testing"
)

func TestLearnedIndexEmpty(t *testing.T) {
	// Test with empty data
	li := TrainLearnedIndex(nil, nil, 0)
	if li == nil {
		t.Fatal("Expected non-nil learned index")
	}
	if li.KeyCount != 0 {
		t.Errorf("Expected KeyCount=0, got %d", li.KeyCount)
	}

	// Prediction should return full range
	_, minB, maxB := li.Predict(12345)
	if minB != 0 || maxB != 0 {
		t.Errorf("Expected range [0,0] for empty index, got [%d,%d]", minB, maxB)
	}
}

func TestLearnedIndexSingleKey(t *testing.T) {
	hashes := []uint32{Hash([]byte("key1"))}
	blocks := []uint32{5}

	li := TrainLearnedIndex(hashes, blocks, 10)
	if li.KeyCount != 1 {
		t.Errorf("Expected KeyCount=1, got %d", li.KeyCount)
	}

	// Should predict the exact block
	pred, minB, maxB := li.Predict(hashes[0])
	if pred != 5 {
		t.Errorf("Expected predicted=5, got %d", pred)
	}
	if minB > 5 || maxB < 5 {
		t.Errorf("Expected range to include 5, got [%d,%d]", minB, maxB)
	}
}

func TestLearnedIndexLinearData(t *testing.T) {
	// Create perfectly linear data: hash maps linearly to block
	n := 100
	hashes := make([]uint32, n)
	blocks := make([]uint32, n)

	for i := 0; i < n; i++ {
		hashes[i] = uint32(i * 1000) // Linear hashes
		blocks[i] = uint32(i)        // Linear blocks
	}

	li := TrainLearnedIndex(hashes, blocks, n)
	if li.KeyCount != uint32(n) {
		t.Errorf("Expected KeyCount=%d, got %d", n, li.KeyCount)
	}

	// Test predictions - should be very accurate for linear data
	for i := 0; i < n; i++ {
		pred, minB, maxB := li.Predict(hashes[i])
		expected := i
		if pred != expected {
			t.Errorf("For hash %d: expected pred=%d, got %d", hashes[i], expected, pred)
		}
		if minB > expected || maxB < expected {
			t.Errorf("For hash %d: expected range to include %d, got [%d,%d]",
				hashes[i], expected, minB, maxB)
		}
	}
}

func TestLearnedIndexRealisticData(t *testing.T) {
	// Simulate realistic SSTable: sorted keys hashed, assigned to blocks
	n := 1000
	numBlocks := 50
	keysPerBlock := n / numBlocks

	hashes := make([]uint32, n)
	blocks := make([]uint32, n)

	// Generate sorted keys and hash them
	for i := 0; i < n; i++ {
		// Keys are sorted, but hashes won't be
		key := make([]byte, 8)
		key[0] = byte(i >> 24)
		key[1] = byte(i >> 16)
		key[2] = byte(i >> 8)
		key[3] = byte(i)
		hashes[i] = Hash(key)
		blocks[i] = uint32(i / keysPerBlock)
	}

	li := TrainLearnedIndex(hashes, blocks, numBlocks)
	t.Logf("Learned index: slope=%f, intercept=%f, minErr=%d, maxErr=%d",
		li.Slope, li.Intercept, li.MinErr, li.MaxErr)
	t.Logf("Error range: %d blocks", li.ErrorRange())

	// Verify that all keys are found within predicted bounds
	for i := 0; i < n; i++ {
		_, minB, maxB := li.Predict(hashes[i])
		actualBlock := int(blocks[i])
		if actualBlock < minB || actualBlock > maxB {
			t.Errorf("Key %d: actual block %d not in predicted range [%d,%d]",
				i, actualBlock, minB, maxB)
		}
	}
}

func TestLearnedIndexSerializationRoundtrip(t *testing.T) {
	hashes := []uint32{100, 200, 300, 400, 500}
	blocks := []uint32{0, 1, 2, 3, 4}

	original := TrainLearnedIndex(hashes, blocks, 5)

	// Serialize and deserialize
	data := original.Serialize()
	if len(data) != LearnedIndexSize {
		t.Errorf("Expected serialized size %d, got %d", LearnedIndexSize, len(data))
	}

	restored := DeserializeLearnedIndex(data)
	if restored == nil {
		t.Fatal("Failed to deserialize")
	}

	// Verify all fields match
	if restored.Slope != original.Slope {
		t.Errorf("Slope mismatch: %f vs %f", restored.Slope, original.Slope)
	}
	if restored.Intercept != original.Intercept {
		t.Errorf("Intercept mismatch: %f vs %f", restored.Intercept, original.Intercept)
	}
	if restored.MinErr != original.MinErr {
		t.Errorf("MinErr mismatch: %d vs %d", restored.MinErr, original.MinErr)
	}
	if restored.MaxErr != original.MaxErr {
		t.Errorf("MaxErr mismatch: %d vs %d", restored.MaxErr, original.MaxErr)
	}
	if restored.KeyCount != original.KeyCount {
		t.Errorf("KeyCount mismatch: %d vs %d", restored.KeyCount, original.KeyCount)
	}
	if restored.MaxPos != original.MaxPos {
		t.Errorf("MaxPos mismatch: %d vs %d", restored.MaxPos, original.MaxPos)
	}
}

func TestLearnedIndexBoundsClamping(t *testing.T) {
	hashes := []uint32{1000, 2000, 3000}
	blocks := []uint32{0, 5, 10}

	li := TrainLearnedIndex(hashes, blocks, 11)

	// Test prediction for hash outside training range (very high)
	_, minB, maxB := li.Predict(999999999)
	if minB < 0 {
		t.Errorf("minBlock should not be negative: %d", minB)
	}
	if maxB > 10 {
		t.Errorf("maxBlock should not exceed MaxPos (10): %d", maxB)
	}

	// Test prediction for hash outside training range (zero)
	_, minB, maxB = li.Predict(0)
	if minB < 0 {
		t.Errorf("minBlock should not be negative: %d", minB)
	}
	if maxB > 10 {
		t.Errorf("maxBlock should not exceed MaxPos (10): %d", maxB)
	}
}

func BenchmarkLearnedIndexTrain(b *testing.B) {
	n := 10000
	numBlocks := 500
	hashes := make([]uint32, n)
	blocks := make([]uint32, n)

	for i := 0; i < n; i++ {
		hashes[i] = rand.Uint32()
		blocks[i] = uint32(i / (n / numBlocks))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TrainLearnedIndex(hashes, blocks, numBlocks)
	}
}

func BenchmarkLearnedIndexPredict(b *testing.B) {
	n := 10000
	numBlocks := 500
	hashes := make([]uint32, n)
	blocks := make([]uint32, n)

	for i := 0; i < n; i++ {
		hashes[i] = rand.Uint32()
		blocks[i] = uint32(i / (n / numBlocks))
	}

	li := TrainLearnedIndex(hashes, blocks, numBlocks)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		li.Predict(rand.Uint32())
	}
}

func BenchmarkLearnedIndexSerialize(b *testing.B) {
	li := &LearnedIndex{
		Slope:     0.001234,
		Intercept: 50.5,
		MinErr:    -10,
		MaxErr:    15,
		KeyCount:  10000,
		MaxPos:    500,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		li.Serialize()
	}
}
