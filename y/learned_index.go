/*
 * Learned Index - Linear Regression Model for SSTable key lookup
 * Replaces Bloom filters with a simple linear model: position = slope * hash(key) + intercept
 */

package y

import (
	"encoding/binary"
	"math"
)

// LearnedIndex represents a simple linear regression model for predicting
// the position (block index) of a key in a sorted SSTable.
//
// Model: predictedPosition = slope * hash(key) + intercept
// Error bounds track the maximum prediction error seen during training.
type LearnedIndex struct {
	Slope     float64 // Slope of the linear model
	Intercept float64 // Y-intercept
	MinErr    int32   // Minimum error (negative = predicted too high)
	MaxErr    int32   // Maximum error (positive = predicted too low)
	KeyCount  uint32  // Number of keys used for training
	MaxPos    uint32  // Maximum position (number of blocks - 1)
}

// LearnedIndexSize is the serialized size in bytes: 8+8+4+4+4+4 = 32 bytes
const LearnedIndexSize = 32

// TrainLearnedIndex builds a linear regression model from sorted key hashes.
// Each hash corresponds to a block index (position).
//
// Parameters:
//   - keyHashes: hash of each key (from y.Hash)
//   - blockIndices: which block each key belongs to (parallel array)
//   - numBlocks: total number of blocks in the table
//
// The model learns: blockIndex ≈ slope * keyHash + intercept
func TrainLearnedIndex(keyHashes []uint32, blockIndices []uint32, numBlocks int) *LearnedIndex {
	n := len(keyHashes)
	if n == 0 {
		return &LearnedIndex{MaxPos: uint32(max(0, numBlocks-1))}
	}

	// For single key, just store the position directly
	if n == 1 {
		return &LearnedIndex{
			Slope:     0,
			Intercept: float64(blockIndices[0]),
			MinErr:    0,
			MaxErr:    0,
			KeyCount:  1,
			MaxPos:    uint32(max(0, numBlocks-1)),
		}
	}

	// Linear regression using least squares method
	// We want to minimize: sum((y - (slope*x + intercept))^2)
	// where x = keyHash, y = blockIndex

	var sumX, sumY, sumXY, sumX2 float64
	for i := 0; i < n; i++ {
		x := float64(keyHashes[i])
		y := float64(blockIndices[i])
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	nf := float64(n)
	denominator := nf*sumX2 - sumX*sumX

	var slope, intercept float64
	if math.Abs(denominator) < 1e-10 {
		// All keys have same hash (unlikely but handle it)
		// Just predict the average position
		slope = 0
		intercept = sumY / nf
	} else {
		slope = (nf*sumXY - sumX*sumY) / denominator
		intercept = (sumY - slope*sumX) / nf
	}

	// Calculate error bounds by checking prediction error for all keys
	var minErr, maxErr int32
	for i := 0; i < n; i++ {
		predicted := slope*float64(keyHashes[i]) + intercept
		actual := float64(blockIndices[i])
		err := int32(actual - predicted) // positive if we predicted too low

		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}

	// Add small buffer to error bounds for safety
	minErr -= 1
	maxErr += 1

	return &LearnedIndex{
		Slope:     slope,
		Intercept: intercept,
		MinErr:    minErr,
		MaxErr:    maxErr,
		KeyCount:  uint32(n),
		MaxPos:    uint32(max(0, numBlocks-1)),
	}
}

// Predict returns the predicted block index for a given key hash.
// Returns (predictedBlock, minBlock, maxBlock) where the key should be
// searched in the range [minBlock, maxBlock].
func (li *LearnedIndex) Predict(keyHash uint32) (predicted, minBlock, maxBlock int) {
	if li == nil || li.KeyCount == 0 {
		// No model - search all blocks
		return 0, 0, int(li.MaxPos)
	}

	// Predict position
	pos := li.Slope*float64(keyHash) + li.Intercept
	predicted = int(math.Round(pos))

	// Apply error bounds
	minBlock = predicted + int(li.MinErr)
	maxBlock = predicted + int(li.MaxErr)

	// Clamp to valid range
	if minBlock < 0 {
		minBlock = 0
	}
	if maxBlock < 0 {
		maxBlock = 0
	}
	maxPosInt := int(li.MaxPos)
	if minBlock > maxPosInt {
		minBlock = maxPosInt
	}
	if maxBlock > maxPosInt {
		maxBlock = maxPosInt
	}
	if predicted < 0 {
		predicted = 0
	}
	if predicted > maxPosInt {
		predicted = maxPosInt
	}

	return predicted, minBlock, maxBlock
}

// MayContainInRange returns true if the key might be in this table.
// This is a probabilistic check similar to Bloom filter's MayContain.
// Unlike Bloom filters, learned index can give false negatives in rare cases
// if error bounds are exceeded, but this is very unlikely with proper training.
func (li *LearnedIndex) MayContainInRange(keyHash uint32) bool {
	if li == nil || li.KeyCount == 0 {
		// No model trained - assume key might be present
		return true
	}
	// With a learned index, we always say "may contain" because
	// we'll do a bounded search. The value is in the search efficiency,
	// not in filtering tables entirely.
	return true
}

// Serialize converts the LearnedIndex to bytes for storage.
// Format: [slope:8][intercept:8][minErr:4][maxErr:4][keyCount:4][maxPos:4] = 32 bytes
func (li *LearnedIndex) Serialize() []byte {
	buf := make([]byte, LearnedIndexSize)
	binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(li.Slope))
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(li.Intercept))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(li.MinErr))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(li.MaxErr))
	binary.LittleEndian.PutUint32(buf[24:28], li.KeyCount)
	binary.LittleEndian.PutUint32(buf[28:32], li.MaxPos)
	return buf
}

// DeserializeLearnedIndex reads a LearnedIndex from bytes.
func DeserializeLearnedIndex(data []byte) *LearnedIndex {
	if len(data) < LearnedIndexSize {
		return nil
	}
	return &LearnedIndex{
		Slope:     math.Float64frombits(binary.LittleEndian.Uint64(data[0:8])),
		Intercept: math.Float64frombits(binary.LittleEndian.Uint64(data[8:16])),
		MinErr:    int32(binary.LittleEndian.Uint32(data[16:20])),
		MaxErr:    int32(binary.LittleEndian.Uint32(data[20:24])),
		KeyCount:  binary.LittleEndian.Uint32(data[24:28]),
		MaxPos:    binary.LittleEndian.Uint32(data[28:32]),
	}
}

// ErrorRange returns the search range size (max - min error).
// Useful for statistics and debugging.
func (li *LearnedIndex) ErrorRange() int {
	if li == nil {
		return 0
	}
	return int(li.MaxErr - li.MinErr)
}
