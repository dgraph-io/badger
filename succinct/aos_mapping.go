package succinct

// For now we use RoaringBitmap that is optimized for And and Or.
// Succinct uses RRR that has constant time Rank and Select.
import (
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"log"
)

type array interface {
	Lookup(idx uint32) uint32
	Length() uint32
}

type SliceWrapper struct {
	a []uint32
}

func NewSliceWrapper(slice []uint32) *SliceWrapper {
	return &SliceWrapper{a: slice}
}

func (na *SliceWrapper) Lookup(idx uint32) uint32 {
	return na.a[idx]
}

func (na *SliceWrapper) Length() uint32 {
	return uint32(len(na.a))
}

// we use rankSelect interface that matches roaring.Bitmap for now.
type rankSelect interface {
	// Rank counts the number of 1's before i+1
	// This is inconsistent with the definitions in the paper.
	// Precisely, rank(i) from paper = Rank(i - 1) but 0, rank(0) = 0
	Rank(i uint32) uint64

	// Select gives the position of the i-th 1
	Select(i uint32) (uint32, error)
}

// AoSMapping is a struct that contains aoS2Input and input2AoS
// tables.
type AoSMapping struct {
	alpha     int8
	next      array
	aoS2Input []uint32 // can use uint8 later
	bPos      *roaring.Bitmap
	input2AoS []uint32 // can use uint8 later
}

// LookupAoS2Input returns index of a suffix from AoS
// in the input file
// See Algorithm 1 in the technical report.
func (mapping *AoSMapping) LookupAoS2Input(i uint32) uint32 {
	noHops := uint32(0)
	idx1 := i
	for !mapping.bPos.Contains(idx1) {
		idx1 = mapping.next.Lookup(idx1)
		noHops++
	}
	var noBitsSet uint64
	if idx1 == 0 {
		noBitsSet = 0
	} else {
		noBitsSet = mapping.bPos.Rank(idx1 - 1)
	}
	val := mapping.aoS2Input[noBitsSet]
	return val*uint32(mapping.alpha) - noHops
}

// LookupInput2AoS returns index of the suffix in the AoS
// for a location in the input file.
// See Algorithm 2 in the technical report.
func (mapping *AoSMapping) LookupInput2AoS(loc uint32) uint32 {
	alpha := uint32(mapping.alpha)
	loc1 := alpha * (loc / alpha)
	idx1 := mapping.input2AoS[loc/alpha]
	idx, err := mapping.bPos.Select(idx1)
	if err != nil {
		log.Fatal(fmt.Errorf("lookup for %d failed", loc))
	}
	for i := uint32(1); i <= loc-loc1; i++ {
		idx = mapping.next.Lookup(idx)
	}
	return idx
}
