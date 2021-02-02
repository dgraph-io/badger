// The algorithm is CRC-32 with Castagnoli's polynomial, followed by a bit
// rotation and an additional delta. The additional processing is to lessen the
// probability of arbitrary key/value data coincidentally containing bytes that
// look like a checksum.
//
// To calculate the uint32 checksum of some data:
//	var u uint32 = crc.New(data).Value()
package crc

import "hash/crc32"

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

// New returns the result of adding the bytes to the zero-value CRC.
func New(b []byte) CRC {
	return CRC(0).Update(b)
}

// Update returns the result of adding the bytes to the CRC.
func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

// Value returns the cooked CRC value. The additional processing is to lessen
// the probability of arbitrary key/value data coincidentally containing bytes
// that look like a checksum.
func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}
