// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bitmap

import "unsafe"

// Bitmap is a helper struct that tracks whether a particular bit has been
// "set".
type Bitmap struct {
	bits []byte
}

// NewBitmap returns a new Bitmap that supports [0, n) range of integers. n is
// expected to be positive.
func NewBitmap(n int) *Bitmap {
	return &Bitmap{bits: make([]byte, (n-1)/8+1)}
}

// bitMask[i] is a byte with a single bit set at ith position.
var bitMask = [8]byte{0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80}

// Set sets the ith bit in the Bitmap.
func (bm *Bitmap) Set(i int) {
	bm.bits[i>>3] |= bitMask[i&7]
}

// IsSet returns true if the ith bit is set in the Bitmap.
func (bm *Bitmap) IsSet(i int) bool {
	return bm.bits[i>>3]&bitMask[i&7] != 0
}

var bitmapOverhead = int64(unsafe.Sizeof(Bitmap{}))

// MemUsage returns the memory footprint of the Bitmap in bytes. Safe to be
// called on nil Bitmap.
func (bm *Bitmap) MemUsage() int64 {
	if bm == nil {
		return 0
	}
	return bitmapOverhead + int64(cap(bm.bits))
}
