// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

// Bytes is a wrapper type for a two-dimensional byte slice.
type Bytes struct {
	data [][]byte
}

// NewBytes returns a Bytes struct with enough capacity for n elements.
func NewBytes(n int) *Bytes {
	return &Bytes{
		data: make([][]byte, n),
	}
}

// Get returns the ith []byte in Bytes.
func (b Bytes) Get(i int) []byte {
	return b.data[i]
}

// Set sets the ith []byte in Bytes.
func (b Bytes) Set(i int, v []byte) {
	b.data[i] = v
}

// Swap swaps the ith []byte with the jth []byte and vice-versa.
func (b Bytes) Swap(i int, j int) {
	b.data[i], b.data[j] = b.data[j], b.data[i]
}

// Slice returns a new Bytes struct with its internal data sliced according to
// start and end.
func (b Bytes) Slice(start, end int) *Bytes {
	return &Bytes{data: b.data[start:end]}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx inclusive []byte values
// from src into the receiver starting at destIdx.
func (b Bytes) CopySlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	copy(b.data[destIdx:], src.data[srcStartIdx:srcEndIdx])
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx inclusive []byte
// values from src into the receiver starting at destIdx.
func (b *Bytes) AppendSlice(src *Bytes, destIdx, srcStartIdx, srcEndIdx int) {
	b.data = append(b.data[:destIdx], src.data[srcStartIdx:srcEndIdx]...)
}

// AppendVal appends the given []byte value to the end of the receiver.
func (b *Bytes) AppendVal(v []byte) {
	b.data = append(b.data, v)
}

// Len returns how many []byte values the receiver contains.
func (b Bytes) Len() int {
	return len(b.data)
}

var zeroBytesColumn = make([][]byte, BatchSize)

// Zero zeroes out the underlying bytes.
func (b Bytes) Zero() {
	for n := 0; n < len(b.data); n += copy(b.data[n:], zeroBytesColumn) {
	}
}

// PrimitiveRepr is a temprorary migration tool.
// TODO(asubiotto): Remove this.
func (b Bytes) PrimitiveRepr() [][]byte {
	return b.data
}
