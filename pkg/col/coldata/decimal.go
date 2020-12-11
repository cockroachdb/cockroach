// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"fmt"
	"math/big"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive []byte
// values from src into the receiver starting at destIdx.
func (d *Decimals) AppendSlice(src *Decimals, destIdx, srcStartIdx, srcEndIdx int) {
	d.Bytes.AppendSlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// AppendVal appends the given []byte value to the end of the receiver. A nil
// value will be "converted" into an empty byte slice.
func (d *Decimals) AppendVal(v apd.Decimal) {
	if d.isWindow {
		panic("AppendVal is called on a window into Decimal")
	}
	d.maybeBackfillOffsets(d.Len())
	d.data = encoding.EncodeFlatDecimal(&v, d.data[:d.offsets[d.Len()]])
	d.maxSetIndex = d.Len()
	d.offsets = append(d.offsets, int32(len(d.data)))
}

// unsafeSetNat sets the backing slice of big.Word of the input big.Int to the
// input []big.Word
func unsafeSetNat(b *big.Int, words []big.Word) {
	ptrToWords := (*[]big.Word)(encoding.UnsafeGetAbsPtr(b))
	*ptrToWords = words
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive apd.Decimal values
// from src into the receiver starting at destIdx. See the comment on
// Bytes.CopySlice for more information.
func (d *Decimals) CopySlice(src *Decimals, destIdx, srcStartIdx, srcEndIdx int) {
	d.Bytes.CopySlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// Set sets the ith apd.Decimal in d. Overwriting a value that is not at the end
// of the Decimals is not allowed since it complicates memory movement to make/take
// away necessary space in the flat buffer. Note that a nil value will be
func (d *Decimals) Set(i int, v apd.Decimal) {
	if d.isWindow {
		panic("Set is called on a window into Decimals")
	}
	if i < d.maxSetIndex {
		panic(
			fmt.Sprintf(
				"cannot overwrite value on flat Decimals: maxSetIndex=%d, setIndex=%d, consider using Reset",
				d.maxSetIndex,
				i,
			),
		)
	}
	// We're maybe setting an element not right after the last already present
	// element (i.e. there might be gaps in b.offsets). This is probably due to
	// NULL values that are stored separately. In order to maintain the
	// assumption of non-decreasing offsets, we need to backfill them.
	d.maybeBackfillOffsets(i)
	d.data = encoding.EncodeFlatDecimal(&v, d.data[:d.offsets[i]])
	d.offsets[i+1] = int32(len(d.data))
	d.maxSetIndex = i
}

// Window creates a "window" into the receiver. It behaves similarly to
// Golang's slice, but the returned object is *not* allowed to be modified - it
// is read-only. Window is a lightweight operation that doesn't involve copying
// the underlying data.
func (d *Decimals) Window(start, end int) *Decimals {
	bytesWindow := d.Bytes.newWindow(start, end)
	return &Decimals{
		Bytes: bytesWindow,
	}
}

var decimalSize = unsafe.Sizeof(apd.Decimal{})

// Size returns the total size of the receiver in bytes.
func (d *Decimals) Size() uintptr {
	return d.Bytes.Size()
}

// SetLength sets the length of this Bytes. Note that it will panic if there is
// not enough capacity.
func (d *Decimals) SetLength(l int) {
	if d.isWindow {
		panic("SetLength is called on a window into Bytes")
	}
	// We need +1 for an extra offset at the end.
	d.offsets = d.offsets[:l+1]
}
