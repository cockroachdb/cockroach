// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

// Enums is a vector of enums stored via their physical representation.
type Enums struct {
	Bytes
}

func newEnums(n int) *Enums {
	return &Enums{
		Bytes: *NewBytes(n),
	}
}

// Window creates a "window" into the receiver. It behaves similarly to
// Golang's slice, but the returned object is *not* allowed to be modified - it
// is read-only. Window is a lightweight operation that doesn't involve copying
// the underlying data.
func (e *Enums) Window(start, end int) *Enums {
	return &Enums{
		Bytes: *e.Bytes.Window(start, end),
	}
}

// Copy copies a single value from src at position srcIdx into position destIdx
// of the receiver.
func (e *Enums) Copy(src *Enums, destIdx, srcIdx int) {
	e.Bytes.Copy(&src.Bytes, destIdx, srcIdx)
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive []byte values
// from src into the receiver starting at destIdx. See Bytes.CopySlice.
func (e *Enums) CopySlice(src *Enums, destIdx, srcStartIdx, srcEndIdx int) {
	e.Bytes.CopySlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive JSON
// values from src into the receiver starting at destIdx.
func (e *Enums) AppendSlice(src *Enums, destIdx, srcStartIdx, srcEndIdx int) {
	e.Bytes.AppendSlice(&src.Bytes, destIdx, srcStartIdx, srcEndIdx)
}

// appendSliceWithSel appends all values specified in sel from the source into
// the receiver starting at position destIdx.
func (e *Enums) appendSliceWithSel(src *Enums, destIdx int, sel []int) {
	e.Bytes.appendSliceWithSel(&src.Bytes, destIdx, sel)
}
