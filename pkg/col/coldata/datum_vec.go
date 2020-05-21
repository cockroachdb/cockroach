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

// Datum is abstract type for elements inside DatumVec, this type in reality
// should be tree.Datum. However, in order to avoid pulling in 'tree' package
// into the 'coldata' package, we use a runtime cast instead.
type Datum interface{}

// DatumVec is the interface for a specialized vector that operates on
// tree.Datums in the vectorized engine. In order to avoid import of 'tree'
// package the implementation of DatumVec lives in 'coldataext' package.
type DatumVec interface {
	// Get returns the datum at index i in the vector.
	Get(i int) Datum
	// Set sets the datum at index i in the vector. It must check whether the
	// provided datum is compatible with the type that the DatumVec stores.
	Set(i int, v Datum)
	// Slice creates a "window" into the vector. It behaves similarly to
	// Golang's slice.
	Slice(start, end int) DatumVec
	// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive
	// tree.Datum values from src into the vector starting at destIdx.
	CopySlice(src DatumVec, destIdx, srcStartIdx, srcEndIdx int)
	// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive
	// tree.Datum values from src into the vector starting at destIdx.
	AppendSlice(src DatumVec, destIdx, srcStartIdx, srcEndIdx int)
	// AppendVal appends the given tree.Datum value to the end of the vector.
	AppendVal(v Datum)
	// SetLength sets the length of the vector.
	SetLength(l int)
	// Len returns the length of the vector.
	Len() int
	// Cap returns the underlying capacity of the vector.
	Cap() int
	// MarshalAt returns the marshaled representation of datum at index i.
	MarshalAt(i int) ([]byte, error)
	// UnmarshalTo unmarshals the byte representation of a datum and sets it at
	// index i.
	UnmarshalTo(i int, b []byte) error
}
