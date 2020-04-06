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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// DatumVec is a vector of tree.Datum of the same type.
// NOTE: currently vectorized engine only support JSON through datum types.
// this is due to the difficulties of consolidating logical type systems and
// physical type systems used in the vectorized engine.
type DatumVec struct {
	// typ is the type of the tree.Datum that DatumVec stores.
	typ *types.T

	// data is the underlying data stored in DatumVec.
	data []tree.Datum

	// evalCtx is required for most of the tree.Datum interfaces.
	evalCtx *tree.EvalContext

	da sqlbase.DatumAlloc
}

// ContextWrappedDatum wraps a tree.Datum with tree.EvalContext. This is the
// struct that DatumVec.Get() returns. The wrapped tree.EvalContext is used for
// calling tree.Datum interfaces and this avoided us to plumb down
// tree.EvalContext everywhere into vectorized engine.
type ContextWrappedDatum struct {
	tree.Datum
	evalCtx *tree.EvalContext
}

// CompareDatum returns the comparison between two datums.
func (cd *ContextWrappedDatum) CompareDatum(other tree.Datum) int {
	if other == nil {
		other = tree.DNull
	}
	unwrapped := other
	if ou, ok := other.(*ContextWrappedDatum); ok {
		unwrapped = ou.Datum
	}
	return cd.Datum.Compare(cd.evalCtx, unwrapped)
}

// NewDatumVec returns a DatumVec struct with capacity of n.
func NewDatumVec(n int, evalCtx *tree.EvalContext) *DatumVec {
	return &DatumVec{
		// TODO(azhng): we hard code the DatumVec type to Json due to the type
		//  system restrictions, see struct definition note for details.
		typ:     types.Jsonb,
		data:    make([]tree.Datum, n),
		evalCtx: evalCtx,
	}
}

// Get returns *ContextWrappedDatum which wraps the ith tree.Datum in DatumVec
// and the evalCtx stored inside DatumVec.
func (dv *DatumVec) Get(i int) tree.Datum {
	v := dv.data[i]
	if v == nil {
		v = tree.DNull
	}
	return &ContextWrappedDatum{
		Datum:   v,
		evalCtx: dv.evalCtx,
	}
}

// Set sets ith element of DatumVec to given tree.Datum v.
func (dv *DatumVec) Set(i int, v tree.Datum) {
	datum, evalCtx := unwrapContextWrappedDatum(v)
	dv.ensureValidDatum(datum)
	dv.data[i] = datum
	if dv.evalCtx == nil {
		dv.evalCtx = evalCtx
	}
}

// Window creates a "window" into the receiver. It behaves similarly to
// Golang's slice.
func (dv *DatumVec) Slice(start, end int) *DatumVec {
	return &DatumVec{
		typ:  dv.typ,
		data: dv.data[start:end],
	}
}

// CopySlice copies srcStartIdx inclusive and srcEndIdx exclusive tree.Datum
// values from src into the receiver starting at destIdx.
func (dv *DatumVec) CopySlice(src *DatumVec, destIdx, srcStartIdx, srcEndIdx int) {
	dv.ensureValidDatumType(src.typ)
	copy(dv.data[destIdx:], src.data[srcStartIdx:srcEndIdx])
	dv.evalCtx = src.evalCtx
}

// AppendSlice appends srcStartIdx inclusive and srcEndIdx exclusive tree.Datum
// values from src into the receiver starting at destIdx.
func (dv *DatumVec) AppendSlice(src *DatumVec, destIdx, srcStartIdx, srcEndIdx int) {
	dv.ensureValidDatumType(src.typ)
	dv.data = append(dv.data[:destIdx], src.data[srcStartIdx:srcEndIdx]...)
	dv.evalCtx = src.evalCtx
}

// AppendVal appends the given tree.Datum value to the end of the receiver.
func (dv *DatumVec) AppendVal(v tree.Datum) {
	datum, evalCtx := unwrapContextWrappedDatum(v)
	dv.ensureValidDatum(datum)
	dv.data = append(dv.data, datum)
	if dv.evalCtx == nil {
		dv.evalCtx = evalCtx
	}
}

// SetLength sets the length of this DatumVec.
func (dv *DatumVec) SetLength(l int) {
	dv.data = dv.data[:l]
}

// Len returns how many tree.Datum the DatumVec contains.
func (dv *DatumVec) Len() int {
	return len(dv.data)
}

// Cap returns the capacity of DatumVec.
func (dv *DatumVec) Cap() int {
	return cap(dv.data)
}

// MarshalAt returns the marshaled byte of datum at i.
func (dv *DatumVec) MarshalAt(i int) ([]byte, error) {
	datum := dv.data[i]
	// Append/Copy will copy/append values to dv.data regardless if it is nil.
	if datum == nil {
		datum = tree.DNull
	}
	switch dv.typ {
	case types.Jsonb:
		var bytes, scratch []byte
		b, err := sqlbase.EncodeTableValue(bytes, sqlbase.ColumnID(encoding.NoColumnID), datum, scratch)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		panic(fmt.Sprintf("unsupported type %v", dv.typ))
	}
}

// UnmarshalTextAt unmarshals the byte to datum and set it at i.
func (dv *DatumVec) UnmarshalTo(i int, b []byte) error {
	switch dv.typ.Family() {
	case types.JsonFamily:
		datum, _, err := sqlbase.DecodeTableValue(&dv.da, dv.typ, b)
		if err != nil {
			return err
		}
		dv.data[i] = datum
		return nil
	default:
		return fmt.Errorf("unsupported type for DatumVec %v", dv.typ)
	}
}

// SetType sets the type of DatumVec.
func (dv *DatumVec) SetType(t *types.T) {
	dv.typ = t
}

// ensureValidDatum ensures that the given datum has the same type as *types.T
// associated with the receiver if it is not null.
func (dv *DatumVec) ensureValidDatum(datum tree.Datum) {
	if datum != tree.DNull {
		dv.ensureValidDatumType(datum.ResolvedType())
	}
}

// ensureValidDatumType ensures that the given *types.T is same as the *types.T
// associated with the receiver if it is not null.
func (dv *DatumVec) ensureValidDatumType(typ *types.T) {
	if !dv.typ.Equal(*typ) {
		panic(
			fmt.Sprintf("cannot use value of type %v on a DatumVec of type: %v",
				typ, dv.typ,
			),
		)
	}
}

// unwrapContextWrappedDatum unwraps the datum and  if it is wrapped inside
// ContextWrappedDatum. This is to prevent us from recursively wrapping datums.
func unwrapContextWrappedDatum(v tree.Datum) (tree.Datum, *tree.EvalContext) {
	if v == nil {
		return tree.DNull, nil
	}
	if datum, ok := v.(*ContextWrappedDatum); ok {
		return datum.Datum, datum.evalCtx
	}
	return v, nil
}
