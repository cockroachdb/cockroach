// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldataext

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// Datum wraps a tree.Datum. This is the struct that datumVec.Get() returns.
type Datum struct {
	tree.Datum
}

var _ coldata.Datum = &Datum{}
var _ tree.Datum = &Datum{}

// datumVec is a vector of tree.Datums of the same type.
type datumVec struct {
	// t is the type of the tree.Datums that datumVec stores.
	t *types.T
	// data is the underlying data stored in datumVec.
	data []tree.Datum
	// evalCtx is required for most of the methods of tree.Datum interface.
	evalCtx *tree.EvalContext

	scratch []byte
	da      sqlbase.DatumAlloc
}

var _ coldata.DatumVec = &datumVec{}

// newDatumVec returns a datumVec struct with capacity of n.
func newDatumVec(t *types.T, n int, evalCtx *tree.EvalContext) coldata.DatumVec {
	return &datumVec{
		t:       t,
		data:    make([]tree.Datum, n),
		evalCtx: evalCtx,
	}
}

// BinFn evaluates the provided binary function between the receiver and other.
// other can either be nil, tree.Datum, or *Datum.
func (d *Datum) BinFn(
	binFn *tree.BinOp, evalCtx *tree.EvalContext, other interface{},
) (tree.Datum, error) {
	return binFn.Fn(evalCtx, d.Datum, maybeUnwrapDatum(other))
}

// CompareDatum returns the comparison between d and other. The other is
// assumed to be tree.Datum. dVec is the datumVec that stores either d or other
// (it doesn't matter which one because it is used only to supply the eval
// context).
// Note that the method is named differently from "Compare" so that we do not
// overload tree.Datum.Compare method.
func (d *Datum) CompareDatum(dVec, other interface{}) int {
	return d.Datum.Compare(dVec.(*datumVec).evalCtx, maybeUnwrapDatum(other))
}

// Cast returns the result of casting d to the type toType. dVec is the
// datumVec that stores d and is used to supply the eval context.
func (d *Datum) Cast(dVec interface{}, toType *types.T) (tree.Datum, error) {
	return tree.PerformCast(dVec.(*datumVec).evalCtx, d.Datum, toType)
}

// Hash returns the hash of the datum as a byte slice.
func (d *Datum) Hash(da *sqlbase.DatumAlloc) []byte {
	ed := sqlbase.EncDatum{Datum: maybeUnwrapDatum(d)}
	b, err := ed.Fingerprint(d.ResolvedType(), da, nil /* appendTo */)
	if err != nil {
		colexecerror.InternalError(err)
	}
	return b
}

// Get implements coldata.DatumVec interface.
func (dv *datumVec) Get(i int) coldata.Datum {
	dv.maybeSetDNull(i)
	return &Datum{Datum: dv.data[i]}
}

// Set implements coldata.DatumVec interface.
func (dv *datumVec) Set(i int, v coldata.Datum) {
	datum := maybeUnwrapDatum(v)
	dv.assertValidDatum(datum)
	dv.data[i] = datum
}

// Slice implements coldata.DatumVec interface.
func (dv *datumVec) Slice(start, end int) coldata.DatumVec {
	return &datumVec{
		t:       dv.t,
		data:    dv.data[start:end],
		evalCtx: dv.evalCtx,
	}
}

// CopySlice implements coldata.DatumVec interface.
func (dv *datumVec) CopySlice(src coldata.DatumVec, destIdx, srcStartIdx, srcEndIdx int) {
	castSrc := src.(*datumVec)
	dv.assertSameTypeFamily(castSrc.t)
	copy(dv.data[destIdx:], castSrc.data[srcStartIdx:srcEndIdx])
}

// AppendSlice implements coldata.DatumVec interface.
func (dv *datumVec) AppendSlice(src coldata.DatumVec, destIdx, srcStartIdx, srcEndIdx int) {
	castSrc := src.(*datumVec)
	dv.assertSameTypeFamily(castSrc.t)
	dv.data = append(dv.data[:destIdx], castSrc.data[srcStartIdx:srcEndIdx]...)
}

// AppendVal implements coldata.DatumVec interface.
func (dv *datumVec) AppendVal(v coldata.Datum) {
	datum := maybeUnwrapDatum(v)
	dv.assertValidDatum(datum)
	dv.data = append(dv.data, datum)
}

// SetLength implements coldata.DatumVec interface.
func (dv *datumVec) SetLength(l int) {
	dv.data = dv.data[:l]
}

// Len implements coldata.DatumVec interface.
func (dv *datumVec) Len() int {
	return len(dv.data)
}

// Cap implements coldata.DatumVec interface.
func (dv *datumVec) Cap() int {
	return cap(dv.data)
}

// MarshalAt implements coldata.DatumVec interface.
func (dv *datumVec) MarshalAt(i int) ([]byte, error) {
	dv.maybeSetDNull(i)
	return sqlbase.EncodeTableValue(
		nil /* appendTo */, sqlbase.ColumnID(encoding.NoColumnID), dv.data[i], dv.scratch,
	)
}

// UnmarshalTo implements coldata.DatumVec interface.
// index i.
func (dv *datumVec) UnmarshalTo(i int, b []byte) error {
	var err error
	dv.data[i], _, err = sqlbase.DecodeTableValue(&dv.da, dv.t, b)
	return err
}

// assertValidDatum asserts that the given datum is valid to be stored in this
// datumVec.
func (dv *datumVec) assertValidDatum(datum tree.Datum) {
	if datum != tree.DNull {
		dv.assertSameTypeFamily(datum.ResolvedType())
	}
}

// assertSameTypeFamily asserts that the provided type is of the same type
// family as the datums this datumVec stores.
func (dv *datumVec) assertSameTypeFamily(t *types.T) {
	if dv.t.Family() != t.Family() {
		colexecerror.InternalError(
			fmt.Sprintf("cannot use value of type %+v on a datumVec of type %+v", t, dv.t),
		)
	}
}

// maybeSetDNull checks whether the datum at index i is nil, and if so, sets it
// to DNull. This is needed because we store whether a value is NULL in a
// separate Nulls vector, but we also might be accessing the garbage value from
// the vectors in certain code paths (for example, in Vec.Append).
func (dv *datumVec) maybeSetDNull(i int) {
	if dv.data[i] == nil {
		dv.data[i] = tree.DNull
	}
}

// maybeUnwrapDatum possibly unwraps tree.Datum from inside of Datum. It also
// checks whether v is nil and returns tree.DNull if it is.
func maybeUnwrapDatum(v coldata.Datum) tree.Datum {
	if v == nil {
		return tree.DNull
	}
	if datum, ok := v.(*Datum); ok {
		return datum.Datum
	} else if datum, ok := v.(tree.Datum); ok {
		return datum
	}
	colexecerror.InternalError(fmt.Sprintf("unexpected value: %v", v))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
