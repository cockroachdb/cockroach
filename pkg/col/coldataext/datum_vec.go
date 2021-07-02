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
	"context"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
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
	da      rowenc.DatumAlloc
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
	binFn tree.TwoArgFn, evalCtx *tree.EvalContext, other interface{},
) (tree.Datum, error) {
	return binFn(evalCtx, d.Datum, maybeUnwrapDatum(other))
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
func (d *Datum) Hash(da *rowenc.DatumAlloc) []byte {
	ed := rowenc.EncDatum{Datum: maybeUnwrapDatum(d)}
	// We know that we have tree.Datum, so there will definitely be no need to
	// decode ed for fingerprinting, so we pass in nil memory account.
	b, err := ed.Fingerprint(context.TODO(), d.ResolvedType(), da, nil /* appendTo */, nil /* acc */)
	if err != nil {
		// Since we know that the memory accounting error cannot occur in
		// Fingerprint, we only can get an expected error (e.g. unable to encode
		// JSON as a key), so we propagate all of them accordingly.
		colexecerror.ExpectedError(err)
	}
	return b
}

// Size returns a lower bound on the total size of the receiver in bytes,
// including memory that is pointed at (even if shared between Datum
// instances) but excluding allocation overhead.
//
// It is assumed that d was obtained via datumVec.Get().
func (d *Datum) Size() uintptr {
	return d.Datum.Size()
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

// Window implements coldata.DatumVec interface.
func (dv *datumVec) Window(start, end int) coldata.DatumVec {
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
func (dv *datumVec) MarshalAt(appendTo []byte, i int) ([]byte, error) {
	dv.maybeSetDNull(i)
	return rowenc.EncodeTableValue(
		appendTo, descpb.ColumnID(encoding.NoColumnID), dv.data[i], dv.scratch,
	)
}

// UnmarshalTo implements coldata.DatumVec interface.
func (dv *datumVec) UnmarshalTo(i int, b []byte) error {
	var err error
	dv.data[i], _, err = rowenc.DecodeTableValue(&dv.da, dv.t, b)
	return err
}

const sizeOfDatum = unsafe.Sizeof(tree.Datum(nil))

// Size implements coldata.DatumVec interface.
func (dv *datumVec) Size(startIdx int) uintptr {
	// Note that we don't account for the overhead of datumVec struct, and the
	// calculations are such that they are in line with
	// colmem.EstimateBatchSizeBytes.
	if startIdx >= dv.Cap() {
		return 0
	}
	if startIdx < 0 {
		startIdx = 0
	}
	count := uintptr(dv.Cap() - startIdx)
	size := sizeOfDatum * count
	if datumSize, variable := tree.DatumTypeSize(dv.t); variable {
		// The elements in dv.data[max(startIdx,len):cap] range are accounted with
		// the default datum size for the type. For those in the range
		// [startIdx, len) we call Datum.Size().
		idx := startIdx
		for ; idx < len(dv.data); idx++ {
			if dv.data[idx] != nil {
				size += dv.data[idx].Size()
			}
		}
		// Pick up where the loop left off.
		size += uintptr(dv.Cap()-idx) * datumSize
	} else {
		size += datumSize * count
	}
	return size
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
			errors.AssertionFailedf("cannot use value of type %+v on a datumVec of type %+v", t, dv.t),
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
	colexecerror.InternalError(errors.AssertionFailedf("unexpected value: %v", v))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
