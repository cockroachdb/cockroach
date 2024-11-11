// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldataext

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// datumVec is a vector of tree.Datums of the same type.
type datumVec struct {
	// t is the type of the tree.Datums that datumVec stores.
	t *types.T
	// data is the underlying data stored in datumVec.
	data []tree.Datum
	// evalCtx is required for most of the methods of tree.Datum interface.
	evalCtx *eval.Context

	scratch []byte
	da      tree.DatumAlloc
}

var _ coldata.DatumVec = &datumVec{}

// newDatumVec returns a datumVec struct with capacity of n.
func newDatumVec(t *types.T, n int, evalCtx *eval.Context) coldata.DatumVec {
	return &datumVec{
		t:       t,
		data:    make([]tree.Datum, n),
		evalCtx: evalCtx,
	}
}

// CompareDatum returns the comparison between d and other. The other is
// assumed to be tree.Datum. dVec is the datumVec that stores either d or other
// (it doesn't matter which one because it is used only to supply the eval
// context).
// Note that the method is named differently from "Compare" so that we do not
// overload tree.Datum.Compare method.
func CompareDatum(d, dVec, other interface{}) int {
	// TODO(yuzefovich): use proper context after the execversion change lands
	// (#119114).
	res, err := d.(tree.Datum).Compare(context.TODO(), dVec.(*datumVec).evalCtx, convertToDatum(other))
	if err != nil {
		colexecerror.InternalError(err)
	}
	return res
}

// Hash returns the hash of the datum as a byte slice.
func Hash(d tree.Datum, da *tree.DatumAlloc) []byte {
	ed := rowenc.EncDatum{Datum: convertToDatum(d)}
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

// Get implements coldata.DatumVec interface.
func (dv *datumVec) Get(i int) coldata.Datum {
	dv.maybeSetDNull(i)
	return dv.data[i]
}

// Set implements coldata.DatumVec interface.
func (dv *datumVec) Set(i int, v coldata.Datum) {
	datum := convertToDatum(v)
	if buildutil.CrdbTestBuild {
		dv.assertValidDatum(datum)
	}
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
	if buildutil.CrdbTestBuild {
		dv.assertSameTypeFamily(castSrc.t)
	}
	copy(dv.data[destIdx:], castSrc.data[srcStartIdx:srcEndIdx])
}

// AppendSlice implements coldata.DatumVec interface.
func (dv *datumVec) AppendSlice(src coldata.DatumVec, destIdx, srcStartIdx, srcEndIdx int) {
	castSrc := src.(*datumVec)
	if buildutil.CrdbTestBuild {
		dv.assertSameTypeFamily(castSrc.t)
	}
	dv.data = append(dv.data[:destIdx], castSrc.data[srcStartIdx:srcEndIdx]...)
}

// AppendVal implements coldata.DatumVec interface.
func (dv *datumVec) AppendVal(v coldata.Datum) {
	datum := convertToDatum(v)
	if buildutil.CrdbTestBuild {
		dv.assertValidDatum(datum)
	}
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
func (dv *datumVec) MarshalAt(appendTo []byte, i int) (_ []byte, err error) {
	dv.maybeSetDNull(i)
	appendTo, dv.scratch, err = valueside.EncodeWithScratch(
		appendTo, valueside.NoColumnID, dv.data[i], dv.scratch[:0],
	)
	return appendTo, err
}

// UnmarshalTo implements coldata.DatumVec interface.
func (dv *datumVec) UnmarshalTo(i int, b []byte) error {
	var err error
	dv.data[i], _, err = valueside.Decode(&dv.da, dv.t, b)
	return err
}

// Size implements coldata.DatumVec interface.
func (dv *datumVec) Size(startIdx int) int64 {
	// Note that we don't account for the overhead of datumVec struct, and the
	// calculations are such that they are in line with
	// colmem.EstimateBatchSizeBytes.
	if startIdx >= dv.Cap() {
		return 0
	}
	if startIdx < 0 {
		startIdx = 0
	}
	count := int64(dv.Cap() - startIdx)
	size := memsize.DatumOverhead * count
	if datumSize, variable := tree.DatumTypeSize(dv.t); variable {
		// The elements in dv.data[max(startIdx,len):cap] range are accounted with
		// the default datum size for the type. For those in the range
		// [startIdx, len) we call Datum.Size().
		idx := startIdx
		for ; idx < len(dv.data); idx++ {
			if dv.data[idx] != nil {
				size += int64(dv.data[idx].Size())
			}
		}
		// Pick up where the loop left off.
		size += int64(dv.Cap()-idx) * int64(datumSize)
	} else {
		size += int64(datumSize) * count
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

// convertToDatum converts v to the corresponding tree.Datum.
func convertToDatum(v coldata.Datum) tree.Datum {
	if v == nil {
		return tree.DNull
	}
	if datum, ok := v.(tree.Datum); ok {
		return datum
	}
	colexecerror.InternalError(errors.AssertionFailedf("unexpected value: %v", v))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// SetEvalCtx implements coldata.DatumVec interface.
func (dv *datumVec) SetEvalCtx(evalCtx interface{}) {
	dv.evalCtx = evalCtx.(*eval.Context)
}

// SetType implements coldata.DatumVec interface.
func (dv *datumVec) SetType(t interface{}) {
	dv.t = t.(*types.T)
}
