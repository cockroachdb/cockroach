// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colencoding

import (
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// DecodeKeyValsToCols decodes the values that are part of the key, writing the
// result to the rowIdx'th row in coldata.TypedVecs.
//
// If the unseen int set is non-nil, upon decoding the column with ordinal i,
// i will be removed from the set to facilitate tracking whether or not columns
// have been observed during decoding.
//
// See the analog in rowenc/index_encoding.go.
//
// DecodeKeyValsToCols additionally returns whether a NULL was encountered when
// decoding. Sometimes it is necessary to determine if the value of a column is
// NULL even though the value itself is not needed. If checkAllColsForNull is
// true, then foundNull=true will be returned if any columns in the key are
// NULL, regardless of whether or not indexColIdx indicates that the column
// should be decoded.
func DecodeKeyValsToCols(
	da *tree.DatumAlloc,
	vecs *coldata.TypedVecs,
	rowIdx int,
	indexColIdx []int,
	checkAllColsForNull bool,
	keyCols []fetchpb.IndexFetchSpec_KeyColumn,
	unseen *intsets.Fast,
	key []byte,
	scratch []byte,
) (remainingKey []byte, foundNull bool, retScratch []byte, _ error) {
	for j := range keyCols {
		var err error
		vecIdx := indexColIdx[j]
		if vecIdx == -1 {
			if checkAllColsForNull {
				isNull := encoding.PeekType(key) == encoding.Null
				foundNull = foundNull || isNull
			}
			// Don't need the coldata - skip it.
			key, err = keyside.Skip(key)
		} else {
			if unseen != nil {
				unseen.Remove(vecIdx)
			}
			var isNull bool
			key, isNull, scratch, err = decodeTableKeyToCol(
				da, vecs, vecIdx, rowIdx,
				keyCols[j].Type, key, keyCols[j].Direction,
				scratch,
			)
			foundNull = isNull || foundNull
		}
		if err != nil {
			return nil, false, scratch, err
		}
	}
	return key, foundNull, scratch, nil
}

// decodeTableKeyToCol decodes a value encoded by keyside.Encode, writing the
// result to the rowIdx'th slot of the vecIdx'th vector in coldata.TypedVecs.
// See the analog, keyside.Decode, in rowenc/column_type_encoding.go.
// decodeTableKeyToCol also returns whether or not the decoded value was NULL.
func decodeTableKeyToCol(
	da *tree.DatumAlloc,
	vecs *coldata.TypedVecs,
	vecIdx int,
	rowIdx int,
	valType *types.T,
	key []byte,
	dir catenumpb.IndexColumn_Direction,
	scratch []byte,
) (_ []byte, _ bool, retScratch []byte, _ error) {
	if (dir != catenumpb.IndexColumn_ASC) && (dir != catenumpb.IndexColumn_DESC) {
		return nil, false, scratch, errors.AssertionFailedf("invalid direction: %d", redact.Safe(dir))
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		vecs.Nulls[vecIdx].SetNull(rowIdx)
		return key, true, scratch, nil
	}

	// Find the position of the target vector among the typed columns of its
	// type.
	colIdx := vecs.ColsMap[vecIdx]

	var rkey []byte
	var err error
	switch valType.Family() {
	case types.BoolFamily:
		var i int64
		if dir == catenumpb.IndexColumn_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		vecs.BoolCols[colIdx][rowIdx] = i != 0
	case types.IntFamily, types.DateFamily:
		var i int64
		if dir == catenumpb.IndexColumn_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		switch valType.Width() {
		case 16:
			vecs.Int16Cols[colIdx][rowIdx] = int16(i)
		case 32:
			vecs.Int32Cols[colIdx][rowIdx] = int32(i)
		case 0, 64:
			vecs.Int64Cols[colIdx][rowIdx] = i
		}
	case types.FloatFamily:
		var f float64
		if dir == catenumpb.IndexColumn_ASC {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		vecs.Float64Cols[colIdx][rowIdx] = f
	case types.DecimalFamily:
		var d apd.Decimal
		if dir == catenumpb.IndexColumn_ASC {
			rkey, d, err = encoding.DecodeDecimalAscending(key, scratch[:0])
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, scratch[:0])
		}
		vecs.DecimalCols[colIdx][rowIdx] = d
	case types.BytesFamily, types.StringFamily, types.UuidFamily, types.EnumFamily:
		if dir == catenumpb.IndexColumn_ASC {
			// We ask for the deep copy to be made so that scratch doesn't
			// reference the memory of key - this allows us to return scratch
			// to the caller to be reused. The deep copy additionally ensures
			// that the memory of the BatchResponse (where key came from) can be
			// GCed.
			rkey, scratch, err = encoding.DecodeBytesAscendingDeepCopy(key, scratch[:0])
		} else {
			// DecodeBytesDescending always performs a deep copy.
			rkey, scratch, err = encoding.DecodeBytesDescending(key, scratch[:0])
		}
		// Set() performs a deep copy, so it is safe to return the scratch slice
		// to the caller. Any modifications to the scratch slice made by the
		// caller will not affect the value in the vector.
		vecs.BytesCols[colIdx].Set(rowIdx, scratch)
	case types.TimestampFamily, types.TimestampTZFamily:
		var t time.Time
		if dir == catenumpb.IndexColumn_ASC {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		vecs.TimestampCols[colIdx][rowIdx] = t
	case types.IntervalFamily:
		var d duration.Duration
		if dir == catenumpb.IndexColumn_ASC {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		vecs.IntervalCols[colIdx][rowIdx] = d
	case types.JsonFamily:
		// Decode the JSON, and then store the bytes in the
		// vector in the value-encoded format.
		// TODO (shivam): Make it possible for the vector to store
		// key-encoded JSONs instead of value-encoded JSONs.
		var d tree.Datum
		encDir := encoding.Ascending
		if dir == catenumpb.IndexColumn_DESC {
			encDir = encoding.Descending
		}
		d, rkey, err = keyside.Decode(da, valType, key, encDir)
		json, ok := d.(*tree.DJSON)
		if !ok {
			return nil, false, scratch, errors.AssertionFailedf("Could not type assert into DJSON")
		}
		vecs.JSONCols[colIdx].Set(rowIdx, json.JSON)
	case types.EncodedKeyFamily:
		// Don't attempt to decode the inverted key.
		keyLen, err := encoding.PeekLength(key)
		if err != nil {
			return nil, false, scratch, err
		}
		vecs.BytesCols[colIdx].Set(rowIdx, key[:keyLen])
		rkey = key[keyLen:]
	default:
		var d tree.Datum
		encDir := encoding.Ascending
		if dir == catenumpb.IndexColumn_DESC {
			encDir = encoding.Descending
		}
		d, rkey, err = keyside.Decode(da, valType, key, encDir)
		vecs.DatumCols[colIdx].Set(rowIdx, d)
	}
	return rkey, false, scratch, err
}

// UnmarshalColumnValueToCol decodes the value from a non-zero length
// roachpb.Value using the type expected by the column, writing into the
// vecIdx'th vector of coldata.TypedVecs at the given rowIdx. An error is
// returned if the value's type does not match the column's type.
//
// See the analog, valueside.UnmarshalLegacy, in valueside/legacy.go.
func UnmarshalColumnValueToCol(
	da *tree.DatumAlloc,
	vecs *coldata.TypedVecs,
	vecIdx, rowIdx int,
	typ *types.T,
	value roachpb.Value,
) error {
	if buildutil.CrdbTestBuild {
		if len(value.RawBytes) == 0 {
			return errors.AssertionFailedf("zero-length value in UnmarshalColumnValueToCol")
		}
	}
	// Find the position of the target vector among the typed columns of its
	// type.
	colIdx := vecs.ColsMap[vecIdx]

	var err error
	switch typ.Family() {
	case types.BoolFamily:
		var v bool
		v, err = value.GetBool()
		vecs.BoolCols[colIdx][rowIdx] = v
	case types.IntFamily:
		var v int64
		v, err = value.GetInt()
		switch typ.Width() {
		case 16:
			vecs.Int16Cols[colIdx][rowIdx] = int16(v)
		case 32:
			vecs.Int32Cols[colIdx][rowIdx] = int32(v)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vecs.Int64Cols[colIdx][rowIdx] = v
		}
	case types.FloatFamily:
		var v float64
		v, err = value.GetFloat()
		vecs.Float64Cols[colIdx][rowIdx] = v
	case types.DecimalFamily:
		err = value.GetDecimalInto(&vecs.DecimalCols[colIdx][rowIdx])
	case types.BytesFamily, types.StringFamily, types.UuidFamily, types.EnumFamily:
		var v []byte
		v, err = value.GetBytes()
		vecs.BytesCols[colIdx].Set(rowIdx, v)
	case types.DateFamily:
		var v int64
		v, err = value.GetInt()
		vecs.Int64Cols[colIdx][rowIdx] = v
	case types.TimestampFamily, types.TimestampTZFamily:
		var v time.Time
		v, err = value.GetTime()
		vecs.TimestampCols[colIdx][rowIdx] = v
	case types.IntervalFamily:
		var v duration.Duration
		v, err = value.GetDuration()
		vecs.IntervalCols[colIdx][rowIdx] = v
	case types.JsonFamily:
		var v []byte
		v, err = value.GetBytes()
		vecs.JSONCols[colIdx].Bytes.Set(rowIdx, v)
	// Types backed by tree.Datums.
	default:
		var d tree.Datum
		d, err = valueside.UnmarshalLegacy(da, typ, value)
		if err != nil {
			return err
		}
		vecs.DatumCols[colIdx].Set(rowIdx, d)
	}
	return err
}
