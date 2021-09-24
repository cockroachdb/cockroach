// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colencoding

import (
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// DecodeKeyValsToCols decodes the values that are part of the key, writing the
// result to the idx'th slot of the input slice of coldata.Vecs.
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
	da *rowenc.DatumAlloc,
	vecs []coldata.Vec,
	idx int,
	indexColIdx []int,
	checkAllColsForNull bool,
	types []*types.T,
	directions []descpb.IndexDescriptor_Direction,
	unseen *util.FastIntSet,
	key []byte,
	invertedColIdx int,
) (remainingKey []byte, foundNull bool, _ error) {
	for j := range types {
		var err error
		i := indexColIdx[j]
		if i == -1 {
			if checkAllColsForNull {
				isNull := encoding.PeekType(key) == encoding.Null
				foundNull = foundNull || isNull
			}
			// Don't need the coldata - skip it.
			key, err = rowenc.SkipTableKey(key)
		} else {
			if unseen != nil {
				unseen.Remove(i)
			}
			var isNull bool
			isInverted := invertedColIdx == i
			key, isNull, err = decodeTableKeyToCol(da, vecs[i], idx, types[j], key, directions[j], isInverted)
			foundNull = isNull || foundNull
		}
		if err != nil {
			return nil, false, err
		}
	}
	return key, foundNull, nil
}

// decodeTableKeyToCol decodes a value encoded by EncodeTableKey, writing the result
// to the idx'th slot of the input colexec.Vec.
// See the analog, DecodeTableKey, in sqlbase/column_type_encoding.go.
// decodeTableKeyToCol also returns whether or not the decoded value was NULL.
func decodeTableKeyToCol(
	da *rowenc.DatumAlloc,
	vec coldata.Vec,
	idx int,
	valType *types.T,
	key []byte,
	dir descpb.IndexDescriptor_Direction,
	isInverted bool,
) ([]byte, bool, error) {
	if (dir != descpb.IndexDescriptor_ASC) && (dir != descpb.IndexDescriptor_DESC) {
		return nil, false, errors.AssertionFailedf("invalid direction: %d", log.Safe(dir))
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		vec.Nulls().SetNull(idx)
		return key, true, nil
	}

	// Inverted columns should not be decoded, but should instead be
	// passed on as a DBytes datum.
	if isInverted {
		keyLen, err := encoding.PeekLength(key)
		if err != nil {
			return nil, false, err
		}
		vec.Bytes().Set(idx, key[:keyLen])
		return key[keyLen:], false, nil
	}

	var rkey []byte
	var err error
	switch valType.Family() {
	case types.BoolFamily:
		var i int64
		if dir == descpb.IndexDescriptor_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		vec.Bool()[idx] = i != 0
	case types.IntFamily, types.DateFamily:
		var i int64
		if dir == descpb.IndexDescriptor_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		switch valType.Width() {
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		case 0, 64:
			vec.Int64()[idx] = i
		}
	case types.FloatFamily:
		var f float64
		if dir == descpb.IndexDescriptor_ASC {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		vec.Float64()[idx] = f
	case types.DecimalFamily:
		var d apd.Decimal
		if dir == descpb.IndexDescriptor_ASC {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		vec.Decimal()[idx] = d
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		var r []byte
		if dir == descpb.IndexDescriptor_ASC {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		vec.Bytes().Set(idx, r)
	case types.TimestampFamily, types.TimestampTZFamily:
		var t time.Time
		if dir == descpb.IndexDescriptor_ASC {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		vec.Timestamp()[idx] = t
	case types.IntervalFamily:
		var d duration.Duration
		if dir == descpb.IndexDescriptor_ASC {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		vec.Interval()[idx] = d
	case types.JsonFamily:
		// Don't attempt to decode the JSON value. Instead, just return the
		// remaining bytes of the key.
		var jsonLen int
		jsonLen, err = encoding.PeekLength(key)
		vec.JSON().Bytes.Set(idx, key[:jsonLen])
		rkey = key[jsonLen:]
	default:
		var d tree.Datum
		encDir := encoding.Ascending
		if dir == descpb.IndexDescriptor_DESC {
			encDir = encoding.Descending
		}
		d, rkey, err = rowenc.DecodeTableKey(da, valType, key, encDir)
		vec.Datum().Set(idx, d)
	}
	return rkey, false, err
}

// UnmarshalColumnValueToCol decodes the value from a roachpb.Value using the
// type expected by the column, writing into the input Vec at the given row
// idx. An error is returned if the value's type does not match the column's
// type.
// See the analog, UnmarshalColumnValue, in sqlbase/column_type_encoding.go
func UnmarshalColumnValueToCol(
	da *rowenc.DatumAlloc, vec coldata.Vec, idx int, typ *types.T, value roachpb.Value,
) error {
	if value.RawBytes == nil {
		vec.Nulls().SetNull(idx)
	}

	var err error
	switch typ.Family() {
	case types.BoolFamily:
		var v bool
		v, err = value.GetBool()
		vec.Bool()[idx] = v
	case types.IntFamily:
		var v int64
		v, err = value.GetInt()
		switch typ.Width() {
		case 16:
			vec.Int16()[idx] = int16(v)
		case 32:
			vec.Int32()[idx] = int32(v)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vec.Int64()[idx] = v
		}
	case types.FloatFamily:
		var v float64
		v, err = value.GetFloat()
		vec.Float64()[idx] = v
	case types.DecimalFamily:
		err = value.GetDecimalInto(&vec.Decimal()[idx])
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		var v []byte
		v, err = value.GetBytes()
		vec.Bytes().Set(idx, v)
	case types.DateFamily:
		var v int64
		v, err = value.GetInt()
		vec.Int64()[idx] = v
	case types.TimestampFamily, types.TimestampTZFamily:
		var v time.Time
		v, err = value.GetTime()
		vec.Timestamp()[idx] = v
	case types.IntervalFamily:
		var v duration.Duration
		v, err = value.GetDuration()
		vec.Interval()[idx] = v
	case types.JsonFamily:
		var v []byte
		v, err = value.GetBytes()
		vec.JSON().Bytes.Set(idx, v)
	// Types backed by tree.Datums.
	default:
		var d tree.Datum
		d, err = rowenc.UnmarshalColumnValue(da, typ, value)
		if err != nil {
			return err
		}
		vec.Datum().Set(idx, d)
	}
	return err
}
