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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
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

// DecodeIndexKeyToCols decodes an index key into the idx'th position of the
// provided slices of colexec.Vecs. The input index key must already have its
// tenant id and first table id / index id prefix removed. If matches is false,
// the key is from a different table, and the returned remainingKey indicates a
// "seek prefix": the next key that might be part of the table being searched
// for. See the analog in sqlbase/index_encoding.go.
//
// Sometimes it is necessary to determine if the value of a column is NULL even
// though the value itself is not needed. If checkAllColsForNull is true, then
// foundNull=true will be returned if any columns in the key are NULL,
// regardless of whether or not indexColIdx indicates that the column should be
// decoded.
func DecodeIndexKeyToCols(
	da *rowenc.DatumAlloc,
	vecs []coldata.Vec,
	idx int,
	desc catalog.TableDescriptor,
	index catalog.Index,
	indexColIdx []int,
	checkAllColsForNull bool,
	types []*types.T,
	colDirs []descpb.IndexDescriptor_Direction,
	key roachpb.Key,
	invertedColIdx int,
	scratch []byte,
) (remainingKey roachpb.Key, matches bool, foundNull bool, retScratch []byte, _ error) {
	var decodedTableID descpb.ID
	var decodedIndexID descpb.IndexID
	var err error

	origKey := key

	if index.NumInterleaveAncestors() > 0 {
		for i := 0; i < index.NumInterleaveAncestors(); i++ {
			ancestor := index.GetInterleaveAncestor(i)
			// Our input key had its first table id / index id chopped off, so
			// don't try to decode those for the first ancestor.
			if i != 0 {
				lastKeyComponentLength := len(key)
				key, decodedTableID, decodedIndexID, err = rowenc.DecodePartialTableIDIndexID(key)
				if err != nil {
					return nil, false, false, scratch, err
				}
				if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
					// We don't match. Return a key with the table ID / index ID we're
					// searching for, so the caller knows what to seek to.
					curPos := len(origKey) - lastKeyComponentLength
					// Prevent unwanted aliasing on the origKey by setting the capacity.
					key = rowenc.EncodePartialTableIDIndexID(origKey[:curPos:curPos], ancestor.TableID, ancestor.IndexID)
					return key, false, false, scratch, nil
				}
			}

			length := int(ancestor.SharedPrefixLen)
			// We don't care about whether this call to DecodeKeyVals found a null or not, because
			// it is a interleaving ancestor.
			var isNull bool
			key, isNull, scratch, err = DecodeKeyValsToCols(
				da, vecs, idx, indexColIdx[:length], checkAllColsForNull, types[:length],
				colDirs[:length], nil /* unseen */, key, invertedColIdx, scratch,
			)
			if err != nil {
				return nil, false, false, scratch, err
			}
			indexColIdx, types, colDirs = indexColIdx[length:], types[length:], colDirs[length:]
			foundNull = foundNull || isNull

			// Consume the interleaved sentinel.
			var ok bool
			key, ok = encoding.DecodeIfInterleavedSentinel(key)
			if !ok {
				// We're expecting an interleaved sentinel but didn't find one. Append
				// one so the caller can seek to it.
				curPos := len(origKey) - len(key)
				// Prevent unwanted aliasing on the origKey by setting the capacity.
				key = encoding.EncodeInterleavedSentinel(origKey[:curPos:curPos])
				return key, false, false, scratch, nil
			}
		}

		lastKeyComponentLength := len(key)
		key, decodedTableID, decodedIndexID, err = rowenc.DecodePartialTableIDIndexID(key)
		if err != nil {
			return nil, false, false, scratch, err
		}
		if decodedTableID != desc.GetID() || decodedIndexID != index.GetID() {
			// We don't match. Return a key with the table ID / index ID we're
			// searching for, so the caller knows what to seek to.
			curPos := len(origKey) - lastKeyComponentLength
			// Prevent unwanted aliasing on the origKey by setting the capacity.
			key = rowenc.EncodePartialTableIDIndexID(origKey[:curPos:curPos], desc.GetID(), index.GetID())
			return key, false, false, scratch, nil
		}
	}

	var isNull bool
	key, isNull, scratch, err = DecodeKeyValsToCols(
		da, vecs, idx, indexColIdx, checkAllColsForNull, types, colDirs,
		nil /* unseen */, key, invertedColIdx, scratch,
	)
	if err != nil {
		return nil, false, false, scratch, err
	}
	foundNull = foundNull || isNull

	// We're expecting a column family id next (a varint). If
	// interleavedSentinel is actually next, then this key is for a child
	// table.
	lastKeyComponentLength := len(key)
	if _, ok := encoding.DecodeIfInterleavedSentinel(key); ok {
		curPos := len(origKey) - lastKeyComponentLength
		// Prevent unwanted aliasing on the origKey by setting the capacity.
		key = encoding.EncodeNullDescending(origKey[:curPos:curPos])
		return key, false, false, scratch, nil
	}

	return key, true, foundNull, scratch, nil
}

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
	scratch []byte,
) (remainingKey []byte, foundNull bool, retScratch []byte, _ error) {
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
			key, isNull, scratch, err = decodeTableKeyToCol(da, vecs[i], idx, types[j], key, directions[j], isInverted, scratch)
			foundNull = isNull || foundNull
		}
		if err != nil {
			return nil, false, scratch, err
		}
	}
	return key, foundNull, scratch, nil
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
	scratch []byte,
) (_ []byte, _ bool, retScratch []byte, _ error) {
	if (dir != descpb.IndexDescriptor_ASC) && (dir != descpb.IndexDescriptor_DESC) {
		return nil, false, scratch, errors.AssertionFailedf("invalid direction: %d", log.Safe(dir))
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		vec.Nulls().SetNull(idx)
		return key, true, scratch, nil
	}
	// We might have read a NULL value in the interleaved child table which
	// would update the nulls vector, so we need to explicitly unset the null
	// value here.
	vec.Nulls().UnsetNull(idx)

	// Inverted columns should not be decoded, but should instead be
	// passed on as a DBytes datum.
	if isInverted {
		keyLen, err := encoding.PeekLength(key)
		if err != nil {
			return nil, false, scratch, err
		}
		vec.Bytes().Set(idx, key[:keyLen])
		return key[keyLen:], false, scratch, nil
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
			rkey, d, err = encoding.DecodeDecimalAscending(key, scratch[:0])
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, scratch[:0])
		}
		vec.Decimal()[idx] = d
	case types.BytesFamily, types.StringFamily, types.UuidFamily:
		if dir == descpb.IndexDescriptor_ASC {
			// We ask for the deep copy to be made so that scratch doesn't
			// reference the memory of key - this allows us to return scratch
			// to the caller to be reused. The deep copy additionally ensures
			// that the memory of the BatchResponse (where key came from) can be
			// GCed.
			rkey, scratch, err = encoding.DecodeBytesAscendingDeepCopy(key, scratch[:0])
		} else {
			rkey, scratch, err = encoding.DecodeBytesDescending(key, scratch[:0])
		}
		// Set() performs a deep copy, so it is safe to return the scratch slice
		// to the caller. Any modifications to the scratch slice made by the
		// caller will not affect the value in the vector.
		vec.Bytes().Set(idx, scratch)
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
	return rkey, false, scratch, err
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
