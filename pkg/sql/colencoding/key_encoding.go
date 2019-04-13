// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package colencoding

import (
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// DecodeIndexKeyToCols decodes an index key into the idx'th position of the
// provided slices of exec.ColVecs. The input index key must already have its
// first table id / index id prefix removed. If matches is false, the key is
// from a different table, and the returned remainingKey indicates a
// "seek prefix": the next key that might be part of the table being searched
// for. The input key will also be mutated if matches is false.
// See the analog in sqlbase/index_encoding.go.
func DecodeIndexKeyToCols(
	vecs []coldata.Vec,
	idx uint16,
	desc *sqlbase.ImmutableTableDescriptor,
	index *sqlbase.IndexDescriptor,
	indexColIdx []int,
	types []types.T,
	colDirs []sqlbase.IndexDescriptor_Direction,
	key roachpb.Key,
) (remainingKey roachpb.Key, matches bool, _ error) {
	var decodedTableID sqlbase.ID
	var decodedIndexID sqlbase.IndexID
	var err error

	origKey := key

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// Our input key had its first table id / index id chopped off, so
			// don't try to decode those for the first ancestor.
			if i != 0 {
				key, decodedTableID, decodedIndexID, err = sqlbase.DecodeTableIDIndexID(key)
				if err != nil {
					return nil, false, err
				}
				if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
					// We don't match. Return a key with the table ID / index ID we're
					// searching for, so the caller knows what to seek to.
					curPos := len(origKey) - len(key)
					key = sqlbase.EncodeTableIDIndexID(origKey[:curPos], ancestor.TableID, ancestor.IndexID)
					return key, false, nil
				}
			}

			length := int(ancestor.SharedPrefixLen)
			key, err = DecodeKeyValsToCols(vecs, idx, indexColIdx[:length], types[:length], colDirs[:length], key)
			if err != nil {
				return nil, false, err
			}
			indexColIdx, types, colDirs = indexColIdx[length:], types[length:], colDirs[length:]

			// Consume the interleaved sentinel.
			var ok bool
			key, ok = encoding.DecodeIfInterleavedSentinel(key)
			if !ok {
				// We're expecting an interleaved sentinel but didn't find one. Append
				// one so the caller can seek to it.
				curPos := len(origKey) - len(key)
				key = encoding.EncodeInterleavedSentinel(origKey[:curPos])
				return key, false, nil
			}
		}

		key, decodedTableID, decodedIndexID, err = sqlbase.DecodeTableIDIndexID(key)
		if err != nil {
			return nil, false, err
		}
		if decodedTableID != desc.ID || decodedIndexID != index.ID {
			// We don't match. Return a key with the table ID / index ID we're
			// searching for, so the caller knows what to seek to.
			curPos := len(origKey) - len(key)
			key = sqlbase.EncodeTableIDIndexID(origKey[:curPos], desc.ID, index.ID)
			return key, false, nil
		}
	}

	key, err = DecodeKeyValsToCols(vecs, idx, indexColIdx, types, colDirs, key)
	if err != nil {
		return nil, false, err
	}

	// We're expecting a column family id next (a varint). If
	// interleavedSentinel is actually next, then this key is for a child
	// table.
	if _, ok := encoding.DecodeIfInterleavedSentinel(key); ok {
		curPos := len(origKey) - len(key)
		key = encoding.EncodeNullDescending(origKey[:curPos])
		return key, false, nil
	}

	return key, true, nil
}

// DecodeKeyValsToCols decodes the values that are part of the key, writing the
// result to the idx'th slot of the input slice of exec.ColVecs. If the
// directions slice is nil, the direction used will default to
// encoding.Ascending.
// See the analog in sqlbase/index_encoding.go.
func DecodeKeyValsToCols(
	vecs []coldata.Vec,
	idx uint16,
	indexColIdx []int,
	types []types.T,
	directions []sqlbase.IndexDescriptor_Direction,
	key []byte,
) ([]byte, error) {
	for j := range types {
		enc := sqlbase.IndexDescriptor_ASC
		if directions != nil {
			enc = directions[j]
		}
		var err error
		i := indexColIdx[j]
		if i == -1 {
			// Don't need the coldata - skip it.
			key, err = skipTableKey(&types[j], key, enc)
		} else {
			key, err = decodeTableKeyToCol(vecs[i], idx, &types[j], key, enc)
		}
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// decodeTableKeyToCol decodes a value encoded by EncodeTableKey, writing the result
// to the idx'th slot of the input exec.Vec.
// See the analog, DecodeTableKey, in
func decodeTableKeyToCol(
	vec coldata.Vec, idx uint16, valType *types.T, key []byte, dir sqlbase.IndexDescriptor_Direction,
) ([]byte, error) {
	if (dir != sqlbase.IndexDescriptor_ASC) && (dir != sqlbase.IndexDescriptor_DESC) {
		return nil, pgerror.NewAssertionErrorf("invalid direction: %d", log.Safe(dir))
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		vec.SetNull(idx)
		return key, nil
	}
	var rkey []byte
	var err error
	switch valType.SemanticType {
	case types.BOOL:
		var i int64
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		vec.Bool()[idx] = i != 0
	case types.INT:
		var i int64
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		switch valType.Width {
		case 8:
			vec.Int8()[idx] = int8(i)
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		case 0, 64:
			vec.Int64()[idx] = i
		}
	case types.FLOAT:
		var f float64
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		vec.Float64()[idx] = f
	case types.DECIMAL:
		var d apd.Decimal
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		vec.Decimal()[idx] = d
	case types.BYTES, types.STRING:
		var r []byte
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		vec.Bytes()[idx] = r
	case types.DATE:
		var t int64
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		vec.Int64()[idx] = t
	default:
		return rkey, pgerror.NewAssertionErrorf("unsupported type %+v", log.Safe(valType))
	}
	return rkey, err
}

// skipTableKey skips a value of type valType in key, returning the remainder
// of the key.
// TODO(jordan): each type could be optimized here.
// TODO(jordan): should use this approach in the normal row fetcher.
func skipTableKey(
	valType *types.T, key []byte, dir sqlbase.IndexDescriptor_Direction,
) ([]byte, error) {
	if (dir != sqlbase.IndexDescriptor_ASC) && (dir != sqlbase.IndexDescriptor_DESC) {
		return nil, pgerror.NewAssertionErrorf("invalid direction: %d", log.Safe(dir))
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return key, nil
	}
	var rkey []byte
	var err error
	switch valType.SemanticType {
	case types.BOOL, types.INT, types.DATE:
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, _, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, _, err = encoding.DecodeVarintDescending(key)
		}
	case types.FLOAT:
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, _, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, _, err = encoding.DecodeFloatDescending(key)
		}
	case types.BYTES, types.STRING:
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, _, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, _, err = encoding.DecodeBytesDescending(key, nil)
		}
	case types.DECIMAL:
		if dir == sqlbase.IndexDescriptor_ASC {
			rkey, _, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, _, err = encoding.DecodeDecimalDescending(key, nil)
		}
	default:
		return key, pgerror.NewAssertionErrorf("unsupported type %+v", log.Safe(valType))
	}
	if err != nil {
		return key, err
	}
	return rkey, nil
}

// UnmarshalColumnValueToCol decodes the value from a roachpb.Value using the
// type expected by the column, writing into the input Vec at the given row
// idx. An error is returned if the value's type does
// not match the column's type.
// See the analog, UnmarshalColumnValue, in sqlbase/column_type_encoding.go
func UnmarshalColumnValueToCol(
	vec coldata.Vec, idx uint16, typ *types.T, value roachpb.Value,
) error {
	if value.RawBytes == nil {
		vec.SetNull(idx)
	}

	var err error
	switch typ.SemanticType {
	case types.BOOL:
		var v bool
		v, err = value.GetBool()
		vec.Bool()[idx] = v
	case types.INT:
		var v int64
		v, err = value.GetInt()
		switch typ.Width {
		case 8:
			vec.Int8()[idx] = int8(v)
		case 16:
			vec.Int16()[idx] = int16(v)
		case 32:
			vec.Int32()[idx] = int32(v)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vec.Int64()[idx] = v
		}
	case types.FLOAT:
		var v float64
		v, err = value.GetFloat()
		vec.Float64()[idx] = v
	case types.DECIMAL:
		err = value.GetDecimalInto(&vec.Decimal()[idx])
	case types.BYTES, types.STRING:
		var v []byte
		v, err = value.GetBytes()
		vec.Bytes()[idx] = v
	case types.DATE:
		var v int64
		v, err = value.GetInt()
		vec.Int64()[idx] = v
	default:
		return pgerror.NewAssertionErrorf("unsupported column type: %s", log.Safe(typ.SemanticType))
	}
	return err
}
