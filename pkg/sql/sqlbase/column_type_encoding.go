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

package sqlbase

import (
	"fmt"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// This file contains facilities to encode values of specific SQL
// types to either index keys or to store in the value part of column
// families.

// EncodeTableKey encodes `val` into `b` and returns the new buffer.
// This is suitable to generate index/lookup keys in KV.
//
// The encoded value is guaranteed to be lexicographically sortable,
// but not guaranteed to be round-trippable during decoding: some
// values like decimals or collated strings have composite encoding
// where part of their value lies in the value part of the key/value
// pair.
//
// See also: docs/tech-notes/encoding.md, EncodeTableValue().
func EncodeTableKey(b []byte, val tree.Datum, dir encoding.Direction) ([]byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, errors.Errorf("invalid direction: %d", dir)
	}

	if val == tree.DNull {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}

	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DBool:
		var x int64
		if *t {
			x = 1
		} else {
			x = 0
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x), nil
		}
		return encoding.EncodeVarintDescending(b, x), nil
	case *tree.DInt:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DFloat:
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, float64(*t)), nil
		}
		return encoding.EncodeFloatDescending(b, float64(*t)), nil
	case *tree.DDecimal:
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, &t.Decimal), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Decimal), nil
	case *tree.DString:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DBytes:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DDate:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DTime:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DTimestamp:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DTimestampTZ:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DInterval:
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, t.Duration)
		}
		return encoding.EncodeDurationDescending(b, t.Duration)
	case *tree.DUuid:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.GetBytes()), nil
		}
		return encoding.EncodeBytesDescending(b, t.GetBytes()), nil
	case *tree.DIPAddr:
		data := t.ToBuffer(nil)
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, data), nil
		}
		return encoding.EncodeBytesDescending(b, data), nil
	case *tree.DTuple:
		for _, datum := range t.D {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *tree.DCollatedString:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.Key), nil
		}
		return encoding.EncodeBytesDescending(b, t.Key), nil
	case *tree.DArray:
		for _, datum := range t.Array {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *tree.DOid:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(t.DInt)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t.DInt)), nil
	}
	return nil, errors.Errorf("unable to encode table key: %T", val)
}

// DecodeTableKey decodes a value encoded by EncodeTableKey.
func DecodeTableKey(
	a *DatumAlloc, valType types.T, key []byte, dir encoding.Direction,
) (tree.Datum, []byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, nil, errors.Errorf("invalid direction: %d", dir)
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return tree.DNull, key, nil
	}
	var rkey []byte
	var err error
	switch valType {
	case types.Bool:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		// No need to chunk allocate DBool as MakeDBool returns either
		// tree.DBoolTrue or tree.DBoolFalse.
		return tree.MakeDBool(tree.DBool(i != 0)), rkey, err
	case types.Int:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDInt(tree.DInt(i)), rkey, err
	case types.Float:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return a.NewDFloat(tree.DFloat(f)), rkey, err
	case types.Decimal:
		var d apd.Decimal
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: d})
		return dd, rkey, err
	case types.String:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDString(tree.DString(r)), rkey, err
	case types.Name:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDName(tree.DString(r)), rkey, err
	case types.JSON:
		return tree.DNull, []byte{}, nil
	case types.Bytes:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(tree.DBytes(r)), rkey, err
	case types.Date:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDDate(tree.DDate(t)), rkey, err
	case types.Time:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDTime(tree.DTime(t)), rkey, err
	case types.Timestamp:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: t}), rkey, err
	case types.TimestampTZ:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: t}), rkey, err
	case types.Interval:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), rkey, err
	case types.UUID:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		u, err := uuid.FromBytes(r)
		return a.NewDUuid(tree.DUuid{UUID: u}), rkey, err
	case types.INet:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err := ipAddr.FromBuffer(r)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), rkey, err
	case types.Oid:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(i))), rkey, err
	default:
		switch t := valType.(type) {
		case types.TCollatedString:
			var r string
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
			if err != nil {
				return nil, nil, err
			}
			return tree.NewDCollatedString(r, t.Locale, &a.env), rkey, err
		}
		return nil, nil, errors.Errorf("TODO(pmattis): decoded index key: %s", valType)
	}
}

// EncodeTableValue encodes `val` into `appendTo` using DatumEncoding_VALUE
// and returns the new buffer.
//
// This is suitable for generating the value part of individual columns
// in a column family.
//
// The encoded value is guaranteed to round
// trip and decode exactly to its input, but is not guaranteed to be
// lexicographically sortable.
//
// See also: docs/tech-notes/encoding.md, EncodeTableKey().
func EncodeTableValue(
	appendTo []byte, colID ColumnID, val tree.Datum, scratch []byte,
) ([]byte, error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), nil
	case *tree.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Decimal), nil
	case *tree.DString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DTime:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DTimestamp:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeDurationValue(appendTo, uint32(colID), t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeIPAddrValue(appendTo, uint32(colID), t.IPAddr), nil
	case *tree.DJSON:
		encoded, err := json.EncodeJSON(scratch, t.JSON)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), encoded), nil
	case *tree.DArray:
		a, err := encodeArray(t, scratch)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), a), nil
	case *tree.DTuple:
		return encodeTuple(t, appendTo, uint32(colID), scratch)
	case *tree.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(t.Contents)), nil
	case *tree.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.DInt)), nil
	}
	return nil, errors.Errorf("unable to encode table value: %T", val)
}

// DecodeTableValue decodes a value encoded by EncodeTableValue.
func DecodeTableValue(a *DatumAlloc, valType types.T, b []byte) (tree.Datum, []byte, error) {
	_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
	if err != nil {
		return nil, b, err
	}
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		return tree.DNull, b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valType != types.Bool {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatum(a, valType, b)
}

// decodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place).
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
func decodeUntaggedDatum(a *DatumAlloc, t types.T, buf []byte) (tree.Datum, []byte, error) {
	switch t {
	case types.Int:
		b, i, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDInt(tree.DInt(i)), b, nil
	case types.String, types.Name:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDString(tree.DString(data)), b, nil
	case types.Bool:
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		b, data, err := encoding.DecodeBoolValue(buf)
		if err != nil {
			return nil, b, err
		}
		return tree.MakeDBool(tree.DBool(data)), b, nil
	case types.Float:
		b, data, err := encoding.DecodeUntaggedFloatValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDFloat(tree.DFloat(data)), b, nil
	case types.Decimal:
		b, data, err := encoding.DecodeUntaggedDecimalValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDecimal(tree.DDecimal{Decimal: data}), b, nil
	case types.Bytes:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDBytes(tree.DBytes(data)), b, nil
	case types.Date:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDate(tree.DDate(data)), b, nil
	case types.Time:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTime(tree.DTime(data)), b, nil
	case types.Timestamp:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: data}), b, nil
	case types.TimestampTZ:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: data}), b, nil
	case types.Interval:
		b, data, err := encoding.DecodeUntaggedDurationValue(buf)
		return a.NewDInterval(tree.DInterval{Duration: data}), b, err
	case types.UUID:
		b, data, err := encoding.DecodeUntaggedUUIDValue(buf)
		return a.NewDUuid(tree.DUuid{UUID: data}), b, err
	case types.INet:
		b, data, err := encoding.DecodeUntaggedIPAddrValue(buf)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: data}), b, err
	case types.JSON:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		j, err := json.FromEncoding(data)
		if err != nil {
			return nil, b, err
		}
		return a.NewDJSON(tree.DJSON{JSON: j}), b, nil
	case types.Oid:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		return a.NewDOid(tree.MakeDOid(tree.DInt(data))), b, err
	default:
		switch typ := t.(type) {
		case types.TOidWrapper:
			wrapped := typ.T
			d, rest, err := decodeUntaggedDatum(a, wrapped, buf)
			if err != nil {
				return d, rest, err
			}
			return &tree.DOidWrapper{
				Wrapped: d,
				Oid:     typ.Oid(),
			}, rest, nil
		case types.TCollatedString:
			b, data, err := encoding.DecodeUntaggedBytesValue(buf)
			return tree.NewDCollatedString(string(data), typ.Locale, &a.env), b, err
		case types.TArray:
			return decodeArray(a, typ.Typ, buf)
		case types.TTuple:
			return decodeTuple(a, typ, buf)
		}
		return nil, buf, errors.Errorf("couldn't decode type %s", t)
	}
}

// EncodeDatumKeyAscending encodes a datum using an order-preserving
// encoding.
// The encoding is lossy: some datums need composite encoding where
// the key part only contains part of the datum's information.
func EncodeDatumKeyAscending(b []byte, d tree.Datum) ([]byte, error) {
	if values, ok := d.(*tree.DTuple); ok {
		return EncodeDatumsKeyAscending(b, values.D)
	}
	return EncodeTableKey(b, d, encoding.Ascending)
}

// EncodeDatumsKeyAscending encodes a Datums (tuple) using an
// order-preserving encoding.
// The encoding is lossy: some datums need composite encoding where
// the key part only contains part of the datum's information.
func EncodeDatumsKeyAscending(b []byte, d tree.Datums) ([]byte, error) {
	for _, val := range d {
		var err error
		b, err = EncodeDatumKeyAscending(b, val)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// MarshalColumnValue produces the value encoding of the given datum,
// constrained by the given column type, into a roachpb.Value.
//
// This is used when when the table format does not use column
// families, such as pre-2.0 tables and some system tables.
//
// If val's type is incompatible with col, or if col's type is not yet
// implemented by this function, an error is returned.
func MarshalColumnValue(col ColumnDescriptor, val tree.Datum) (roachpb.Value, error) {
	var r roachpb.Value

	if val == tree.DNull {
		return r, nil
	}

	switch col.Type.SemanticType {
	case ColumnType_BOOL:
		if v, ok := val.(*tree.DBool); ok {
			r.SetBool(bool(*v))
			return r, nil
		}
	case ColumnType_INT:
		if v, ok := tree.AsDInt(val); ok {
			r.SetInt(int64(v))
			return r, nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(*tree.DFloat); ok {
			r.SetFloat(float64(*v))
			return r, nil
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*tree.DDecimal); ok {
			err := r.SetDecimal(&v.Decimal)
			return r, err
		}
	case ColumnType_STRING, ColumnType_NAME:
		if v, ok := tree.AsDString(val); ok {
			r.SetString(string(v))
			return r, nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(*tree.DBytes); ok {
			r.SetString(string(*v))
			return r, nil
		}
	case ColumnType_DATE:
		if v, ok := val.(*tree.DDate); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_TIME:
		if v, ok := val.(*tree.DTime); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_TIMESTAMP:
		if v, ok := val.(*tree.DTimestamp); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_TIMESTAMPTZ:
		if v, ok := val.(*tree.DTimestampTZ); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_INTERVAL:
		if v, ok := val.(*tree.DInterval); ok {
			err := r.SetDuration(v.Duration)
			return r, err
		}
	case ColumnType_UUID:
		if v, ok := val.(*tree.DUuid); ok {
			r.SetBytes(v.GetBytes())
			return r, nil
		}
	case ColumnType_INET:
		if v, ok := val.(*tree.DIPAddr); ok {
			data := v.ToBuffer(nil)
			r.SetBytes(data)
			return r, nil
		}
	case ColumnType_JSONB:
		if v, ok := val.(*tree.DJSON); ok {
			data, err := json.EncodeJSON(nil, v.JSON)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	case ColumnType_ARRAY:
		if v, ok := val.(*tree.DArray); ok {
			semanticType, err := datumTypeToColumnSemanticType(v.ParamTyp)
			if err != nil {
				return r, err
			}
			if semanticType != *col.Type.ArrayContents {
				return r, errors.Errorf("type of array contents %s doesn't match column type %s",
					v.ParamTyp, col.Type.ArrayContents)
			}
			if cs, ok := v.ParamTyp.(types.TCollatedString); ok {
				if cs.Locale != *col.Type.Locale {
					return r, errors.Errorf("locale of collated string array being inserted (%s) doesn't match locale of column type (%s)",
						cs.Locale, *col.Type.Locale)
				}
			}

			b, err := encodeArray(v, nil)
			if err != nil {
				return r, err
			}
			r.SetBytes(b)
			return r, nil
		}
	case ColumnType_COLLATEDSTRING:
		if col.Type.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		if v, ok := val.(*tree.DCollatedString); ok {
			if v.Locale == *col.Type.Locale {
				r.SetString(v.Contents)
				return r, nil
			}
			return r, fmt.Errorf("locale %q doesn't match locale %q of column %q",
				v.Locale, *col.Type.Locale, col.Name)
		}
	case ColumnType_OID:
		if v, ok := val.(*tree.DOid); ok {
			r.SetInt(int64(v.DInt))
			return r, nil
		}
	default:
		return r, errors.Errorf("unsupported column type: %s", col.Type.SemanticType)
	}
	return r, fmt.Errorf("value type %s doesn't match type %s of column %q",
		val.ResolvedType(), col.Type.SemanticType, col.Name)
}

// UnmarshalColumnValue is the counterpart to MarshalColumnValues.
//
// It decodes the value from a roachpb.Value using the type expected
// by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalColumnValue(a *DatumAlloc, typ ColumnType, value roachpb.Value) (tree.Datum, error) {
	if value.RawBytes == nil {
		return tree.DNull, nil
	}

	switch typ.SemanticType {
	case ColumnType_BOOL:
		v, err := value.GetBool()
		if err != nil {
			return nil, err
		}
		return tree.MakeDBool(tree.DBool(v)), nil
	case ColumnType_INT:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDInt(tree.DInt(v)), nil
	case ColumnType_FLOAT:
		v, err := value.GetFloat()
		if err != nil {
			return nil, err
		}
		return a.NewDFloat(tree.DFloat(v)), nil
	case ColumnType_DECIMAL:
		v, err := value.GetDecimal()
		if err != nil {
			return nil, err
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: v})
		return dd, nil
	case ColumnType_STRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDString(tree.DString(v)), nil
	case ColumnType_BYTES:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDBytes(tree.DBytes(v)), nil
	case ColumnType_DATE:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDDate(tree.DDate(v)), nil
	case ColumnType_TIME:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDTime(tree.DTime(v)), nil
	case ColumnType_TIMESTAMP:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: v}), nil
	case ColumnType_TIMESTAMPTZ:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: v}), nil
	case ColumnType_INTERVAL:
		d, err := value.GetDuration()
		if err != nil {
			return nil, err
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), nil
	case ColumnType_COLLATEDSTRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return tree.NewDCollatedString(string(v), *typ.Locale, &a.env), nil
	case ColumnType_UUID:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		u, err := uuid.FromBytes(v)
		if err != nil {
			return nil, err
		}
		return a.NewDUuid(tree.DUuid{UUID: u}), nil
	case ColumnType_INET:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err = ipAddr.FromBuffer(v)
		if err != nil {
			return nil, err
		}
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
	case ColumnType_NAME:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDName(tree.DString(v)), nil
	case ColumnType_OID:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(v))), nil
	default:
		return nil, errors.Errorf("unsupported column type: %s", typ.SemanticType)
	}
}

// encodeTuple produces the value encoding for a tuple.
func encodeTuple(t *tree.DTuple, appendTo []byte, colID uint32, scratch []byte) ([]byte, error) {
	appendTo = encoding.EncodeValueTag(appendTo, colID, encoding.Tuple)
	appendTo = encoding.EncodeNonsortingUvarint(appendTo, uint64(len(t.D)))

	var err error
	for _, dd := range t.D {
		appendTo, err = EncodeTableValue(appendTo, ColumnID(encoding.NoColumnID), dd, scratch)
		if err != nil {
			return nil, err
		}
	}
	return appendTo, nil
}

// decodeTuple decodes a tuple from its value encoding. It is the
// counterpart of encodeTuple().
func decodeTuple(a *DatumAlloc, elementTypes types.TTuple, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, nil, err
	}

	result := tree.DTuple{
		D: a.NewDatums(len(elementTypes.Types)),
	}

	var datum tree.Datum
	for i, typ := range elementTypes.Types {
		datum, b, err = DecodeTableValue(a, typ, b)
		if err != nil {
			return nil, b, err
		}
		result.D[i] = datum
	}
	return a.NewDTuple(result), b, nil
}

// encodeArray produces the value encoding for an array.
func encodeArray(d *tree.DArray, scratch []byte) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return scratch, err
	}
	scratch = scratch[0:0]
	unwrapped := types.UnwrapType(d.ParamTyp)
	elementType, err := datumTypeToArrayElementEncodingType(unwrapped)

	if err != nil {
		return nil, err
	}
	header := arrayHeader{
		hasNulls: d.HasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   elementType,
		length:        uint64(d.Len()),
		// We don't encode the NULL bitmap in this function because we do it in lockstep with the
		// main data.
	}
	scratch, err = encodeArrayHeader(header, scratch)
	if err != nil {
		return nil, err
	}
	nullBitmapStart := len(scratch)
	if d.HasNulls {
		for i := 0; i < numBytesInBitArray(d.Len()); i++ {
			scratch = append(scratch, 0)
		}
	}
	for i, e := range d.Array {
		var err error
		if d.HasNulls && e == tree.DNull {
			setBit(scratch[nullBitmapStart:], i)
		} else {
			scratch, err = encodeArrayElement(scratch, e)
			if err != nil {
				return nil, err
			}
		}
	}
	return scratch, nil
}

// decodeArray decodes the value encoding for an array.
func decodeArray(a *DatumAlloc, elementType types.T, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, b, err
	}
	header, b, err := decodeArrayHeader(b)
	if err != nil {
		return nil, b, err
	}
	result := tree.DArray{
		Array:    make(tree.Datums, header.length),
		ParamTyp: elementType,
	}
	var val tree.Datum
	for i := uint64(0); i < header.length; i++ {
		if header.isNull(i) {
			result.Array[i] = tree.DNull
			result.HasNulls = true
		} else {
			val, b, err = decodeUntaggedDatum(a, elementType, b)
			if err != nil {
				return nil, b, err
			}
			result.Array[i] = val
		}
	}
	return &result, b, nil
}

// arrayHeader is a parameter passing struct between
// encodeArray/decodeArray and encodeArrayHeader/decodeArrayHeader.
//
// It describes the important properties of an array that are useful
// for an efficient value encoding.
type arrayHeader struct {
	// hasNulls is set if the array contains any NULL values.
	hasNulls bool
	// numDimensions is the number of dimensions in the array.
	numDimensions int
	// elementType is the encoding type of the array elements.
	elementType encoding.Type
	// length is the total number of elements encoded.
	length uint64
	// nullBitmap is a compact representation of which array indexes
	// have NULL values.
	nullBitmap []byte
}

// isNull returns true iff the array element at the given index is
// NULL.
func (h arrayHeader) isNull(i uint64) bool {
	return h.hasNulls && ((h.nullBitmap[i/8]>>(i%8))&1) == 1
}

// setBit sets the bit in the given bitmap at index idx to 1. It's used to
// construct the NULL bitmap within arrays.
func setBit(bitmap []byte, idx int) {
	bitmap[idx/8] = bitmap[idx/8] | (1 << uint(idx%8))
}

// numBytesInBitArray returns the minimum number of bytes necessary to
// store the given number of bits.
func numBytesInBitArray(numBits int) int {
	return (numBits + 7) / 8
}

// makeBitVec carves a bitmap (byte array intended to store bits) for
// the given number of bits out of its first argument. It returns the
// remainder of the first argument after the bitmap has been reserved
// into it.
func makeBitVec(src []byte, length int) (b, bitVec []byte) {
	nullBitmapNumBytes := numBytesInBitArray(length)
	return src[nullBitmapNumBytes:], src[:nullBitmapNumBytes]
}

const hasNullFlag = 1 << 4

// encodeArrayHeader is used by encodeArray to encode the header
// at the beginning of the value encoding.
func encodeArrayHeader(h arrayHeader, buf []byte) ([]byte, error) {
	// The header byte we append here is formatted as follows:
	// * The low 4 bits encode the number of dimensions in the array.
	// * The high 4 bits are flags, with the lowest representing whether the array
	//   contains NULLs, and the rest reserved.
	headerByte := h.numDimensions
	if h.hasNulls {
		headerByte = headerByte | hasNullFlag
	}
	buf = append(buf, byte(headerByte))
	buf = encoding.EncodeValueTag(buf, encoding.NoColumnID, h.elementType)
	buf = encoding.EncodeNonsortingUvarint(buf, h.length)
	return buf, nil
}

// decodeArrayHeader is used by decodeArray to decode the header at
// the beginning of the value encoding.
func decodeArrayHeader(b []byte) (arrayHeader, []byte, error) {
	if len(b) < 2 {
		return arrayHeader{}, b, errors.Errorf("buffer too small")
	}
	hasNulls := b[0]&hasNullFlag != 0
	b = b[1:]
	_, dataOffset, _, encType, err := encoding.DecodeValueTag(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	b = b[dataOffset:]
	b, _, length, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	nullBitmap := []byte(nil)
	if hasNulls {
		b, nullBitmap = makeBitVec(b, int(length))
	}
	return arrayHeader{
		hasNulls: hasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   encType,
		length:        length,
		nullBitmap:    nullBitmap,
	}, b, nil
}

// datumTypeToArrayElementEncodingType decides an encoding type to
// place in the array header given a datum type. The element encoding
// type is then used to encode/decode array elements.
func datumTypeToArrayElementEncodingType(t types.T) (encoding.Type, error) {
	switch t {
	case types.Int:
		return encoding.Int, nil
	case types.Oid:
		return encoding.Int, nil
	case types.Float:
		return encoding.Float, nil
	case types.Decimal:
		return encoding.Decimal, nil
	case types.Bytes, types.String, types.Name:
		return encoding.Bytes, nil
	case types.Timestamp, types.TimestampTZ:
		return encoding.Time, nil
	// Note: types.Date was incorrectly mapped to encoding.Time when arrays were
	// first introduced. If any 1.1 users used date arrays, they would have been
	// persisted with incorrect elementType values.
	case types.Date, types.Time:
		return encoding.Int, nil
	case types.Interval:
		return encoding.Duration, nil
	case types.Bool:
		return encoding.True, nil
	case types.UUID:
		return encoding.UUID, nil
	case types.INet:
		return encoding.IPAddr, nil
	default:
		if t.FamilyEqual(types.FamCollatedString) {
			return encoding.Bytes, nil
		}
		return 0, errors.Errorf("Don't know encoding type for %s", t)
	}
}

// encodeArrayElement appends the encoded form of one array element to
// the target byte buffer.
func encodeArrayElement(b []byte, d tree.Datum) ([]byte, error) {
	switch t := d.(type) {
	case *tree.DInt:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DString:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DBytes:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DFloat:
		return encoding.EncodeUntaggedFloatValue(b, float64(*t)), nil
	case *tree.DBool:
		return encoding.EncodeBoolValue(b, encoding.NoColumnID, bool(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeUntaggedDecimalValue(b, &t.Decimal), nil
	case *tree.DDate:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DTime:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DTimestamp:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeUntaggedDurationValue(b, t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUntaggedUUIDValue(b, t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeUntaggedIPAddrValue(b, t.IPAddr), nil
	case *tree.DOid:
		return encoding.EncodeUntaggedIntValue(b, int64(t.DInt)), nil
	case *tree.DCollatedString:
		return encoding.EncodeUntaggedBytesValue(b, []byte(t.Contents)), nil
	}
	return nil, errors.Errorf("don't know how to encode %s", d)
}
