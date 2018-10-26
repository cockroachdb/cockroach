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
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the idx'th position of the input exec.ColVec.
// See the analog in sqlbase/column_type_encoding.go.
func DecodeTableValueToCol(
	vec exec.ColVec,
	idx uint16,
	typ encoding.Type,
	dataOffset int,
	valTyp *sqlbase.ColumnType,
	b []byte,
) ([]byte, error) {
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vec.SetNull(idx)
		return b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valTyp.SemanticType != sqlbase.ColumnType_BOOL {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatumToCol(vec, idx, valTyp, b)
}

// decodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place). It writes the result
// to the idx'th position of the input exec.ColVec.
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
// See the analog in sqlbase/column_type_encoding.go.
func decodeUntaggedDatumToCol(
	vec exec.ColVec, idx uint16, t *sqlbase.ColumnType, buf []byte,
) ([]byte, error) {
	var err error
	switch t.SemanticType {
	case sqlbase.ColumnType_BOOL:
		var b bool
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		buf, b, err = encoding.DecodeBoolValue(buf)
		vec.Bool()[idx] = b
	case sqlbase.ColumnType_BYTES, sqlbase.ColumnType_STRING, sqlbase.ColumnType_NAME:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vec.Bytes()[idx] = data
	case sqlbase.ColumnType_DATE, sqlbase.ColumnType_OID:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		vec.Int64()[idx] = i
	case sqlbase.ColumnType_DECIMAL:
		buf, err = encoding.DecodeIntoUntaggedDecimalValue(&vec.Decimal()[idx], buf)
	case sqlbase.ColumnType_FLOAT:
		var f float64
		buf, f, err = encoding.DecodeUntaggedFloatValue(buf)
		vec.Float64()[idx] = f
	case sqlbase.ColumnType_INT:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		switch t.Width {
		case 8:
			vec.Int8()[idx] = int8(i)
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		case 0, 64:
			vec.Int64()[idx] = i
		default:
			return buf, errors.Errorf("unknown integer width %d", t.Width)
		}
	default:
		return buf, errors.Errorf("couldn't decode type %s", t)
	}
	return buf, err
}
