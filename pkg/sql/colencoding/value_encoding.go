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
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the idx'th position of the input exec.Vec.
// See the analog in sqlbase/column_type_encoding.go.
func DecodeTableValueToCol(
	vec coldata.Vec,
	idx uint16,
	typ encoding.Type,
	dataOffset int,
	valTyp *types.ColumnType,
	b []byte,
) ([]byte, error) {
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vec.SetNull(idx)
		return b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valTyp.SemanticType != types.BOOL {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatumToCol(vec, idx, valTyp, b)
}

// decodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place). It writes the result
// to the idx'th position of the input exec.Vec.
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
// See the analog in sqlbase/column_type_encoding.go.
func decodeUntaggedDatumToCol(
	vec coldata.Vec, idx uint16, t *types.ColumnType, buf []byte,
) ([]byte, error) {
	var err error
	switch t.SemanticType {
	case types.BOOL:
		var b bool
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		buf, b, err = encoding.DecodeBoolValue(buf)
		vec.Bool()[idx] = b
	case types.BYTES, types.STRING:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vec.Bytes()[idx] = data
	case types.DATE, types.OID:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		vec.Int64()[idx] = i
	case types.DECIMAL:
		buf, err = encoding.DecodeIntoUntaggedDecimalValue(&vec.Decimal()[idx], buf)
	case types.FLOAT:
		var f float64
		buf, f, err = encoding.DecodeUntaggedFloatValue(buf)
		vec.Float64()[idx] = f
	case types.INT:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		switch t.Width {
		case 8:
			vec.Int8()[idx] = int8(i)
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vec.Int64()[idx] = i
		}
	default:
		return buf, pgerror.NewAssertionErrorf(
			"couldn't decode type: %s", log.Safe(t))
	}
	return buf, err
}
