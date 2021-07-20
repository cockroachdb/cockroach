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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the idx'th position of the input exec.Vec.
// See the analog in sqlbase/column_type_encoding.go.
func DecodeTableValueToCol(
	da *rowenc.DatumAlloc,
	vec coldata.Vec,
	idx int,
	typ encoding.Type,
	dataOffset int,
	valTyp *types.T,
	b []byte,
) ([]byte, error) {
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vec.Nulls().SetNull(idx)
		return b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valTyp.Family() != types.BoolFamily {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatumToCol(da, vec, idx, valTyp, b)
}

// decodeUntaggedDatum is used to decode a Datum whose type is known,
// and which doesn't have a value tag (either due to it having been
// consumed already or not having one in the first place). It writes the result
// to the idx'th position of the input coldata.Vec.
//
// This is used to decode datums encoded using value encoding.
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
// See the analog in sqlbase/column_type_encoding.go.
func decodeUntaggedDatumToCol(
	da *rowenc.DatumAlloc, vec coldata.Vec, idx int, t *types.T, buf []byte,
) ([]byte, error) {
	var err error
	switch t.Family() {
	case types.BoolFamily:
		var b bool
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		buf, b, err = encoding.DecodeBoolValue(buf)
		vec.Bool()[idx] = b
	case types.BytesFamily, types.StringFamily:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vec.Bytes().Set(idx, data)
	case types.DateFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		vec.Int64()[idx] = i
	case types.DecimalFamily:
		buf, err = encoding.DecodeIntoUntaggedDecimalValue(&vec.Decimal()[idx], buf)
	case types.FloatFamily:
		var f float64
		buf, f, err = encoding.DecodeUntaggedFloatValue(buf)
		vec.Float64()[idx] = f
	case types.IntFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		switch t.Width() {
		case 16:
			vec.Int16()[idx] = int16(i)
		case 32:
			vec.Int32()[idx] = int32(i)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vec.Int64()[idx] = i
		}
	case types.JsonFamily:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vec.JSON().Bytes.Set(idx, data)
	case types.UuidFamily:
		var data uuid.UUID
		buf, data, err = encoding.DecodeUntaggedUUIDValue(buf)
		// TODO(yuzefovich): we could peek inside the encoding package to skip a
		// couple of conversions.
		if err != nil {
			return buf, err
		}
		vec.Bytes().Set(idx, data.GetBytes())
	case types.TimestampFamily, types.TimestampTZFamily:
		var t time.Time
		buf, t, err = encoding.DecodeUntaggedTimeValue(buf)
		vec.Timestamp()[idx] = t
	case types.IntervalFamily:
		var d duration.Duration
		buf, d, err = encoding.DecodeUntaggedDurationValue(buf)
		vec.Interval()[idx] = d
	// Types backed by tree.Datums.
	default:
		var d tree.Datum
		d, buf, err = rowenc.DecodeUntaggedDatum(da, t, buf)
		if err != nil {
			return buf, err
		}
		vec.Datum().Set(idx, d)
	}
	return buf, err
}
