// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// valuesideEncodeCol is the vector version of valueside.Encode.
func valuesideEncodeCol(
	appendTo []byte, colID valueside.ColumnIDDelta, vec *coldata.Vec, row int,
) ([]byte, error) {
	if vec.Nulls().NullAt(row) {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch typ := vec.Type(); typ.Family() {
	case types.BoolFamily:
		bs := vec.Bool()
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bs[row]), nil
	case types.IntFamily, types.DateFamily:
		var val int64
		switch typ.Width() {
		case 16:
			is := vec.Int16()
			val = int64(is[row])
		case 32:
			is := vec.Int32()
			val = int64(is[row])
		default:
			is := vec.Int64()
			val = is[row]
		}
		return encoding.EncodeIntValue(appendTo, uint32(colID), val), nil
	case types.FloatFamily:
		fs := vec.Float64()
		return encoding.EncodeFloatValue(appendTo, uint32(colID), fs[row]), nil
	case types.DecimalFamily:
		ds := vec.Decimal()
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &ds[row]), nil
	case types.BytesFamily, types.StringFamily, types.EnumFamily:
		b := vec.Bytes().Get(row)
		return encoding.EncodeBytesValue(appendTo, uint32(colID), b), nil
	case types.JsonFamily:
		j := vec.JSON().Get(row)
		encoded, err := json.EncodeJSON(nil, j)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), encoded), nil
	case types.TimestampFamily, types.TimestampTZFamily:
		t := vec.Timestamp()[row]
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t), nil
	case types.IntervalFamily:
		d := vec.Interval()[row]
		return encoding.EncodeDurationValue(appendTo, uint32(colID), d), nil
	case types.UuidFamily:
		b := vec.Bytes().Get(row)
		u, err := uuid.FromBytes(b)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), u), nil
	default:
		return valueside.Encode(appendTo, colID, vec.Datum().Get(row).(tree.Datum))
	}
}
