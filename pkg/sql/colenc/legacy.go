// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colenc

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// MarshalLegacy is the vectorized version of the row based valueside.MarshalLegacy.
// It plucks off the vectorized types for special handling and delegates the rest.
func MarshalLegacy(colType *types.T, vec *coldata.Vec, row int) (roachpb.Value, error) {
	var r roachpb.Value

	if vec.Nulls().NullAt(row) {
		return r, nil
	}

	switch colType.Family() {
	case types.BoolFamily:
		if vec.Type().Family() == types.BoolFamily {
			r.SetBool(vec.Bool()[row])
			return r, nil
		}
	case types.IntFamily, types.DateFamily:
		if vec.Type().Family() == types.IntFamily || vec.Type().Family() == types.DateFamily {
			var i int64
			switch vec.Type().Width() {
			case 16:
				is := vec.Int16()
				i = int64(is[row])
			case 32:
				is := vec.Int32()
				i = int64(is[row])
			default:
				is := vec.Int64()
				i = is[row]
			}
			r.SetInt(i)
			return r, nil
		}
	case types.FloatFamily:
		if vec.Type().Family() == types.FloatFamily {
			r.SetFloat(vec.Float64()[row])
			return r, nil
		}
	case types.DecimalFamily:
		if vec.Type().Family() == types.DecimalFamily {
			err := r.SetDecimal(&vec.Decimal()[row])
			return r, err
		}
	case types.StringFamily, types.BytesFamily, types.UuidFamily, types.EnumFamily:
		switch vec.Type().Family() {
		case types.StringFamily, types.BytesFamily, types.UuidFamily, types.EnumFamily:
			b := vec.Bytes().Get(row)
			r.SetString(encoding.UnsafeConvertBytesToString(b))
			return r, nil
		}
	case types.TimestampFamily, types.TimestampTZFamily:
		switch vec.Type().Family() {
		case types.TimestampFamily, types.TimestampTZFamily:
			t := vec.Timestamp().Get(row)
			r.SetTime(t)
			return r, nil
		}
	case types.IntervalFamily:
		if vec.Type().Family() == types.IntervalFamily {
			err := r.SetDuration(vec.Interval()[row])
			return r, err
		}
	case types.JsonFamily:
		if vec.Type().Family() == types.JsonFamily {
			j := vec.JSON().Get(row)
			data, err := json.EncodeJSON(nil, j)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	default:
		return valueside.MarshalLegacy(colType, vec.Datum().Get(row).(tree.Datum))
	}
	return r, errors.AssertionFailedf("mismatched type %q vs %q", vec.Type(), colType.Family())
}
