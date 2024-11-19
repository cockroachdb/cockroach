// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldataext

import (
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/errors"
)

// MakeVecHandler makes a tree.ValueHandler that stores values to a coldata.Vec.
func MakeVecHandler(vec *coldata.Vec) tree.ValueHandler {
	v := vecHandler{nulls: vec.Nulls()}
	switch vec.CanonicalTypeFamily() {
	case types.BoolFamily:
		v.bools = vec.Bool()
	case types.BytesFamily:
		v.bytes = vec.Bytes()
	case types.DecimalFamily:
		v.decimals = vec.Decimal()
	case types.IntFamily:
		switch vec.Type().Width() {
		case 16:
			v.int16s = vec.Int16()
		case 32:
			v.int32s = vec.Int32()
		default:
			v.ints = vec.Int64()
		}
	case types.FloatFamily:
		v.floats = vec.Float64()
	case types.TimestampTZFamily:
		v.timestamps = vec.Timestamp()
	case types.IntervalFamily:
		v.intervals = vec.Interval()
	case types.JsonFamily:
		v.jsons = vec.JSON()
	case typeconv.DatumVecCanonicalTypeFamily:
		v.datums = vec.Datum()
	default:
		colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", vec.Type()))
	}
	return &v
}

type vecHandler struct {
	nulls      *coldata.Nulls
	bools      coldata.Bools
	bytes      *coldata.Bytes
	decimals   coldata.Decimals
	int16s     coldata.Int16s
	int32s     coldata.Int32s
	ints       coldata.Int64s
	floats     coldata.Float64s
	timestamps coldata.Times
	intervals  coldata.Durations
	jsons      *coldata.JSONs
	datums     coldata.DatumVec
	row        int
}

var _ tree.ValueHandler = (*vecHandler)(nil)

// Reset is used to re-use a batch handler across batches.
func (v *vecHandler) Reset() {
	v.row = 0
}

// Len returns the current length of the vector.
func (v *vecHandler) Len() int {
	return v.row
}

// Decimal implements tree.ValueHandler interface. It returns a pointer into the
// vec to allow the decimal to be constructed in place which avoids expensive
// copying and temporary allocations.
func (v *vecHandler) Decimal() *apd.Decimal {
	d := &v.decimals[v.row]
	v.row++
	return d
}

// Null implements tree.ValueHandler interface.
func (v *vecHandler) Null() {
	v.nulls.SetNull(v.row)
	v.row++
}

// String is part of the tree.ValueHandler interface.
func (v *vecHandler) String(s string) {
	v.bytes.Set(v.row, encoding.UnsafeConvertStringToBytes(s))
	v.row++
}

// Date is part of the tree.ValueHandler interface.
func (v *vecHandler) Date(d pgdate.Date) {
	v.ints[v.row] = d.UnixEpochDaysWithOrig()
	v.row++
}

// Datum is part of the tree.ValueHandler interface.
func (v *vecHandler) Datum(d tree.Datum) {
	v.datums.Set(v.row, d)
	v.row++
}

// Bool is part of the tree.ValueHandler interface.
func (v *vecHandler) Bool(b bool) {
	v.bools[v.row] = b
	v.row++
}

// Bytes is part of the tree.ValueHandler interface.
func (v *vecHandler) Bytes(b []byte) {
	v.bytes.Set(v.row, b)
	v.row++
}

// Float is part of the tree.ValueHandler interface.
func (v *vecHandler) Float(f float64) {
	v.floats[v.row] = f
	v.row++
}

// Int16 is part of the tree.ValueHandler interface.
func (v *vecHandler) Int16(i int16) {
	v.int16s[v.row] = i
	v.row++
}

// Int32 is part of the tree.ValueHandler interface.
func (v *vecHandler) Int32(i int32) {
	v.int32s[v.row] = i
	v.row++
}

// Int is part of the tree.ValueHandler interface.
func (v *vecHandler) Int(i int64) {
	v.ints[v.row] = i
	v.row++
}

// Duration is part of the tree.ValueHandler interface.
func (v *vecHandler) Duration(d duration.Duration) {
	v.intervals[v.row] = d
	v.row++
}

// JSON is part of the tree.ValueHandler interface.
func (v *vecHandler) JSON(j json.JSON) {
	v.jsons.Set(v.row, j)
	v.row++
}

// TimestampTZ is part of the tree.ValueHandler interface.
func (v *vecHandler) TimestampTZ(t time.Time) {
	v.timestamps[v.row] = t
	v.row++
}
