// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldataext

import (
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

// MakeVecHandler makes a tree.ValueHandler that stores values to a coldata.Vec.
func MakeVecHandler(v coldata.Vec) tree.ValueHandler {
	return &vecHandler{col: v}
}

type vecHandler struct {
	col coldata.Vec
	row int
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
// vec to avoid copying.
func (v *vecHandler) Decimal() *apd.Decimal {
	d := &v.col.Decimal()[v.row]
	v.row++
	return d
}

// Null implements tree.ValueHandler interface.
func (v *vecHandler) Null() {
	v.col.Nulls().SetNull(v.row)
	v.row++
}

// String is part of the tree.ValueHandler interface.
func (v *vecHandler) String(s string) {
	v.col.Bytes().Set(v.row, encoding.UnsafeConvertStringToBytes(s))
	v.row++
}

// Date is part of the tree.ValueHandler interface.
func (v *vecHandler) Date(d pgdate.Date) {
	v.col.Int64().Set(v.row, d.UnixEpochDaysWithOrig())
	v.row++
}

// Datum is part of the tree.ValueHandler interface.
func (v *vecHandler) Datum(d tree.Datum) {
	v.col.Datum().Set(v.row, d)
	v.row++
}

// Bool is part of the tree.ValueHandler interface.
func (v *vecHandler) Bool(b bool) {
	v.col.Bool().Set(v.row, b)
	v.row++
}

// Bytes is part of the tree.ValueHandler interface.
func (v *vecHandler) Bytes(b []byte) {
	v.col.Bytes().Set(v.row, b)
	v.row++
}

// Float is part of the tree.ValueHandler interface.
func (v *vecHandler) Float(f float64) {
	v.col.Float64().Set(v.row, f)
	v.row++
}

// Int is part of the tree.ValueHandler interface.
func (v *vecHandler) Int(i int64) {
	v.col.Int64().Set(v.row, i)
	v.row++
}

// Duration is part of the tree.ValueHandler interface.
func (v *vecHandler) Duration(d duration.Duration) {
	v.col.Interval().Set(v.row, d)
	v.row++
}

// JSON is part of the tree.ValueHandler interface.
func (v *vecHandler) JSON(j json.JSON) {
	v.col.JSON().Set(v.row, j)
	v.row++
}

// TimestampTZ is part of the tree.ValueHandler interface.
func (v *vecHandler) TimestampTZ(t time.Time) {
	v.col.Timestamp().Set(v.row, t)
	v.row++
}
