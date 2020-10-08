// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// BuiltinCounter creates a telemetry counter for a built-in function.
// This is to be incremented upon type checking of a function application.
func BuiltinCounter(name, signature string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.builtins.%s%s", name, signature))
}

// UnaryOpCounter creates a telemetry counter for a scalar unary operator.
// This is to be incremented upon type checking of this type of scalar operation.
func UnaryOpCounter(op, typ string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.ops.un.%s %s", op, typ))
}

// CmpOpCounter creates a telemetry counter for a scalar comparison operator.
// This is to be incremented upon type checking of this type of scalar operation.
func CmpOpCounter(op, ltyp, rtyp string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.ops.cmp.%s %s %s", ltyp, op, rtyp))
}

// BinOpCounter creates a telemetry counter for a scalar binary operator.
// This is to be incremented upon type checking of this type of scalar operation.
func BinOpCounter(op, ltyp, rtyp string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.ops.bin.%s %s %s", ltyp, op, rtyp))
}

// CastOpCounter creates a telemetry counter for a scalar cast operator.
// This is to be incremented upon type checking of this type of scalar operation.
func CastOpCounter(ftyp, ttyp string) telemetry.Counter {
	return telemetry.GetCounterOnce(fmt.Sprintf("sql.plan.ops.cast.%s::%s", ftyp, ttyp))
}

// ArrayCastCounter is to be incremented when type checking all casts
// that involve arrays.  This separate telemetry counter is needed
// because an inter-array cast lands on `sql.plan.ops.cast` telemetry
// counter for the element type.
var ArrayCastCounter = telemetry.GetCounterOnce("sql.plan.ops.cast.arrays")

// EnumCastCounter is to be incremented when typechecking casts that
// are between enums.
var EnumCastCounter = telemetry.GetCounterOnce("sql.plan.ops.cast.enums")

// ArrayConstructorCounter is to be incremented upon type checking
// of ARRAY[...] expressions/
var ArrayConstructorCounter = telemetry.GetCounterOnce("sql.plan.ops.array.cons")

// ArrayFlattenCounter is to be incremented upon type checking
// of ARRAY(...) expressions.
var ArrayFlattenCounter = telemetry.GetCounterOnce("sql.plan.ops.array.flatten")

// ArraySubscriptCounter is to be incremented upon type checking an
// array subscript expression x[...].
var ArraySubscriptCounter = telemetry.GetCounterOnce("sql.plan.ops.array.ind")

// IfErrCounter is to be incremented upon type checking an
// IFERROR(...) expression or analogous.
var IfErrCounter = telemetry.GetCounterOnce("sql.plan.ops.iferr")

// LargeLShiftArgumentCounter is to be incremented upon evaluating a scalar
// expressions a << b when b is larger than 64 or negative.
var LargeLShiftArgumentCounter = telemetry.GetCounterOnce("sql.large_lshift_argument")

// LargeRShiftArgumentCounter is to be incremented upon evaluating a scalar
// expressions a >> b when b is larger than 64 or negative.
var LargeRShiftArgumentCounter = telemetry.GetCounterOnce("sql.large_rshift_argument")
