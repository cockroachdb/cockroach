// Copyright 2019 The Cockroach Authors.
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
