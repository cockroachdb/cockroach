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

package tree

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/lib/pq/oid"
)

// This file implements the generation of unique names for every
// operator overload.
//
// The historical first purpose of generating these names is to be used
// as telemetry keys, for feature usage reporting.

// Scalar operators will be counter as sql.ops.<kind>.<lhstype> <opname> <rhstype>.
const unOpCounterNameFmt = "sql.ops.un.%s %s"
const cmpOpCounterNameFmt = "sql.ops.cmp.%s %s %s"
const binOpCounterNameFmt = "sql.ops.bin.%s %s %s"

// Cast operators will be counted as sql.ops.cast.<fromtype>::<totype>.
const castCounterNameFmt = "sql.ops.cast.%s::%s"

// All casts that involve arrays will also be counted towards this
// feature counter.
var arrayCastCounter = telemetry.GetCounter("sql.ops.cast.arrays")

// Detailed counter name generation follows.
//
// We pre-allocate the counter objects upfront here and later use
// Inc(), to avoid the hash map lookup in telemetry.Count() upon type
// checking every scalar operator node.

// The logic that follows is also associated with a related feature in
// PostgreSQL, which may be implemented by CockroachDB in the future:
// exposing all the operators as unambiguous, non-overloaded built-in
// functions.  For example, in PostgreSQL, one can use `SELECT
// int8um(123)` to apply the int8-specific unary minus operator.
// This feature can be considered in the future for two reasons:
//
// 1. some pg applications may simply require the ability to use the
//    pg native operator built-ins. If/when this compatibility is
//    considered, care should be taken to tweak the string maps below
//    to ensure that the operator names generated here coincide with
//    those used in the postgres library.
//
// 2. since the operator built-in functions are non-overloaded, they
//    remove the requirement to disambiguate the type of operands
//    with the ::: (annotate_type) operator. This may be useful
//    to simplify/accelerate the serialization of scalar expressions
//    in distsql.
//

// makeOidName generates a short name for the given type OID.
func makeOidName(op fmt.Stringer, tOid oid.Oid) (string, bool) {
	oidName, ok := oid.TypeName[tOid]
	if !ok {
		return "", false
	}
	name := strings.ToLower(oidName)
	if strings.HasPrefix(name, "timestamp") {
		name = "ts" + name[9:]
	}
	return name, true
}

func init() {
	seen := map[string]struct{}{}

	// Label the unary operators.
	for op, overloads := range UnaryOps {
		if int(op) >= len(unaryOpName) || unaryOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := unaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*UnaryOp)
			uname, ok := makeOidName(op, o.Typ.Oid())
			if !ok {
				continue
			}
			name := fmt.Sprintf(unOpCounterNameFmt, opName, uname)
			if _, ok := seen[name]; ok {
				panic(fmt.Sprintf("duplicate name: %q", name))
			}
			o.counter = telemetry.GetCounter(name)
		}
	}

	// Label the comparison operators.
	for op, overloads := range CmpOps {
		if int(op) >= len(comparisonOpName) || comparisonOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := comparisonOpName[op]
		for _, impl := range overloads {
			o := impl.(*CmpOp)
			lname, lok := makeOidName(op, o.LeftType.Oid())
			rname, rok := makeOidName(op, o.RightType.Oid())
			if !lok || !rok {
				continue
			}
			name := fmt.Sprintf(cmpOpCounterNameFmt, lname, opName, rname)
			if _, ok := seen[name]; ok {
				panic(fmt.Sprintf("duplicate name: %q", name))
			}
			o.counter = telemetry.GetCounter(name)
		}
	}

	// Label the binary operators.
	for op, overloads := range BinOps {
		if int(op) >= len(binaryOpName) || binaryOpName[op] == "" {
			panic(fmt.Sprintf("missing name for operator %q", op.String()))
		}
		opName := binaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*BinOp)
			lname, lok := makeOidName(op, o.LeftType.Oid())
			rname, rok := makeOidName(op, o.RightType.Oid())
			if !lok || !rok {
				continue
			}
			name := fmt.Sprintf(binOpCounterNameFmt, lname, opName, rname)
			if _, ok := seen[name]; ok {
				panic(fmt.Sprintf("duplicate name: %q", name))
			}
			o.counter = telemetry.GetCounter(name)
		}
	}
}

// annotateCast produces an array of cast types decorated with cast
// type telemetry counters.
func annotateCast(toType types.T, fromTypes []types.T) []castInfo {
	ci := make([]castInfo, len(fromTypes))
	for i, fromType := range fromTypes {
		ci[i].fromT = fromType
	}
	var rname string
	if toType.FamilyEqual(types.FamArray) {
		rname = "array"
	} else {
		var rok bool
		rname, rok = makeOidName(nil, toType.Oid())
		if !rok {
			return ci
		}
	}

	for i, fromType := range fromTypes {
		var lname string
		if fromType.FamilyEqual(types.FamArray) {
			lname = "array"
		} else {
			var lok bool
			lname, lok = makeOidName(nil, fromType.Oid())
			if !lok {
				continue
			}
		}
		ci[i].counter = telemetry.GetCounter(fmt.Sprintf(castCounterNameFmt, lname, rname))
	}
	return ci
}
