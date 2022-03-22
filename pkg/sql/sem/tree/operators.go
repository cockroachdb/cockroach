// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// This file implements the generation of unique names for every
// operator overload.
//
// The historical first purpose of generating these names is to be used
// as telemetry keys, for feature usage reporting.

// Detailed counter name generation follows.
//
// We pre-allocate the counter objects upfront here and later use
// Inc(), to avoid the hash map lookup in telemetry.Count upon type
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

func init() {
	// Label the unary operators.
	for op, overloads := range UnaryOps {
		if int(op) >= len(unaryOpName) || unaryOpName[op] == "" {
			panic(errors.AssertionFailedf("missing name for operator %q", op.String()))
		}
		opName := unaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*UnaryOp)
			o.counter = sqltelemetry.UnaryOpCounter(opName, o.Typ.String())
		}
	}

	// Label the comparison operators.
	for op, overloads := range CmpOps {
		opName := treecmp.ComparisonOpName(op)
		for _, impl := range overloads {
			o := impl.(*CmpOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			o.counter = sqltelemetry.CmpOpCounter(opName, lname, rname)
		}
	}

	// Label the binary operators.
	for op, overloads := range BinOps {
		opName := treebin.BinaryOpName(op)
		for _, impl := range overloads {
			o := impl.(*BinOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			o.counter = sqltelemetry.BinOpCounter(opName, lname, rname)
		}
	}
}
