// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// init injects the requisite telemetry components into the tree package.
// We pre-allocate the counter objects upfront here and later use
// Inc(), to avoid the hash map lookup in telemetry.Count upon type
// checking every scalar operator node.
func init() {
	for op, overloads := range tree.UnaryOps {
		if int(op) >= len(tree.UnaryOpName) || tree.UnaryOpName[op] == "" {
			panic(errors.AssertionFailedf("missing name for operator %q", op.String()))
		}
		opName := tree.UnaryOpName[op]
		for _, impl := range overloads {
			o := impl.(*tree.UnaryOp)
			c := sqltelemetry.UnaryOpCounter(opName, o.Typ.String())
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
		}
	}

	for op, overloads := range tree.BinOps {
		opName := treebin.BinaryOpName(op)
		for _, impl := range overloads {
			o := impl.(*tree.BinOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			c := sqltelemetry.BinOpCounter(opName, lname, rname)
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
		}
	}

	for op, overloads := range tree.CmpOps {
		opName := treecmp.ComparisonOpName(op)
		for _, impl := range overloads {
			o := impl.(*tree.CmpOp)
			lname := o.LeftType.String()
			rname := o.RightType.String()
			c := sqltelemetry.CmpOpCounter(opName, lname, rname)
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
		}
	}

	tree.OnTypeCheckIfErr = func() {
		telemetry.Inc(sqltelemetry.IfErrCounter)
	}
	tree.OnTypeCheckArrayConstructor = func() {
		telemetry.Inc(sqltelemetry.ArrayConstructorCounter)
	}
	tree.OnTypeCheckArraySubscript = func() {
		telemetry.Inc(sqltelemetry.ArraySubscriptCounter)
	}
	tree.OnTypeCheckArrayFlatten = func() {
		telemetry.Inc(sqltelemetry.ArrayFlattenCounter)
	}
}
