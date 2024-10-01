// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treebin"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
		_ = overloads.ForEachUnaryOp(func(o *tree.UnaryOp) error {
			c := sqltelemetry.UnaryOpCounter(opName, o.Typ.String())
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
			return nil
		})
	}

	for op, overloads := range tree.BinOps {
		opName := treebin.BinaryOpName(op)
		_ = overloads.ForEachBinOp(func(o *tree.BinOp) error {
			lname := o.LeftType.String()
			rname := o.RightType.String()
			c := sqltelemetry.BinOpCounter(opName, lname, rname)
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
			return nil
		})
	}

	for op, overloads := range tree.CmpOps {
		opName := treecmp.ComparisonOpName(op)
		_ = overloads.ForEachCmpOp(func(o *tree.CmpOp) error {
			lname := o.LeftType.String()
			rname := o.RightType.String()
			c := sqltelemetry.CmpOpCounter(opName, lname, rname)
			o.OnTypeCheck = func() {
				telemetry.Inc(c)
			}
			return nil
		})
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
	tree.OnTypeCheckJSONBSubscript = func() {
		telemetry.Inc(sqltelemetry.JSONBSubscriptCounter)
	}
	tree.OnTypeCheckArrayFlatten = func() {
		telemetry.Inc(sqltelemetry.ArrayFlattenCounter)
	}
	tree.OnCastTypeCheck = make(map[tree.CastCounterType]func())
	for fromID := range types.Family_name {
		for toID := range types.Family_name {
			from := types.Family(fromID)
			to := types.Family(toID)
			var c telemetry.Counter
			switch {
			case from == types.ArrayFamily && to == types.ArrayFamily:
				c = sqltelemetry.ArrayCastCounter
			case from == types.TupleFamily && to == types.TupleFamily:
				c = sqltelemetry.TupleCastCounter
			case from == types.EnumFamily && to == types.EnumFamily:
				c = sqltelemetry.EnumCastCounter
			default:
				c = sqltelemetry.CastOpCounter(string(from.Name()), string(to.Name()))
			}
			tree.OnCastTypeCheck[tree.CastCounterType{From: from, To: to}] = func() {
				telemetry.Inc(c)
			}
		}
	}
}
