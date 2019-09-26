// Copyright 2015 The Cockroach Authors.
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
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// limitNode represents a node that limits the number of rows
// returned or only return them past a given number (offset).
type limitNode struct {
	plan       planNode
	countExpr  tree.TypedExpr
	offsetExpr tree.TypedExpr
	evaluated  bool
	count      int64
	offset     int64
}

func (n *limitNode) startExec(params runParams) error {
	panic("limitNode cannot be run in local mode")
}

func (n *limitNode) Next(params runParams) (bool, error) {
	panic("limitNode cannot be run in local mode")
}

func (n *limitNode) Values() tree.Datums {
	panic("limitNode cannot be run in local mode")
}

func (n *limitNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}

// evalLimit evaluates the Count and Offset fields. If Count is missing, the
// value is MaxInt64. If Offset is missing, the value is 0
func (n *limitNode) evalLimit(evalCtx *tree.EvalContext) error {
	n.count = math.MaxInt64
	n.offset = 0

	data := []struct {
		name string
		src  tree.TypedExpr
		dst  *int64
	}{
		{"LIMIT", n.countExpr, &n.count},
		{"OFFSET", n.offsetExpr, &n.offset},
	}

	for _, datum := range data {
		if datum.src != nil {
			dstDatum, err := datum.src.Eval(evalCtx)
			if err != nil {
				return err
			}

			if dstDatum == tree.DNull {
				// Use the default value.
				continue
			}

			dstDInt := tree.MustBeDInt(dstDatum)
			val := int64(dstDInt)
			if val < 0 {
				return fmt.Errorf("negative value for %s", datum.name)
			}
			*datum.dst = val
		}
	}
	n.evaluated = true
	return nil
}
