// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

// PickFromTuple picks the greatest (or least value) from a tuple.
func PickFromTuple(
	ctx context.Context, evalCtx *Context, greatest bool, args tree.Datums,
) (tree.Datum, error) {
	g := args[0]
	// Pick a greater (or smaller) value.
	for _, d := range args[1:] {
		var eval tree.Datum
		var err error
		if greatest {
			eval, err = evalComparison(ctx, evalCtx, treecmp.MakeComparisonOperator(treecmp.LT), g, d)
		} else {
			eval, err = evalComparison(ctx, evalCtx, treecmp.MakeComparisonOperator(treecmp.LT), d, g)
		}
		if err != nil {
			return nil, err
		}
		if eval == tree.DBoolTrue ||
			(eval == tree.DNull && g == tree.DNull) {
			g = d
		}
	}
	return g, nil
}
