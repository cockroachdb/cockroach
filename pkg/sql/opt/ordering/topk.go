// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func TopKCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// TopK orders its own input, so the ordering it can provide is its own.

	// TODO(harding): In some cases it seems like the child's ordering could also
	// be passed through if it's compatible, as limit does. I think the following
	// line, taken from limitOrOffsetCanProvideOrdering, does this. Please confirm
	// that this is expected behavior for topk sort.
	return required.Intersects(expr.Private().(*props.OrderingChoice))

}

func TopKBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	// TODO(harding): I think this is supposed to be returning the required order
	// for the output of topk, but I'm not certain this is what's supposed to be
	// returned by .*BuildProvided or that it's what I actually implemented
	// here...
	return trimProvided(expr.ProvidedPhysical().Ordering, required, &expr.Relational().FuncDeps)
}
