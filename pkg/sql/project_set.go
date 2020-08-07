// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// projectSetNode zips through a list of generators for every row of
// the table source.
//
// Reminder, for context: the functional zip over iterators a,b,c
// returns tuples of values from a,b,c picked "simultaneously". NULLs
// are used when an iterator is "shorter" than another. For example:
//
//    zip([1,2,3], ['a','b']) = [(1,'a'), (2,'b'), (3, null)]
//
// In this context, projectSetNode corresponds to a relational
// operator project(R, a, b, c, ...) which, for each row in R,
// produces all the rows produced by zip(a, b, c, ...) with the values
// of R prefixed. Formally, this performs a lateral cross join of R
// with zip(a,b,c).
type projectSetNode struct {
	source planNode
	projectSetPlanningInfo
}

// projectSetPlanningInfo is a helper struct that is extracted from
// projectSetNode to be reused during physical planning.
type projectSetPlanningInfo struct {
	// columns contains all the columns from the source, and then
	// the columns from the generators.
	columns colinfo.ResultColumns

	// numColsInSource is the number of columns in the source plan, i.e.
	// the number of columns at the beginning of rowBuffer that do not
	// contain SRF results.
	numColsInSource int

	// exprs are the constant-folded, type checked expressions specified
	// in the ROWS FROM syntax. This can contain many kinds of expressions
	// (anything that is "function-like" including COALESCE, NULLIF) not just
	// SRFs.
	exprs tree.TypedExprs

	// numColsPerGen indicates how many columns are produced by
	// each entry in `exprs`.
	numColsPerGen []int
}

func (n *projectSetNode) startExec(runParams) error {
	panic("projectSetNode can't be run in local mode")
}

func (n *projectSetNode) Next(params runParams) (bool, error) {
	panic("projectSetNode can't be run in local mode")
}

func (n *projectSetNode) Values() tree.Datums {
	panic("projectSetNode can't be run in local mode")
}

func (n *projectSetNode) Close(ctx context.Context) {
	n.source.Close(ctx)
}
