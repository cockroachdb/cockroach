// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc *sqlbase.ImmutableTableDescriptor
	// deps is the list of ways in which the current plan depends on
	// that table. There can be more than one entries when the same
	// table is used in different places. The entries can also be
	// different because some may reference an index and others may
	// reference only a subset of the table's columns.
	// Note: the "ID" field of TableDescriptor_Reference is not
	// (and cannot be) filled during plan construction / dependency
	// analysis because the descriptor that is using this dependency
	// has not been constructed yet.
	deps []sqlbase.TableDescriptor_Reference
}

// planDependencies maps the ID of a table depended upon to a list of
// detailed dependencies on that table.
type planDependencies map[sqlbase.ID]planDependencyInfo

// String implements the fmt.Stringer interface.
func (d planDependencies) String() string {
	var buf bytes.Buffer
	for id, deps := range d {
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameStringP(&deps.desc.Name))
		for _, dep := range deps.deps {
			buf.WriteString(" [")
			if dep.IndexID != 0 {
				fmt.Fprintf(&buf, "idx: %d ", dep.IndexID)
			}
			fmt.Fprintf(&buf, "cols: %v]", dep.ColumnIDs)
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// analyzeViewQuery extracts the set of dependencies (tables and views
// that this view's query depends on), together with the more detailed
// information about which indexes and columns are needed from each
// dependency. The set of columns from the view query's results is
// also returned.
func (p *planner) analyzeViewQuery(
	ctx context.Context, viewSelect *tree.Select,
) (planDependencies, sqlbase.ResultColumns, error) {
	// Request dependency tracking.
	defer func(prev planDependencies) { p.curPlan.deps = prev }(p.curPlan.deps)
	p.curPlan.deps = make(planDependencies)

	// Request star detection
	defer func(prev bool) { p.curPlan.hasStar = prev }(p.curPlan.hasStar)
	p.curPlan.hasStar = false

	// Now generate the source plan.
	sourcePlan, err := p.Select(ctx, viewSelect, []*types.T{})
	if err != nil {
		return nil, nil, err
	}
	// The plan will not be needed further.
	defer sourcePlan.Close(ctx)

	// TODO(a-robinson): Support star expressions as soon as we can (#10028).
	if p.curPlan.hasStar {
		return nil, nil, unimplemented.NewWithIssue(10028, "views do not currently support * expressions")
	}

	return p.curPlan.deps, planColumns(sourcePlan), nil
}
