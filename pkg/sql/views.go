// Copyright 2015 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// planDependencyInfo collects the dependencies related to a single
// table -- which index and columns are being depended upon.
type planDependencyInfo struct {
	// desc is a reference to the descriptor for the table being
	// depended on.
	desc *sqlbase.TableDescriptor
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
		fmt.Fprintf(&buf, "%d (%q):", id, tree.ErrNameString(&deps.desc.Name))
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
	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
	p.avoidCachedDescriptors = true

	// Request dependency tracking.
	defer func(prev planDependencies) { p.curPlan.deps = prev }(p.curPlan.deps)
	p.curPlan.deps = make(planDependencies)

	// Request star detection
	defer func(prev bool) { p.curPlan.hasStar = prev }(p.curPlan.hasStar)
	p.curPlan.hasStar = false

	// Now generate the source plan.
	sourcePlan, err := p.Select(ctx, viewSelect, []types.T{})
	if err != nil {
		return nil, nil, err
	}
	// The plan will not be needed further.
	defer sourcePlan.Close(ctx)

	// TODO(a-robinson): Support star expressions as soon as we can (#10028).
	if p.curPlan.hasStar {
		return nil, nil, fmt.Errorf("views do not currently support * expressions")
	}

	return p.curPlan.deps, planColumns(sourcePlan), nil
}
