// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

func constructPlan(
	planner *planner,
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
	usesPlanNodeRepresentation bool,
) (exec.Plan, error) {
	res := &planTop{
		// TODO(radu): these fields can be modified by planning various opaque
		// statements. We should have a cleaner way of plumbing these.
		avoidBuffering:  planner.curPlan.avoidBuffering,
		auditEvents:     planner.curPlan.auditEvents,
		instrumentation: planner.curPlan.instrumentation,
	}
	assignPlan := func(plan *planMaybePhysical, node exec.Node) {
		if usesPlanNodeRepresentation {
			plan.planNode = node.(planNode)
		} else {
			*plan = node.(planMaybePhysical)
		}
	}
	assignPlan(&res.main, root)
	if len(subqueries) > 0 {
		res.subqueryPlans = make([]subquery, len(subqueries))
		for i := range subqueries {
			in := &subqueries[i]
			out := &res.subqueryPlans[i]
			out.subquery = in.ExprNode
			switch in.Mode {
			case exec.SubqueryExists:
				out.execMode = rowexec.SubqueryExecModeExists
			case exec.SubqueryOneRow:
				out.execMode = rowexec.SubqueryExecModeOneRow
			case exec.SubqueryAnyRows:
				out.execMode = rowexec.SubqueryExecModeAllRowsNormalized
			case exec.SubqueryAllRows:
				out.execMode = rowexec.SubqueryExecModeAllRows
			default:
				return nil, errors.Errorf("invalid SubqueryMode %d", in.Mode)
			}
			out.expanded = true
			assignPlan(&out.plan, in.Root)
		}
	}
	if len(cascades) > 0 {
		res.cascades = make([]cascadeMetadata, len(cascades))
		for i := range cascades {
			res.cascades[i].Cascade = cascades[i]
		}
	}
	if len(checks) > 0 {
		res.checkPlans = make([]checkPlan, len(checks))
		for i := range checks {
			assignPlan(&res.checkPlans[i].plan, checks[i])
		}
	}

	return res, nil
}

// makeScanColumnsConfig builds a scanColumnsConfig struct by constructing a
// list of descriptor IDs for columns in the given cols set. Columns are
// identified by their ordinal position in the table schema.
func makeScanColumnsConfig(table cat.Table, cols exec.TableColumnOrdinalSet) scanColumnsConfig {
	// Set visibility=execinfra.ScanVisibilityPublicAndNotPublic, since all
	// columns in the "cols" set should be projected, regardless of whether
	// they're public or non- public. The caller decides which columns to
	// include (or not include). Note that when wantedColumns is non-empty,
	// the visibility flag will never trigger the addition of more columns.
	colCfg := scanColumnsConfig{
		wantedColumns: make([]tree.ColumnID, 0, cols.Len()),
		visibility:    execinfra.ScanVisibilityPublicAndNotPublic,
	}
	for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
		desc := table.Column(c).(*sqlbase.ColumnDescriptor)
		colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(desc.ID))
	}
	return colCfg
}
