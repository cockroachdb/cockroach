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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/errors"
)

func constructPlan(
	planner *planner,
	root exec.Node,
	subqueries []exec.Subquery,
	cascades []exec.Cascade,
	checks []exec.Node,
) (exec.Plan, error) {
	res := &planTop{
		// TODO(radu): these fields can be modified by planning various opaque
		// statements. We should have a cleaner way of plumbing these.
		avoidBuffering:  planner.curPlan.avoidBuffering,
		auditEvents:     planner.curPlan.auditEvents,
		instrumentation: planner.curPlan.instrumentation,
	}
	assignPlan := func(plan *planMaybePhysical, node exec.Node) {
		switch n := node.(type) {
		case planNode:
			plan.planNode = n
		case planMaybePhysical:
			*plan = n
		default:
			panic(fmt.Sprintf("unexpected node type %T", node))
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
