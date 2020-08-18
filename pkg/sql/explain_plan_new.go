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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// explainNewPlanNode implements EXPLAIN (PLAN); it produces the output of
// EXPLAIN given an explain.Plan.
//
// TODO(radu): move and rename this once explainPlanNode is removed.
type explainNewPlanNode struct {
	flags explain.Flags
	plan  *explain.Plan
	run   explainNewPlanNodeRun

	columns sqlbase.ResultColumns
}

type explainNewPlanNodeRun struct {
	results *valuesNode
}

func (e *explainNewPlanNode) startExec(params runParams) error {
	realPlan := e.plan.WrappedPlan.(*planComponents)
	distribution, willVectorize := explainGetDistributedAndVectorized(params, realPlan)

	ob := explain.NewOutputBuilder(e.flags)
	if err := emitExplain(ob, params.p.ExecCfg().Codec, e.plan, distribution, willVectorize); err != nil {
		return err
	}
	v := params.p.newContainerValuesNode(e.columns, 0)
	for _, row := range ob.BuildExplainRows() {
		if _, err := v.rows.AddRow(params.ctx, row); err != nil {
			return err
		}
	}
	e.run.results = v

	return nil
}

func emitExplain(
	ob *explain.OutputBuilder,
	codec keys.SQLCodec,
	explainPlan *explain.Plan,
	distribution physicalplan.PlanDistribution,
	vectorized bool,
) error {
	ob.AddField("distribution", distribution.String())
	ob.AddField("vectorized", fmt.Sprintf("%t", vectorized))
	spanFormatFn := func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string {
		var tabDesc *sqlbase.ImmutableTableDescriptor
		var idxDesc *descpb.IndexDescriptor
		if table.IsVirtualTable() {
			tabDesc = table.(*optVirtualTable).desc
			idxDesc = index.(*optVirtualIndex).desc
		} else {
			tabDesc = table.(*optTable).desc
			idxDesc = index.(*optIndex).desc
		}
		spans, err := generateScanSpans(codec, tabDesc, idxDesc, scanParams)
		if err != nil {
			return err.Error()
		}
		// skip is how many fields to skip when pretty-printing spans.
		// Usually 2, but can be 4 when running EXPLAIN from a tenant since there
		// will be an extra tenant prefix and ID. For example:
		//  - /51/1/1 is a key read as a system tenant where the first two values
		//    are the table ID and the index ID.
		//  - /Tenant/10/51/1/1 is a key read as a non-system tenant where the first
		//    four values are the special tenant prefix byte and tenant ID, followed
		//    by the table ID and the index ID.
		skip := 2
		if !codec.ForSystemTenant() {
			skip = 4
		}
		return sqlbase.PrettySpans(idxDesc, spans, skip)
	}

	return explain.Emit(explainPlan, ob, spanFormatFn)
}

func (e *explainNewPlanNode) Next(params runParams) (bool, error) { return e.run.results.Next(params) }
func (e *explainNewPlanNode) Values() tree.Datums                 { return e.run.results.Values() }

func (e *explainNewPlanNode) Close(ctx context.Context) {
	e.plan.Root.WrappedNode().(planNode).Close(ctx)
	for i := range e.plan.Subqueries {
		e.plan.Subqueries[i].Root.(*explain.Node).WrappedNode().(planNode).Close(ctx)
	}
	for i := range e.plan.Checks {
		e.plan.Checks[i].WrappedNode().(planNode).Close(ctx)
	}
	if e.run.results != nil {
		e.run.results.Close(ctx)
	}
}
