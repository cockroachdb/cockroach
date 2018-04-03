// Copyright 2016 The Cockroach Authors.
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
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func initBackfillerSpec(
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
	evalCtx *tree.EvalContext,
) (distsqlrun.BackfillerSpec, error) {
	ret := distsqlrun.BackfillerSpec{
		Table:       desc,
		Duration:    duration,
		ChunkSize:   chunkSize,
		OtherTables: otherTables,
		ReadAsOf:    readAsOf,
	}
	switch backfillType {
	case indexBackfill:
		ret.Type = distsqlrun.BackfillerSpec_Index
	case columnBackfill:
		ret.Type = distsqlrun.BackfillerSpec_Column

		// Get update expressions for changed columns. This includes doing name
		// resolution for computed columns so that it doesn't have to be done in
		// the backfiller.
		added, dropped := distsqlrun.GetColumnMutations(&desc)
		updateCols := append(added, dropped...)
		defaultExprs, err := sqlbase.MakeDefaultExprs(
			added, &transform.ExprTransformContext{}, evalCtx,
		)
		if err != nil {
			return distsqlrun.BackfillerSpec{}, err
		}
		var txCtx transform.ExprTransformContext
		computedExprs, err := sqlbase.MakeComputedExprs(added, &desc, tree.NewUnqualifiedTableName(tree.Name(desc.Name)), &txCtx, evalCtx)
		if err != nil {
			return distsqlrun.BackfillerSpec{}, err
		}

		if len(dropped) > 0 || len(defaultExprs) > 0 || len(computedExprs) > 0 {
			// Populate default or computed values.
			updateExprs := make([]tree.TypedExpr, len(updateCols))
			for j, col := range added {
				if col.IsComputed() {
					updateExprs[j] = computedExprs[j]
				} else if defaultExprs == nil || defaultExprs[j] == nil {
					updateExprs[j] = tree.DNull
				} else {
					updateExprs[j] = defaultExprs[j]
				}
			}
			for j := range dropped {
				updateExprs[j+len(added)] = tree.DNull
			}
			ret.UpdateExprs = make([]distsqlrun.Expression, len(updateExprs))
			for i, e := range updateExprs {
				ret.UpdateExprs[i] = distsqlplan.MakeExpression(e, evalCtx, nil)
			}
		}
	default:
		return distsqlrun.BackfillerSpec{}, errors.Errorf("bad backfill type %d", backfillType)
	}
	return ret, nil
}

// createBackfiller generates a plan consisting of index/column backfiller
// processors, one for each node that has spans that we are reading. The plan is
// finalized.
func (dsp *DistSQLPlanner) createBackfiller(
	planCtx *planningCtx,
	backfillType backfillType,
	desc sqlbase.TableDescriptor,
	duration time.Duration,
	chunkSize int64,
	spans []roachpb.Span,
	otherTables []sqlbase.TableDescriptor,
	readAsOf hlc.Timestamp,
) (physicalPlan, error) {
	spec, err := initBackfillerSpec(backfillType, desc, duration, chunkSize, otherTables, readAsOf, planCtx.EvalContext())
	if err != nil {
		return physicalPlan{}, err
	}

	spanPartitions, err := dsp.partitionSpans(planCtx, spans)
	if err != nil {
		return physicalPlan{}, err
	}

	var p physicalPlan
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(spanPartitions))
	for i, sp := range spanPartitions {
		ib := &distsqlrun.BackfillerSpec{}
		*ib = spec
		ib.Spans = make([]distsqlrun.TableReaderSpan, len(sp.spans))
		for j := range sp.spans {
			ib.Spans[j].Span = sp.spans[j]
		}

		proc := distsqlplan.Processor{
			Node: sp.node,
			Spec: distsqlrun.ProcessorSpec{
				Core:   distsqlrun.ProcessorCoreUnion{Backfiller: ib},
				Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
			},
		}

		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}
	dsp.FinalizePlan(planCtx, &p)
	return p, nil
}
