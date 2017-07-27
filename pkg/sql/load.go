// Copyright 2017 The Cockroach Authors.
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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// LoadCSVDistributed TODO(dan): This entire method is a placeholder to get the
// distsql plumbing worked out while mjibson works on the new processors and
// router. The intention is to manually create the distsql plan, so we can have
// control over where the work is scheduled, but then use the normal distsql
// machinery for everything else. Currently, it runs a very simple flow just to
// make sure everything gets set up correctly. It is in no way representive of
// the actual flow that will be used for csv -> BACKUP, but is enough to get the
// flow setup worked out.
func LoadCSVDistributed(
	ctx context.Context,
	db *client.DB,
	evalCtx parser.EvalContext,
	nodes []roachpb.NodeDescriptor,
	// TODO(dan): Take the distSQLPlanner directly once the TODO on
	// PlanHookState is done.
	distPlannerI interface{},
) (int, error) {
	distPlanner := distPlannerI.(*distSQLPlanner)

	// Manually construct a plan that schedules a values processor on every
	// node, each of returns one row with a single column containing the node's
	// nodeID. These are passed back and locally summed.
	var p physicalPlan
	for _, node := range nodes {
		values := distsqlrun.ValuesCoreSpec{
			Columns: []distsqlrun.DatumInfo{{
				Encoding: sqlbase.DatumEncoding_VALUE,
				Type:     sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
			}},
			RawBytes: [][]byte{
				encoding.EncodeIntValue(nil, encoding.NoColumnID, int64(node.NodeID)),
			},
		}
		proc := distsqlplan.Processor{
			Node: node.NodeID,
			Spec: distsqlrun.ProcessorSpec{
				Core:    distsqlrun.ProcessorCoreUnion{Values: &values},
				Output:  []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
				StageID: 0,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}
	p.planToStreamColMap = []int{0}

	var rows *sqlbase.RowContainer
	runPlanFn := func(ctx context.Context, txn *client.Txn) error {
		planCtx := distPlanner.NewPlanningCtx(ctx, txn)
		// Because we're not going through the normal pathways, we have to set up
		// the nodeID -> nodeAddress map ourselves.
		for _, node := range nodes {
			planCtx.nodeAddresses[node.NodeID] = node.Address.String()
		}

		distPlanner.FinalizePlan(&planCtx, &p)

		ci := sqlbase.ColTypeInfoFromColTypes(
			[]sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}})
		rows = sqlbase.NewRowContainer(*evalCtx.ActiveMemAcc, ci, 0)
		recv, err := makeDistSQLReceiver(
			ctx, rows, nil, nil, txn, func(ts hlc.Timestamp) {},
		)
		if err != nil {
			return err
		}
		if err := distPlanner.Run(&planCtx, txn, &p, &recv, evalCtx); err != nil {
			return err
		}
		return nil
	}
	if err := db.Txn(ctx, runPlanFn); err != nil {
		return 0, err
	}

	var total int
	for i := 0; i < rows.Len(); i++ {
		row := rows.At(i)
		total += int(*row[0].(*parser.DInt))
	}
	return int(total), nil
}
