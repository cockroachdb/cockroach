package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// CTASPlanResultTypes is the result types for EXPORT plans.
var CTASPlanResultTypes = []types.T{
	*types.Bytes, // rows
}

// PlanAndRunCTAS plans and runs the CREATE TABLE AS command.
func PlanAndRunCTAS(
	ctx context.Context,
	dsp *DistSQLPlanner,
	evalCtx *extendedEvalContext,
	txn *client.Txn,
	canDistribute bool,
	isLocal bool,
	in planNode,
	out distsqlpb.ProcessorCoreUnion,
	recv *DistSQLReceiver,
) {
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, txn)
	planCtx.isLocal = isLocal

	p, err := dsp.createPlanForNode(planCtx, in)
	if err != nil {
		recv.SetError(errors.Wrapf(err, "constructing distSQL plan"))
		return
	}

	p.AddNoGroupingStage(
		out, distsqlpb.PostProcessSpec{}, CTASPlanResultTypes, distsqlpb.Ordering{},
	)

	// The bulk row writers will emit a binary encoded BulkOpSummary.
	p.PlanToStreamColMap = []int{0}
	p.ResultTypes = CTASPlanResultTypes

	dsp.FinalizePlan(planCtx, &p)
	dsp.Run(planCtx, txn, &p, recv, evalCtx, nil /* finishedSetupFn */)
}
