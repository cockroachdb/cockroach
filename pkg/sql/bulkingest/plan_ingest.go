// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func planIngest(
	ctx context.Context, execCtx sql.JobExecContext, tasks []execinfrapb.BulkMergeSpec_SST,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	// The IngestFileProcessor was introduced in v26.1. Ensure all nodes support it.
	if !execCtx.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V26_1) {
		return nil, nil, unimplemented.New("distributed merge",
			"ingest file processor requires all nodes to be running v26.1 or later")
	}

	planCtx, sqlInstanceIDs, err := execCtx.DistSQLPlanner().SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
	)
	if err != nil {
		return nil, nil, err
	}

	plan := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs))
	for i := range sqlInstanceIDs {
		corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
		corePlacement[i].Core.IngestFile = &execinfrapb.IngestFileSpec{
			SSTs: taskSlice(tasks, i, len(sqlInstanceIDs)),
		}
	}

	plan.AddNoInputStage(
		corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{},
		execinfrapb.Ordering{}, nil, /* finalizeLastStageCb */
	)

	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}

// taskSlice splits tasks across workers. It returns the contiguous slice of
// tasks assigned to workerId out of numWorkers workers. Workers may receive
// zero tasks if there are more workers than tasks.
func taskSlice[T any](tasks []T, workerId, numWorkers int) []T {
	// Split the tasks across the workers.
	taskPerWorker := max((len(tasks)+numWorkers-1)/numWorkers, 1)

	start := workerId * taskPerWorker
	end := min(start+taskPerWorker, len(tasks))

	if len(tasks) <= start {
		// There may be more workers than tasks.
		return nil
	}

	return tasks[start:end]
}
