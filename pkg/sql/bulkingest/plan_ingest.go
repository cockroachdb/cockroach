// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func planIngest(
	ctx context.Context, execCtx sql.JobExecContext, tasks []execinfrapb.BulkMergeSpec_SST,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	planCtx, sqlInstanceIDs, err := execCtx.DistSQLPlanner().SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
	if err != nil {
		return nil, nil, err
	}

	plan := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs))
	for i := range sqlInstanceIDs {
		corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
		corePlacement[i].Core.IngestFile = &execinfrapb.IngestFileSpec{
			Ssts: taskSlice(tasks, i, len(sqlInstanceIDs)),
		}
	}

	plan.AddNoInputStage(
		corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{}, execinfrapb.Ordering{},
	)

	sql.FinalizePlan(ctx, planCtx, plan)

	return plan, planCtx, nil
}

// TODO(jeffswenson): unit test this
func taskSlice[T any](tasks []T, workerId, numWorkers int) []T {
	// Split the tasks across the workers.
	taskPerWorker := (len(tasks) + numWorkers - 1) / numWorkers

	start := workerId * taskPerWorker
	end := start + taskPerWorker
	if workerId == numWorkers-1 {
		end = len(tasks)
	}

	return tasks[start:end]
}
