// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ingeststopped

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// WaitForNoIngestingNodes contacts all nodes in the cluster and verifies that
// they are not ingesting data for the given job ID, and continues to try to do
// so for the specified duration until it succeeds or times out.
func WaitForNoIngestingNodes(
	ctx context.Context, execCtx sql.JobExecContext, job *jobs.Job, maxWait time.Duration,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "WaitForNoIngestingNodes")
	defer sp.Finish()

	retries := retry.StartWithCtx(ctx, retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second * 10,
	})

	started := timeutil.Now()

	var lastStatusUpdate time.Time
	const statusUpdateFrequency = time.Second * 30

	for retries.Next() {
		err := checkAllNodesForIngestingJob(ctx, execCtx, job.ID())
		if err == nil {
			break
		}
		log.Infof(ctx, "failed to verify job no longer importing on all nodes: %+v", err)

		if timeutil.Since(started) > maxWait {
			return err
		}

		if timeutil.Since(lastStatusUpdate) > statusUpdateFrequency {
			status := jobs.StatusMessage(fmt.Sprintf("waiting for all nodes to finish ingesting writing before proceeding: %s", err))
			if statusErr := job.NoTxn().UpdateStatusMessage(ctx, status); statusErr != nil {
				log.Warningf(ctx, "failed to update running status of job %d: %s", job.ID(), statusErr)
			} else {
				lastStatusUpdate = timeutil.Now()
			}
		}
	}
	return nil
}

func checkAllNodesForIngestingJob(
	ctx context.Context, execCtx sql.JobExecContext, jobID catpb.JobID,
) error {
	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	// TODO(dt): We should record which nodes were assigned ingestion processors
	// and then ensure we're reaching out to them specifically here, in particular
	// in the event a node that was importing is no longer in liveness but might
	// still be off ingesting.
	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return err
	}

	p := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDs))
	for i := range sqlInstanceIDs {
		corePlacement[i].SQLInstanceID = sqlInstanceIDs[i]
		corePlacement[i].Core.IngestStopped = &execinfrapb.IngestStoppedSpec{JobID: jobID}
	}

	p.AddNoInputStage(
		corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{}, execinfrapb.Ordering{},
	)
	sql.FinalizePlan(ctx, planCtx, p)

	res := sql.NewMetadataOnlyMetadataCallbackWriter()

	recv := sql.MakeDistSQLReceiver(
		ctx,
		res,
		tree.Ack,
		nil, /* rangeCache */
		nil, /* txn - the flow does not read or write the database */
		nil, /* clockUpdater */
		evalCtx.Tracing,
	)
	defer recv.Release()

	evalCtxCopy := *evalCtx
	dsp.Run(ctx, planCtx, nil, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	return res.Err()
}
