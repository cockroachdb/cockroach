// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type inspectResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &inspectResumer{}

// Resume implements the Resumer interface
func (c *inspectResumer) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting INSPECT job")

	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	var knobs sql.InspectTestingKnobs
	if inspectKnobs := execCfg.InspectTestingKnobs; inspectKnobs != nil {
		knobs = *inspectKnobs
	}

	if err := maybeRunOnJobStartHook(knobs); err != nil {
		return err
	}

	pkSpans, err := c.getPrimaryIndexSpans(ctx, execCfg)
	if err != nil {
		return err
	}

	// TODO(149460): add a goroutine that will replan the job on topology changes
	plan, planCtx, err := c.planInspectProcessors(ctx, jobExecCtx, pkSpans)
	if err != nil {
		return err
	}

	if err := c.runInspectPlan(ctx, jobExecCtx, planCtx, plan); err != nil {
		return err
	}

	return c.markJobComplete(ctx)
}

// OnFailOrCancel implements the Resumer interface
func (c *inspectResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile implements the Resumer interface
func (c *inspectResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

func maybeRunOnJobStartHook(knobs sql.InspectTestingKnobs) error {
	if knobs.OnInspectJobStart != nil {
		return knobs.OnInspectJobStart()
	}
	return nil
}

// getPrimaryIndexSpans returns the primary index spans for all tables involved in
// the INSPECT job's checks.
func (c *inspectResumer) getPrimaryIndexSpans(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) ([]roachpb.Span, error) {
	details := c.job.Details().(jobspb.InspectDetails)

	spans := make([]roachpb.Span, 0, len(details.Checks))
	err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for i := range details.Checks {
			desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, details.Checks[i].TableID)
			if err != nil {
				return err
			}
			spans = append(spans, desc.PrimaryIndexSpan(execCfg.Codec))
		}
		return nil
	})
	return spans, err
}

// planInspectProcessors constructs the physical plan for the INSPECT job by
// partitioning the given primary index spans across all nodes in the cluster.
// Each processor will be assigned one or more spans to run their checks on.
func (c *inspectResumer) planInspectProcessors(
	ctx context.Context, jobExecCtx sql.JobExecContext, entirePKSpans []roachpb.Span,
) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
	if len(entirePKSpans) > 1 {
		return nil, nil, errors.AssertionFailedf("we only support one check: %d", len(entirePKSpans))
	}
	distSQLPlanner := jobExecCtx.DistSQLPlanner()
	planCtx, _, err := distSQLPlanner.SetupAllNodesPlanning(ctx, jobExecCtx.ExtendedEvalContext(), jobExecCtx.ExecCfg())
	if err != nil {
		return nil, nil, err
	}

	spanPartitions, err := distSQLPlanner.PartitionSpans(ctx, planCtx, entirePKSpans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return nil, nil, err
	}

	jobID := c.job.ID()
	newProcessorSpec := func(spans []roachpb.Span) *execinfrapb.InspectSpec {
		return &execinfrapb.InspectSpec{
			JobID: jobID,
			Spans: spans,
		}
	}

	// Set up a one-stage plan with one proc per input spec.
	processorCorePlacements := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
	for i, spanPartition := range spanPartitions {
		processorCorePlacements[i].SQLInstanceID = spanPartition.SQLInstanceID
		processorCorePlacements[i].Core.Inspect = newProcessorSpec(spanPartition.Spans)
	}

	physicalPlan := planCtx.NewPhysicalPlan()
	physicalPlan.AddNoInputStage(
		processorCorePlacements,
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
		execinfrapb.Ordering{},
		nil, /* finalizeLastStageCb */
	)
	physicalPlan.PlanToStreamColMap = []int{}

	sql.FinalizePlan(ctx, planCtx, physicalPlan)
	return physicalPlan, planCtx, nil
}

// runInspectPlan executes the distributed physical plan for the INSPECT job.
// It sets up a metadata-only DistSQL receiver to collect any execution errors,
// then runs the plan using the provided planning context and evaluation context.
// This function returns any error surfaced via metadata from the processors.
func (c *inspectResumer) runInspectPlan(
	ctx context.Context,
	jobExecCtx sql.JobExecContext,
	planCtx *sql.PlanningCtx,
	plan *sql.PhysicalPlan,
) error {
	execCfg := jobExecCtx.ExecCfg()

	metadataCallbackWriter := sql.NewMetadataOnlyMetadataCallbackWriter()

	distSQLReceiver := sql.MakeDistSQLReceiver(
		ctx,
		metadataCallbackWriter,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		nil, /* txn */
		nil, /* clockUpdater */
		jobExecCtx.ExtendedEvalContext().Tracing,
	)
	defer distSQLReceiver.Release()

	distSQLPlanner := jobExecCtx.DistSQLPlanner()

	// Copy the eval.Context, as dsp.Run() might change it.
	evalCtxCopy := jobExecCtx.ExtendedEvalContext().Context.Copy()

	distSQLPlanner.Run(ctx, planCtx, nil /* txn */, plan,
		distSQLReceiver, evalCtxCopy, nil /* finishedSetupFn */)
	return metadataCallbackWriter.Err()
}

func (c *inspectResumer) markJobComplete(ctx context.Context) error {
	// TODO(148297): add fine-grained progress reporting
	return c.job.NoTxn().Update(ctx,
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1,
			}
			ju.UpdateProgress(progress)
			return nil
		},
	)
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &inspectResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeInspect, createResumerFn, jobs.UsesTenantCostControl)
}
