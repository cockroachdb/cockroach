// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
	log.Dev.Infof(ctx, "starting INSPECT job")

	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()

	execCfg.JobRegistry.MetricsStruct().Inspect.(*InspectMetrics).Runs.Inc(1)

	if err := c.maybeRunOnJobStartHook(execCfg); err != nil {
		return err
	}

	details := c.job.Details().(jobspb.InspectDetails)
	if len(details.Checks) == 0 {
		return nil
	}

	pkSpans, err := c.getPrimaryIndexSpans(ctx, execCfg)
	if err != nil {
		return err
	}

	if err := c.maybeProtectTimestamp(ctx, execCfg, details); err != nil {
		return err
	}

	defer c.maybeCleanupProtectedTimestamp(ctx, execCfg)

	if err := c.maybeRunAfterProtectedTimestampHook(execCfg); err != nil {
		return err
	}

	progressTracker, completedSpans, cleanupProgress, err := c.setupProgressTracking(ctx, execCfg)
	if err != nil {
		return err
	}
	defer cleanupProgress()

	remainingSpans := c.filterCompletedSpans(pkSpans, completedSpans)

	// If all spans are completed, job is done.
	if len(remainingSpans) == 0 {
		log.Dev.Infof(ctx, "all spans already completed, INSPECT job finished")
		return nil
	}

	plan, planCtx, err := c.planInspectProcessors(ctx, jobExecCtx, remainingSpans)
	if err != nil {
		return err
	}

	// After planning, we have the finalized set of spans to process (adjacent
	// spans on the same node are merged). Compute the checks to run and initialize
	// progress tracking from the plan.
	if err := c.initProgressFromPlan(ctx, execCfg, progressTracker, plan, completedSpans); err != nil {
		return err
	}

	if err := c.runInspectPlan(ctx, jobExecCtx, planCtx, plan, progressTracker); err != nil {
		return err
	}

	return nil
}

// OnFailOrCancel implements the Resumer interface
func (c *inspectResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()
	c.maybeCleanupProtectedTimestamp(ctx, execCfg)

	// Record RunsWithIssues metric if the job failed due to finding issues (including internal errors).
	if errors.Is(jobErr, errInspectFoundInconsistencies) || errors.Is(jobErr, errInspectInternalErrors) {
		execCfg.JobRegistry.MetricsStruct().Inspect.(*InspectMetrics).RunsWithIssues.Inc(1)
	}
	return nil
}

// CollectProfile implements the Resumer interface
func (c *inspectResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

func (c *inspectResumer) maybeRunOnJobStartHook(execCfg *sql.ExecutorConfig) error {
	if execCfg.InspectTestingKnobs == nil || execCfg.InspectTestingKnobs.OnInspectJobStart == nil {
		return nil
	}
	return execCfg.InspectTestingKnobs.OnInspectJobStart()
}

func (c *inspectResumer) maybeRunAfterProtectedTimestampHook(execCfg *sql.ExecutorConfig) error {
	if execCfg.InspectTestingKnobs == nil || execCfg.InspectTestingKnobs.OnInspectAfterProtectedTimestamp == nil {
		return nil
	}
	return execCfg.InspectTestingKnobs.OnInspectAfterProtectedTimestamp()
}

// getPrimaryIndexSpans returns the primary index spans for all tables involved in
// the INSPECT job's checks.
func (c *inspectResumer) getPrimaryIndexSpans(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) ([]roachpb.Span, error) {
	details := c.job.Details().(jobspb.InspectDetails)

	// Deduplicate by table ID to avoid processing the same span multiple times
	// when there are multiple checks on the same table.
	uniqueTableIDs := make(map[descpb.ID]struct{})
	for i := range details.Checks {
		uniqueTableIDs[details.Checks[i].TableID] = struct{}{}
	}

	spans := make([]roachpb.Span, 0, len(uniqueTableIDs))
	err := execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for tableID := range uniqueTableIDs {
			desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
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
			JobID:          jobID,
			InspectDetails: c.job.Details().(jobspb.InspectDetails),
			Spans:          spans,
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
// It sets up a metadata-only DistSQL receiver to collect any execution errors
// and progress updates, then runs the plan using the provided planning context
// and evaluation context. This function returns any error surfaced via metadata
// from the processors.
func (c *inspectResumer) runInspectPlan(
	ctx context.Context,
	jobExecCtx sql.JobExecContext,
	planCtx *sql.PlanningCtx,
	plan *sql.PhysicalPlan,
	progressTracker *inspectProgressTracker,
) error {
	execCfg := jobExecCtx.ExecCfg()

	metadataCallbackWriter := sql.NewMetadataOnlyMetadataCallbackWriter(
		func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
			if meta.BulkProcessorProgress != nil {
				return progressTracker.handleProgressUpdate(ctx, meta)
			}
			return nil
		})

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

// setupProgressTracking initializes progress tracking and returns
// it, along with a cleanup function.
func (c *inspectResumer) setupProgressTracking(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) (*inspectProgressTracker, []roachpb.Span, func(), error) {
	// Create and initialize the tracker. We use the completed spans from the job
	// (if any) to filter out the spans we need to process in this run of the job.
	progressTracker := newInspectProgressTracker(
		c.job,
		&execCfg.Settings.SV,
		execCfg.InternalDB,
	)
	completedSpans, err := progressTracker.initTracker(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	cleanup := func() {
		progressTracker.terminateTracker()
	}

	return progressTracker, completedSpans, cleanup, nil
}

// initProgressFromPlan initializes job progress based on the actual partitioned spans
// that will be processed.
func (c *inspectResumer) initProgressFromPlan(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	progressTracker *inspectProgressTracker,
	plan *sql.PhysicalPlan,
	completedPartitionedSpans []roachpb.Span,
) error {
	// Extract all spans from the plan processors.
	var remainingPartitionedSpans []roachpb.Span
	for i := range plan.Processors {
		if plan.Processors[i].Spec.Core.Inspect != nil {
			remainingPartitionedSpans = append(remainingPartitionedSpans, plan.Processors[i].Spec.Core.Inspect.Spans...)
		}
	}

	applicabilityCheckers, err := buildApplicabilityCheckers(c.job.Details().(jobspb.InspectDetails))
	if err != nil {
		return err
	}

	// Calculate total applicable checks on ALL spans (not just remaining ones)
	// This ensures consistent progress calculation across job restarts.
	completedCheckCount, err := countApplicableChecks(completedPartitionedSpans, applicabilityCheckers, execCfg.Codec)
	if err != nil {
		return err
	}
	remainingCheckCount, err := countApplicableChecks(remainingPartitionedSpans, applicabilityCheckers, execCfg.Codec)
	if err != nil {
		return err
	}

	totalCheckCount := completedCheckCount + remainingCheckCount

	log.Dev.Infof(ctx, "INSPECT progress init: %d partitioned spans, %d total checks (%d remaining + %d completed)",
		len(remainingPartitionedSpans), totalCheckCount, remainingCheckCount, completedCheckCount)

	return progressTracker.initJobProgress(ctx, totalCheckCount, completedCheckCount)
}

// filterCompletedSpans removes spans that are already completed from the list to process.
func (c *inspectResumer) filterCompletedSpans(
	allSpans []roachpb.Span, completedSpans []roachpb.Span,
) []roachpb.Span {
	if len(completedSpans) == 0 {
		return allSpans
	}

	completedGroup := roachpb.SpanGroup{}
	completedGroup.Add(completedSpans...)

	var remainingSpans []roachpb.Span
	for _, span := range allSpans {
		// Check if this span is fully contained in completed spans.
		// We need to check if the entire span is covered by completed spans.
		if !completedGroup.Encloses(span) {
			remainingSpans = append(remainingSpans, span)
		}
	}

	return remainingSpans
}

// buildApplicabilityCheckers creates lightweight applicability checkers from InspectDetails.
// These are used only for progress calculation and don't require the full check machinery.
func buildApplicabilityCheckers(
	details jobspb.InspectDetails,
) ([]inspectCheckApplicability, error) {
	checkers := make([]inspectCheckApplicability, 0, len(details.Checks))
	for _, specCheck := range details.Checks {
		switch specCheck.Type {
		case jobspb.InspectCheckIndexConsistency:
			checkers = append(checkers, &indexConsistencyCheckApplicability{
				tableID: specCheck.TableID,
			})
		default:
			return nil, errors.AssertionFailedf("unsupported inspect check type: %v", specCheck.Type)
		}
	}
	return checkers, nil
}

// maybeProtectTimestamp creates a protected timestamp record for the AsOf
// timestamp to prevent garbage collection during the inspect operation.
// If no AsOf timestamp is specified, this function does nothing.
// The protection target includes all tables involved in the inspect checks.
// Uses the jobsprotectedts.Manager to store the PTS ID in job details.
func (c *inspectResumer) maybeProtectTimestamp(
	ctx context.Context, execCfg *sql.ExecutorConfig, details jobspb.InspectDetails,
) error {
	// If we are not running a historical query, nothing to do here.
	if details.AsOf.IsEmpty() {
		return nil
	}

	// Create a target for the specific tables involved in the inspect checks
	var tableIDSet catalog.DescriptorIDSet
	for _, check := range details.Checks {
		tableIDSet.Add(check.TableID)
	}
	target := ptpb.MakeSchemaObjectsTarget(tableIDSet.Ordered())

	// Protect will store the PTS ID in job details.
	_, err := execCfg.ProtectedTimestampManager.Protect(ctx, c.job, target, details.AsOf)
	if err != nil {
		return errors.Wrapf(err, "failed to protect timestamp %s for INSPECT job %d", details.AsOf, c.job.ID())
	}

	log.Dev.Infof(ctx, "protected timestamp created for INSPECT job %d at %s", c.job.ID(), details.AsOf)
	return nil
}

// maybeCleanupProtectedTimestamp cleans up any protected timestamp record
// associated with this job. If no protected timestamp was created, this
// function does nothing.
func (c *inspectResumer) maybeCleanupProtectedTimestamp(
	ctx context.Context, execCfg *sql.ExecutorConfig,
) {
	details := c.job.Details().(jobspb.InspectDetails)
	if details.ProtectedTimestampRecord != nil {
		if err := execCfg.ProtectedTimestampManager.Unprotect(ctx, c.job); err != nil {
			log.Dev.Warningf(ctx, "failed to clean up protected timestamp: %v", err)
		}
	}
}

// ReportResults implements the JobResultsReporter interface.
func (r *inspectResumer) ReportResults(ctx context.Context, resultsCh chan<- tree.Datums) error {
	select {
	case resultsCh <- tree.Datums{
		tree.NewDInt(tree.DInt(r.job.ID())),
		tree.NewDString(string(r.job.State())),
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &inspectResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeInspect, createResumerFn, jobs.UsesTenantCostControl)
}
