// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var replanThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.ttl.replan_flow_threshold",
	"the fraction of flow instances that must change (added or updated) before the TTL job is replanned; set to 0 to disable",
	0.1,
	settings.FloatInRange(0, 1),
)

var replanFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.ttl.replan_flow_frequency",
	"how frequently the TTL job checks whether to replan its physical execution flow",
	time.Minute*2,
	settings.PositiveDuration,
)

var replanStabilityWindow = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.ttl.replan_stability_window",
	"number of consecutive replan evaluations required before triggering a replan; set to 1 to disable stability window",
	2,
	settings.PositiveInt,
)

// rowLevelTTLResumer implements the TTL job. The job can run on any node, but
// the job node distributes SELECT/DELETE work via DistSQL to ttlProcessor
// nodes. DistSQL divides work into spans that each ttlProcessor scans in a
// SELECT/DELETE loop.
type rowLevelTTLResumer struct {
	job *jobs.Job
	st  *cluster.Settings

	// consecutiveReplanDecisions tracks how many consecutive times replan was deemed necessary.
	consecutiveReplanDecisions *atomic.Int64
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) (retErr error) {
	defer func() {
		if retErr == nil {
			return
		} else if joberror.IsPermanentBulkJobError(retErr) && !errors.Is(retErr, sql.ErrPlanChanged) {
			retErr = jobs.MarkAsPermanentJobError(retErr)
		} else {
			retErr = jobs.MarkAsRetryJobError(retErr)
		}
	}()

	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()
	db := execCfg.InternalDB

	settingsValues := execCfg.SV()
	if err := ttlbase.CheckJobEnabled(settingsValues); err != nil {
		return err
	}

	telemetry.Inc(sqltelemetry.RowLevelTTLExecuted)

	var knobs sql.TTLTestingKnobs
	if ttlKnobs := execCfg.TTLTestingKnobs; ttlKnobs != nil {
		knobs = *ttlKnobs
	}

	details := t.job.Details().(jobspb.RowLevelTTLDetails)

	aostDuration := ttlbase.DefaultAOSTDuration
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	}

	var rowLevelTTL *catpb.RowLevelTTL
	var relationName string
	var entirePKSpan roachpb.Span
	var owner username.SQLUsername
	var descOverrides map[uint32]sessiondata.DescriptorOverride
	if err := db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, details.TableID)
		if err != nil {
			return err
		}
		// If the AOST timestamp is before the latest descriptor timestamp, exit
		// early as the delete will not work.
		modificationTime := desc.GetModificationTime().GoTime()
		aost := details.Cutoff.Add(aostDuration)
		if modificationTime.After(aost) {
			return pgerror.Newf(
				pgcode.ObjectNotInPrerequisiteState,
				"found a recent schema change on the table at %s, job will run at the next scheduled time",
				modificationTime.Format(time.RFC3339),
			)
		}

		if !desc.HasRowLevelTTL() {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		rowLevelTTL = desc.GetRowLevelTTL()

		if rowLevelTTL.Pause {
			return pgerror.Newf(pgcode.OperatorIntervention, "ttl jobs on table %s are currently paused", tree.Name(desc.GetName()))
		}

		tn, err := descs.GetObjectName(ctx, txn.KV(), txn.Descriptors(), desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}
		relationName = tn.FQString()

		entirePKSpan = desc.PrimaryIndexSpan(execCfg.Codec)

		owner, descOverrides, err = ttlSessionOverrides(desc)
		return err
	}); err != nil {
		return err
	}

	ttlExpr := rowLevelTTL.GetTTLExpr()

	labelMetrics := rowLevelTTL.LabelMetrics
	statsCtx, statsCancel := context.WithCancelCause(ctx)
	defer statsCancel(nil)
	statsGroup := ctxgroup.WithContext(statsCtx)
	// statsErr is the stats goroutine's own result. It is written as the
	// goroutine exits and read only after statsGroup.Wait returns; see the
	// comment there for why it is consulted instead of Wait's result.
	var statsErr error
	if rowLevelTTL.RowStatsPollInterval != 0 {
		metrics := execCfg.JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
			labelMetrics,
			relationName,
		)

		statsGroup.GoCtx(func(ctx context.Context) (retErr error) {
			defer func() { statsErr = retErr }()
			fetch := func() error {
				err := metrics.fetchStatistics(
					ctx, execCfg, owner, descOverrides, relationName, details, aostDuration, ttlExpr,
				)
				if err != nil && ctx.Err() != nil {
					// The stats context is cancelled when the TTL work completes, and
					// a stats query interrupted mid-flight surfaces that as a
					// cancellation error. Classify the error here, while the context
					// still distinguishes our own shutdown from an external
					// cancellation (such as CANCEL QUERY on the stats query, which
					// must be reported as a stats failure below).
					return nil //nolint:returnerrcheck
				}
				return err
			}
			// Do once initially to ensure we have some base statistics.
			if err := fetch(); err != nil {
				return err
			}
			// Wait until poll interval is reached, or early exit when we are done
			// with the TTL job.
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(rowLevelTTL.RowStatsPollInterval):
					if err := fetch(); err != nil {
						return err
					}
				}
			}
		})
	}

	distSQLPlanner := jobExecCtx.DistSQLPlanner()

	jobSpanCount := 0
	makePlan := func(ctx context.Context, distSQLPlanner *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
		// We don't return the compatible nodes here since PartitionSpans will
		// filter out incompatible nodes.
		planCtx, _, err := distSQLPlanner.SetupAllNodesPlanning(ctx, jobExecCtx.ExtendedEvalContext(), execCfg)
		if err != nil {
			return nil, nil, err
		}
		spanPartitions, err := distSQLPlanner.PartitionSpans(ctx, planCtx, []roachpb.Span{entirePKSpan}, sql.PartitionSpansBoundDefault)
		if err != nil {
			return nil, nil, err
		}
		expectedNumSpanPartitions := knobs.ExpectedNumSpanPartitions
		if expectedNumSpanPartitions != 0 {
			actualNumSpanPartitions := len(spanPartitions)
			if expectedNumSpanPartitions != actualNumSpanPartitions {
				return nil, nil, errors.AssertionFailedf(
					"incorrect number of span partitions expected=%d actual=%d",
					expectedNumSpanPartitions, actualNumSpanPartitions,
				)
			}
		}

		jobID := t.job.ID()
		selectBatchSize := ttlbase.GetSelectBatchSize(settingsValues, rowLevelTTL)
		deleteBatchSize := ttlbase.GetDeleteBatchSize(settingsValues, rowLevelTTL)
		selectRateLimit := ttlbase.GetSelectRateLimit(settingsValues, rowLevelTTL)
		deleteRateLimit := ttlbase.GetDeleteRateLimit(settingsValues, rowLevelTTL)
		disableChangefeedReplication := ttlbase.GetChangefeedReplicationDisabled(settingsValues, rowLevelTTL)
		newTTLSpec := func(spans []roachpb.Span) *execinfrapb.TTLSpec {
			return &execinfrapb.TTLSpec{
				JobID:                        jobID,
				RowLevelTTLDetails:           details,
				TTLExpr:                      ttlExpr,
				Spans:                        spans,
				SelectBatchSize:              selectBatchSize,
				DeleteBatchSize:              deleteBatchSize,
				SelectRateLimit:              selectRateLimit,
				DeleteRateLimit:              deleteRateLimit,
				LabelMetrics:                 rowLevelTTL.LabelMetrics,
				PreDeleteChangeTableVersion:  knobs.PreDeleteChangeTableVersion,
				PreSelectStatement:           knobs.PreSelectStatement,
				AOSTDuration:                 aostDuration,
				DisableChangefeedReplication: disableChangefeedReplication,
			}
		}

		jobSpanCount = 0
		for _, spanPartition := range spanPartitions {
			jobSpanCount += len(spanPartition.Spans)
		}

		sqlInstanceIDToTTLSpec := make(map[base.SQLInstanceID]*execinfrapb.TTLSpec, len(spanPartitions))
		for _, spanPartition := range spanPartitions {
			sqlInstanceIDToTTLSpec[spanPartition.SQLInstanceID] = newTTLSpec(spanPartition.Spans)
		}

		// Setup a one-stage plan with one proc per input spec.
		processorCorePlacements := make([]physicalplan.ProcessorCorePlacement, len(sqlInstanceIDToTTLSpec))
		i := 0
		for sqlInstanceID, ttlSpec := range sqlInstanceIDToTTLSpec {
			processorCorePlacements[i].SQLInstanceID = sqlInstanceID
			processorCorePlacements[i].Core.Ttl = ttlSpec
			i++
		}

		physicalPlan := planCtx.NewPhysicalPlan()
		// Job progress is updated inside ttlProcessor, so we
		// have an empty result stream.
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

	metadataCallbackWriter := sql.NewMetadataOnlyMetadataCallbackWriter()

	physicalPlan, planCtx, err := makePlan(ctx, distSQLPlanner)
	if err != nil {
		return err
	}

	// Call the BeforeProcessorStart hook for test synchronization.
	if knobs.BeforeProcessorStart != nil {
		if err := knobs.BeforeProcessorStart(ctx); err != nil {
			return err
		}
	}

	if err := t.job.NoTxn().Update(ctx,
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			rowLevelTTL.JobTotalSpanCount = int64(jobSpanCount)
			rowLevelTTL.JobProcessedSpanCount = 0
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 0,
			}
			ju.UpdateProgress(progress)
			return nil
		},
	); err != nil {
		return err
	}

	// Get a function to be used in a goroutine to monitor whether a replan is
	// needed due to changes in node membership. This is important because if
	// there are idle nodes that become available, it's more efficient to restart
	// the TTL job to utilize those nodes for parallel work.
	replanChecker, cancelReplanner := sql.PhysicalPlanChangeChecker(
		ctx, physicalPlan, makePlan, jobExecCtx,
		replanDecider(t.consecutiveReplanDecisions,
			func() int64 { return replanStabilityWindow.Get(&execCfg.Settings.SV) },
			func() float64 { return replanThreshold.Get(&execCfg.Settings.SV) },
		),
		func() time.Duration { return replanFrequency.Get(&execCfg.Settings.SV) },
	)

	// Create a separate context group to run the replanner and the TTL distSQL driver.
	// Note: this is distinct from the stats collection group, as errors from stats
	// collection are non-fatal and treated as warnings.
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer cancelReplanner()
		if rowLevelTTL.RowStatsPollInterval != 0 {
			defer statsCancel(errors.New("cancelling TTL stats query because TTL job completed"))
		}
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

		// Copy the eval.Context, as dsp.Run() might change it.
		evalCtxCopy := jobExecCtx.ExtendedEvalContext().Context.Copy()
		distSQLPlanner.Run(
			ctx,
			planCtx,
			nil, /* txn */
			physicalPlan,
			distSQLReceiver,
			evalCtxCopy,
			nil, /* finishedSetupFn */
		)

		return metadataCallbackWriter.Err()
	})

	g.GoCtx(replanChecker)

	if err := g.Wait(); err != nil {
		return err
	}

	// ctxgroup.Wait reports the group context's error even when every
	// goroutine returned nil, so once the stats context is canceled at job
	// completion, Wait returns context.Canceled regardless of how the stats
	// goroutine exited. Its result is therefore ignored in favor of the
	// goroutine's own.
	_ = statsGroup.Wait()
	if err := statsErr; err != nil {
		// Errors caused by our own shutdown cancellation of the stats context
		// are filtered out in the stats goroutine, so anything that reaches
		// here is a real stats failure (including an external CANCEL QUERY of
		// a stats query).
		if knobs.ReturnStatsError {
			return err
		}
		log.Warningf(ctx, "failed to get statistics for table id %d: %v", details.TableID, err)
	}
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

// replanDecider returns a function that determines whether a TTL job should be
// replanned based on changes in the physical execution plan. It compares the
// old and new plans to detect node availability changes and decides if the
// benefit of replanning (better parallelization) outweighs the cost of
// restarting the job. It implements a stability window to avoid replanning
// due to transient changes.
func replanDecider(
	consecutiveReplanDecisions *atomic.Int64,
	stabilityWindowFn func() int64,
	thresholdFn func() float64,
) sql.PlanChangeDecision {
	return func(ctx context.Context, oldPlan, newPlan *sql.PhysicalPlan) bool {
		changed, growth := detectNodeAvailabilityChanges(oldPlan, newPlan)
		threshold := thresholdFn()
		shouldReplan := threshold != 0.0 && growth > threshold

		stabilityWindow := stabilityWindowFn()

		var currentDecisions int64
		if shouldReplan {
			currentDecisions = consecutiveReplanDecisions.Add(1)
		} else {
			consecutiveReplanDecisions.Store(0)
			currentDecisions = 0
		}

		// If stability window is 1, replan immediately. Otherwise, require
		// consecutive decisions to meet the window threshold.
		replan := currentDecisions >= stabilityWindow

		// Reset the counter when we decide to replan, since the job will restart
		if replan {
			consecutiveReplanDecisions.Store(0)
		}

		if shouldReplan || growth > 0.1 || log.V(1) {
			log.Infof(ctx, "Re-planning would add or alter flows on %d nodes / %.2f, threshold %.2f, consecutive decisions %d/%d, replan %v",
				changed, growth, threshold, currentDecisions, stabilityWindow, replan)
		}

		return replan
	}
}

// detectNodeAvailabilityChanges analyzes differences between two physical plans
// to determine if nodes have become unavailable. It returns the number of nodes
// that are no longer available and the fraction of the original plan affected.
//
// The function focuses on detecting when nodes from the original plan are missing
// from the new plan, which typically indicates node failures. When nodes fail,
// their work gets redistributed to remaining nodes, making a job restart
// beneficial for better parallelization. We ignore newly added nodes since
// continuing the current job on existing nodes is usually more efficient than
// restarting to incorporate new capacity.
func detectNodeAvailabilityChanges(before, after *sql.PhysicalPlan) (int, float64) {
	var changed int
	beforeSpecs, beforeCleanup := before.GenerateFlowSpecs()
	defer beforeCleanup(beforeSpecs)
	afterSpecs, afterCleanup := after.GenerateFlowSpecs()
	defer afterCleanup(afterSpecs)

	// Count nodes from the original plan that are no longer present in the new plan.
	// We only check nodes in beforeSpecs because we specifically want to detect
	// when nodes that were doing work are no longer available, which typically
	// indicates beneficial restart scenarios (node failures where work can be
	// redistributed more efficiently).
	for n := range beforeSpecs {
		if _, ok := afterSpecs[n]; !ok {
			changed++
		}
	}

	var frac float64
	if changed > 0 {
		frac = float64(changed) / float64(len(beforeSpecs))
	}
	return changed, frac
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeRowLevelTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &rowLevelTTLResumer{
			job:                        job,
			st:                         settings,
			consecutiveReplanDecisions: &atomic.Int64{},
		}
	}, jobs.UsesTenantCostControl)
}
