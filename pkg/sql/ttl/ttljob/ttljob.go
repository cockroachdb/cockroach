// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	defaultSelectBatchSize = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_select_batch_size",
		"default amount of rows to select in a single query during a TTL job",
		500,
		settings.PositiveInt,
	).WithPublic()
	defaultDeleteBatchSize = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_delete_batch_size",
		"default amount of rows to delete in a single query during a TTL job",
		100,
		settings.PositiveInt,
	).WithPublic()
	defaultDeleteRateLimit = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_delete_rate_limit",
		"default delete rate limit for all TTL jobs. Use 0 to signify no rate limit.",
		0,
		settings.NonNegativeInt,
	).WithPublic()

	jobEnabled = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"sql.ttl.job.enabled",
		"whether the TTL job is enabled",
		true,
	).WithPublic()
)

type rowLevelTTLResumer struct {
	job *jobs.Job
	st  *cluster.Settings
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	jobExecCtx := execCtx.(sql.JobExecContext)
	execCfg := jobExecCtx.ExecCfg()
	db := execCfg.DB
	descsCol := jobExecCtx.ExtendedEvalContext().Descs

	settingsValues := execCfg.SV()
	if err := checkEnabled(settingsValues); err != nil {
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
	aost := details.Cutoff.Add(aostDuration)

	ttlSpecAOST := aost
	// Set ttlSpec.AOST to 0 to avoid overriding 0 duration in tests.
	if knobs.AOSTDuration != nil {
		ttlSpecAOST = time.Time{}
	}

	var rowLevelTTL catpb.RowLevelTTL
	var relationName string
	var entirePKSpan roachpb.Span
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := descsCol.ByIDWithLeased(txn).WithoutNonPublic().Get().Table(ctx, details.TableID)
		if err != nil {
			return err
		}
		// If the AOST timestamp is before the latest descriptor timestamp, exit
		// early as the delete will not work.
		modificationTime := desc.GetModificationTime().GoTime()
		if modificationTime.After(aost) {
			return errors.Newf(
				"found a recent schema change on the table at %s, aborting",
				modificationTime.Format(time.RFC3339),
			)
		}

		if !desc.HasRowLevelTTL() {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		rowLevelTTL = *desc.GetRowLevelTTL()

		if rowLevelTTL.Pause {
			return errors.Newf("ttl jobs on table %s are currently paused", tree.Name(desc.GetName()))
		}

		tn, err := descs.GetObjectName(ctx, txn, descsCol, desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}
		relationName = tn.FQString()

		entirePKSpan = desc.PrimaryIndexSpan(execCfg.Codec)
		return nil
	}); err != nil {
		return err
	}

	ttlExpr := rowLevelTTL.GetTTLExpr()

	labelMetrics := rowLevelTTL.LabelMetrics
	group := ctxgroup.WithContext(ctx)
	err := func() error {
		statsCloseCh := make(chan struct{})
		defer close(statsCloseCh)
		if rowLevelTTL.RowStatsPollInterval != 0 {

			metrics := execCfg.JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
				labelMetrics,
				relationName,
			)

			group.GoCtx(func(ctx context.Context) error {

				handleError := func(err error) error {
					if knobs.ReturnStatsError {
						return err
					}
					log.Warningf(ctx, "failed to get statistics for table id %d: %s", details.TableID, err)
					return nil
				}

				// Do once initially to ensure we have some base statistics.
				err := metrics.fetchStatistics(ctx, execCfg, relationName, details, aostDuration, ttlExpr)
				if err := handleError(err); err != nil {
					return err
				}
				// Wait until poll interval is reached, or early exit when we are done
				// with the TTL job.
				for {
					select {
					case <-statsCloseCh:
						return nil
					case <-time.After(rowLevelTTL.RowStatsPollInterval):
						err := metrics.fetchStatistics(ctx, execCfg, relationName, details, aostDuration, ttlExpr)
						if err := handleError(err); err != nil {
							return err
						}
					}
				}
			})
		}

		distSQLPlanner := jobExecCtx.DistSQLPlanner()
		evalCtx := jobExecCtx.ExtendedEvalContext()

		// We don't return the compatible nodes here since PartitionSpans will
		// filter out incompatible nodes.
		planCtx, _, err := distSQLPlanner.SetupAllNodesPlanning(ctx, evalCtx, execCfg)
		if err != nil {
			return err
		}
		spanPartitions, err := distSQLPlanner.PartitionSpans(ctx, planCtx, []roachpb.Span{entirePKSpan})
		if err != nil {
			return err
		}
		expectedNumSpanPartitions := knobs.ExpectedNumSpanPartitions
		if expectedNumSpanPartitions != 0 {
			actualNumSpanPartitions := len(spanPartitions)
			if expectedNumSpanPartitions != actualNumSpanPartitions {
				return errors.AssertionFailedf(
					"incorrect number of span partitions expected=%d actual=%d",
					expectedNumSpanPartitions, actualNumSpanPartitions,
				)
			}
		}

		jobID := t.job.ID()
		selectBatchSize := getSelectBatchSize(settingsValues, rowLevelTTL)
		deleteBatchSize := getDeleteBatchSize(settingsValues, rowLevelTTL)
		deleteRateLimit := getDeleteRateLimit(settingsValues, rowLevelTTL)
		newTTLSpec := func(spans []roachpb.Span) *execinfrapb.TTLSpec {
			return &execinfrapb.TTLSpec{
				JobID:              jobID,
				RowLevelTTLDetails: details,
				// Set AOST in case of mixed 22.2.0/22.2.1+ cluster where the job started on a 22.2.1+ node.
				AOST:                        ttlSpecAOST,
				TTLExpr:                     ttlExpr,
				Spans:                       spans,
				SelectBatchSize:             selectBatchSize,
				DeleteBatchSize:             deleteBatchSize,
				DeleteRateLimit:             deleteRateLimit,
				LabelMetrics:                rowLevelTTL.LabelMetrics,
				PreDeleteChangeTableVersion: knobs.PreDeleteChangeTableVersion,
				PreSelectStatement:          knobs.PreSelectStatement,
				AOSTDuration:                aostDuration,
			}
		}

		jobSpanCount := 0
		for _, spanPartition := range spanPartitions {
			jobSpanCount += len(spanPartition.Spans)
		}

		jobRegistry := execCfg.JobRegistry
		if err := jobRegistry.UpdateJobWithTxn(
			ctx,
			jobID,
			nil,  /* txn */
			true, /* useReadLock */
			func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				progress := md.Progress
				rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
				rowLevelTTL.JobSpanCount = int64(jobSpanCount)
				ju.UpdateProgress(progress)
				return nil
			},
		); err != nil {
			return err
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
		)
		physicalPlan.PlanToStreamColMap = []int{}

		sql.FinalizePlan(ctx, planCtx, physicalPlan)

		metadataCallbackWriter := sql.NewMetadataOnlyMetadataCallbackWriter()

		distSQLReceiver := sql.MakeDistSQLReceiver(
			ctx,
			metadataCallbackWriter,
			tree.Rows,
			execCfg.RangeDescriptorCache,
			nil, /* txn */
			nil, /* clockUpdater */
			evalCtx.Tracing,
		)
		defer distSQLReceiver.Release()

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		distSQLPlanner.Run(
			ctx,
			planCtx,
			nil, /* txn */
			physicalPlan,
			distSQLReceiver,
			&evalCtxCopy,
			nil, /* finishedSetupFn */
		)

		return metadataCallbackWriter.Err()
	}()
	if err != nil {
		return err
	}

	return group.Wait()
}

func checkEnabled(settingsValues *settings.Values) error {
	if enabled := jobEnabled.Get(settingsValues); !enabled {
		return errors.Newf(
			"ttl jobs are currently disabled by CLUSTER SETTING %s",
			jobEnabled.Key(),
		)
	}
	return nil
}

func getSelectBatchSize(sv *settings.Values, ttl catpb.RowLevelTTL) int64 {
	bs := ttl.SelectBatchSize
	if bs == 0 {
		bs = defaultSelectBatchSize.Get(sv)
	}
	return bs
}

func getDeleteBatchSize(sv *settings.Values, ttl catpb.RowLevelTTL) int64 {
	bs := ttl.DeleteBatchSize
	if bs == 0 {
		bs = defaultDeleteBatchSize.Get(sv)
	}
	return bs
}

func getDeleteRateLimit(sv *settings.Values, ttl catpb.RowLevelTTL) int64 {
	rl := ttl.DeleteRateLimit
	if rl == 0 {
		rl = defaultDeleteRateLimit.Get(sv)
	}
	// Put the maximum tokens possible if there is no rate limit.
	if rl == 0 {
		rl = math.MaxInt64
	}
	return rl
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeRowLevelTTL, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &rowLevelTTLResumer{
			job: job,
			st:  settings,
		}
	}, jobs.UsesTenantCostControl)
}
