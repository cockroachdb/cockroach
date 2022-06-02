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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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
	defaultRangeConcurrency = settings.RegisterIntSetting(
		settings.TenantWritable,
		"sql.ttl.default_range_concurrency",
		"default amount of ranges to process at once during a TTL delete",
		1,
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

type rangeToProcess struct {
	startPK, endPK tree.Datums
}

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

	aostDuration := -time.Second * 30
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	}
	aost := details.Cutoff.Add(aostDuration)

	var tableVersion descpb.DescriptorVersion
	var rowLevelTTL catpb.RowLevelTTL
	var relationName string
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		desc, err := descsCol.GetImmutableTableByID(
			ctx,
			txn,
			details.TableID,
			tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		tableVersion = desc.GetVersion()
		// If the AOST timestamp is before the latest descriptor timestamp, exit
		// early as the delete will not work.
		if desc.GetModificationTime().GoTime().After(aost) {
			return errors.Newf(
				"found a recent schema change on the table at %s, aborting",
				desc.GetModificationTime().GoTime().Format(time.RFC3339),
			)
		}

		if !desc.HasRowLevelTTL() {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		rowLevelTTL = *desc.GetRowLevelTTL()

		if rowLevelTTL.Pause {
			return errors.Newf("ttl jobs on table %s are currently paused", tree.Name(desc.GetName()))
		}

		tn, err := descs.GetTableNameByDesc(ctx, txn, descsCol, desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}

		relationName = tn.FQString()
		return nil
	}); err != nil {
		return err
	}

	group := ctxgroup.WithContext(ctx)

	rangeConcurrency := getRangeConcurrency(settingsValues, rowLevelTTL)
	selectBatchSize := getSelectBatchSize(settingsValues, rowLevelTTL)
	deleteBatchSize := getDeleteBatchSize(settingsValues, rowLevelTTL)
	deleteRateLimit := getDeleteRateLimit(settingsValues, rowLevelTTL)

	ttlExpr := colinfo.DefaultTTLExpirationExpr
	if rowLevelTTL.HasExpirationExpr() {
		ttlExpr = "(" + rowLevelTTL.ExpirationExpr + ")"
	}

	labelMetrics := rowLevelTTL.LabelMetrics

	rowCount, err := func() (int64, error) {
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

		return t.work(
			ctx,
			db,
			rangeConcurrency,
			execCfg,
			details,
			descsCol,
			knobs,
			tableVersion,
			selectBatchSize,
			deleteBatchSize,
			deleteRateLimit,
			aost,
			ttlExpr,
		)
	}()
	if err != nil {
		return err
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return db.Txn(ctx, func(_ context.Context, txn *kv.Txn) error {
		return t.job.Update(ctx, txn, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL.RowCount += rowCount
			ju.UpdateProgress(progress)
			return nil
		})
	})
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

func getRangeConcurrency(sv *settings.Values, ttl catpb.RowLevelTTL) int64 {
	rc := ttl.RangeConcurrency
	if rc == 0 {
		rc = defaultRangeConcurrency.Get(sv)
	}
	return rc
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
