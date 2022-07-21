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
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	descsCol := p.ExtendedEvalContext().Descs

	if err := checkEnabled(p.ExecCfg().SV()); err != nil {
		return err
	}

	telemetry.Inc(sqltelemetry.RowLevelTTLExecuted)

	var knobs sql.TTLTestingKnobs
	if ttlKnobs := p.ExecCfg().TTLTestingKnobs; ttlKnobs != nil {
		knobs = *ttlKnobs
	}

	details := t.job.Details().(jobspb.RowLevelTTLDetails)

	aostDuration := -time.Second * 30
	if knobs.AOSTDuration != nil {
		aostDuration = *knobs.AOSTDuration
	}
	aost, err := tree.MakeDTimestampTZ(timeutil.Now().Add(aostDuration), time.Microsecond)
	if err != nil {
		return err
	}

	var initialVersion descpb.DescriptorVersion

	var ttlSettings catpb.RowLevelTTL
	var pkColumns []string
	var pkTypes []*types.T
	var relationName string
	var rangeSpan, entirePKSpan roachpb.Span
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
		initialVersion = desc.GetVersion()
		// If the AOST timestamp is before the latest descriptor timestamp, exit
		// early as the delete will not work.
		if desc.GetModificationTime().GoTime().After(aost.Time) {
			return errors.Newf(
				"found a recent schema change on the table at %s, aborting",
				desc.GetModificationTime().GoTime().Format(time.RFC3339),
			)
		}
		pkColumns = desc.GetPrimaryIndex().IndexDesc().KeyColumnNames
		for _, id := range desc.GetPrimaryIndex().IndexDesc().KeyColumnIDs {
			col, err := desc.FindColumnWithID(id)
			if err != nil {
				return err
			}
			pkTypes = append(pkTypes, col.GetType())
		}

		ttl := desc.GetRowLevelTTL()
		if ttl == nil {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		if ttl.Pause {
			return errors.Newf("ttl jobs on table %s are currently paused", tree.Name(desc.GetName()))
		}

		tn, err := descs.GetTableNameByDesc(ctx, txn, descsCol, desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}

		relationName = tn.FQString()
		entirePKSpan = desc.IndexSpan(p.ExecCfg().Codec, desc.GetPrimaryIndex().GetID())
		rangeSpan = entirePKSpan
		ttlSettings = *ttl
		return nil
	}); err != nil {
		return err
	}

	metrics := p.ExecCfg().JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
		ttlSettings.LabelMetrics,
		relationName,
	)
	var alloc tree.DatumAlloc
	type rangeToProcess struct {
		startPK, endPK tree.Datums
	}

	g := ctxgroup.WithContext(ctx)

	rangeConcurrency := getRangeConcurrency(p.ExecCfg().SV(), ttlSettings)
	selectBatchSize := getSelectBatchSize(p.ExecCfg().SV(), ttlSettings)
	deleteBatchSize := getDeleteBatchSize(p.ExecCfg().SV(), ttlSettings)
	deleteRateLimit := getDeleteRateLimit(p.ExecCfg().SV(), ttlSettings)
	deleteRateLimiter := quotapool.NewRateLimiter(
		"ttl-delete",
		quotapool.Limit(deleteRateLimit),
		deleteRateLimit,
	)

	ttlExpression := colinfo.TTLDefaultExpirationColumnName
	if ttlSettings.HasExpirationExpr() {
		ttlExpression = "(" + string(ttlSettings.ExpirationExpr) + ")"
	}

	statsCloseCh := make(chan struct{})
	ch := make(chan rangeToProcess, rangeConcurrency)
	rowCount := int64(0)
	for i := int64(0); i < rangeConcurrency; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for r := range ch {
				start := timeutil.Now()
				rangeRowCount, err := runTTLOnRange(
					ctx,
					p.ExecCfg(),
					details,
					p.ExtendedEvalContext().Descs,
					knobs,
					metrics,
					initialVersion,
					r.startPK,
					r.endPK,
					pkColumns,
					relationName,
					selectBatchSize,
					deleteBatchSize,
					deleteRateLimiter,
					*aost,
					ttlExpression,
				)
				// add before returning err in case of partial success
				atomic.AddInt64(&rowCount, rangeRowCount)
				metrics.RangeTotalDuration.RecordValue(int64(timeutil.Since(start)))
				if err != nil {
					// Continue until channel is fully read.
					// Otherwise, the keys input will be blocked.
					for r = range ch {
					}
					return err
				}
			}
			return nil
		})
	}

	if ttlSettings.RowStatsPollInterval != 0 {

		g.GoCtx(func(ctx context.Context) error {

			handleError := func(err error) error {
				if knobs.ReturnStatsError {
					return err
				}
				log.Warningf(ctx, "failed to get statistics for table id %d: %s", details.TableID, err)
				return nil
			}

			// Do once initially to ensure we have some base statistics.
			err := fetchStatistics(ctx, p.ExecCfg(), relationName, details, metrics, aostDuration, ttlExpression)
			if err := handleError(err); err != nil {
				return err
			}
			// Wait until poll interval is reached, or early exit when we are done
			// with the TTL job.
			for {
				select {
				case <-statsCloseCh:
					return nil
				case <-time.After(ttlSettings.RowStatsPollInterval):
					err := fetchStatistics(ctx, p.ExecCfg(), relationName, details, metrics, aostDuration, ttlExpression)
					if err := handleError(err); err != nil {
						return err
					}
				}
			}
		})
	}

	// Iterate over every range to feed work for the goroutine processors.
	if err := func() (retErr error) {
		defer func() {
			close(ch)
			close(statsCloseCh)
			retErr = errors.CombineErrors(retErr, g.Wait())
			retErr = errors.CombineErrors(retErr, db.Txn(ctx, func(_ context.Context, txn *kv.Txn) error {
				return t.job.Update(ctx, txn, func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
					progress := md.Progress
					progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL.RowCount += rowCount
					ju.UpdateProgress(progress)
					return nil
				})
			}))
		}()

		ri := kvcoord.MakeRangeIterator(p.ExecCfg().DistSender)
		done := false
		ri.Seek(ctx, roachpb.RKey(entirePKSpan.Key), kvcoord.Ascending)
		for ; ri.Valid() && !done; ri.Next(ctx) {
			// Send range info to each goroutine worker.
			rangeDesc := ri.Desc()
			var nextRange rangeToProcess
			// A single range can contain multiple tables or indexes.
			// If this is the case, the rangeDesc.StartKey would be less than entirePKSpan.Key
			// or the rangeDesc.EndKey would be greater than the entirePKSpan.EndKey, meaning
			// the range contains the start or the end of the range respectively.
			// Trying to decode keys outside the PK range will lead to a decoding error.
			// As such, only populate nextRange.startPK and nextRange.endPK if this is the case
			// (by default, a 0 element startPK or endPK means the beginning or end).
			if rangeDesc.StartKey.AsRawKey().Compare(entirePKSpan.Key) > 0 {
				nextRange.startPK, err = keyToDatums(rangeDesc.StartKey, p.ExecCfg().Codec, pkTypes, &alloc)
				if err != nil {
					return errors.Wrapf(
						err,
						"error decoding starting PRIMARY KEY for range ID %d (start key %x, table start key %x)",
						rangeDesc.RangeID,
						rangeDesc.StartKey.AsRawKey(),
						entirePKSpan.Key,
					)
				}
			}
			if rangeDesc.EndKey.AsRawKey().Compare(entirePKSpan.EndKey) < 0 {
				rangeSpan.Key = rangeDesc.EndKey.AsRawKey()
				nextRange.endPK, err = keyToDatums(rangeDesc.EndKey, p.ExecCfg().Codec, pkTypes, &alloc)
				if err != nil {
					return errors.Wrapf(
						err,
						"error decoding ending PRIMARY KEY for range ID %d (end key %x, table end key %x)",
						rangeDesc.RangeID,
						rangeDesc.EndKey.AsRawKey(),
						entirePKSpan.EndKey,
					)
				}
			} else {
				done = true
			}
			ch <- nextRange
		}
		return nil
	}(); err != nil {
		return err
	}
	return nil
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

func fetchStatistics(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	relationName string,
	details jobspb.RowLevelTTLDetails,
	metrics rowLevelTTLMetrics,
	aostDuration time.Duration,
	ttlExpression string,
) error {
	aost, err := tree.MakeDTimestampTZ(timeutil.Now().Add(aostDuration), time.Microsecond)
	if err != nil {
		return err
	}

	for _, c := range []struct {
		opName string
		query  string
		args   []interface{}
		gauge  *aggmetric.Gauge
	}{
		{
			opName: fmt.Sprintf("ttl num rows stats %s", relationName),
			query:  `SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s`,
			gauge:  metrics.TotalRows,
		},
		{
			opName: fmt.Sprintf("ttl num expired rows stats %s", relationName),
			query:  `SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s WHERE ` + ttlExpression + ` < $1`,
			args:   []interface{}{details.Cutoff},
			gauge:  metrics.TotalExpiredRows,
		},
	} {
		// User a super low quality of service (lower than TTL low), as we don't
		// really care if statistics gets left behind and prefer the TTL job to
		// have priority.
		qosLevel := sessiondatapb.SystemLow
		datums, err := execCfg.InternalExecutor.QueryRowEx(
			ctx,
			c.opName,
			nil,
			sessiondata.InternalExecutorOverride{
				User:             username.RootUserName(),
				QualityOfService: &qosLevel,
			},
			fmt.Sprintf(c.query, details.TableID, aost.String()),
			c.args...,
		)
		if err != nil {
			return err
		}
		c.gauge.Update(int64(tree.MustBeDInt(datums[0])))
	}
	return nil
}

// rangeRowCount should be checked even if the function returns an error because it may have partially succeeded
func runTTLOnRange(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.RowLevelTTLDetails,
	descriptors *descs.Collection,
	knobs sql.TTLTestingKnobs,
	metrics rowLevelTTLMetrics,
	tableVersion descpb.DescriptorVersion,
	startPK tree.Datums,
	endPK tree.Datums,
	pkColumns []string,
	relationName string,
	selectBatchSize, deleteBatchSize int64,
	deleteRateLimiter *quotapool.RateLimiter,
	aost tree.DTimestampTZ,
	ttlExpression string,
) (rangeRowCount int64, err error) {
	metrics.NumActiveRanges.Inc(1)
	defer metrics.NumActiveRanges.Dec(1)

	ie := execCfg.InternalExecutor
	db := execCfg.DB

	// TODO(#76914): look at using a dist sql flow job, utilize any existing index
	// on crdb_internal_expiration.

	selectBuilder := makeSelectQueryBuilder(
		details.TableID,
		details.Cutoff,
		pkColumns,
		relationName,
		startPK,
		endPK,
		aost,
		selectBatchSize,
		ttlExpression,
	)
	deleteBuilder := makeDeleteQueryBuilder(
		details.TableID,
		details.Cutoff,
		pkColumns,
		relationName,
		deleteBatchSize,
		ttlExpression,
	)

	if preSelectDeleteStatement := knobs.PreSelectDeleteStatement; preSelectDeleteStatement != "" {
		if _, err := ie.ExecEx(
			ctx,
			"pre-select-delete-statement",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			preSelectDeleteStatement,
		); err != nil {
			return rangeRowCount, err
		}
	}

	for {
		// Check the job is enabled on every iteration.
		if err := checkEnabled(execCfg.SV()); err != nil {
			return rangeRowCount, err
		}

		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		start := timeutil.Now()
		expiredRowsPKs, err := selectBuilder.run(ctx, ie)
		metrics.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
		if err != nil {
			return rangeRowCount, errors.Wrapf(err, "error selecting rows to delete")
		}
		numExpiredRows := int64(len(expiredRowsPKs))
		metrics.RowSelections.Inc(numExpiredRows)

		// Step 2. Delete the rows which have expired.

		for startRowIdx := int64(0); startRowIdx < numExpiredRows; startRowIdx += deleteBatchSize {
			until := startRowIdx + deleteBatchSize
			if until > numExpiredRows {
				until = numExpiredRows
			}
			deleteBatch := expiredRowsPKs[startRowIdx:until]
			if err := db.TxnWithSteppingEnabled(ctx, sessiondatapb.TTLLow, func(ctx context.Context, txn *kv.Txn) error {
				// If we detected a schema change here, the delete will not succeed
				// (the SELECT still will because of the AOST). Early exit here.
				desc, err := descriptors.GetImmutableTableByID(
					ctx,
					txn,
					details.TableID,
					tree.ObjectLookupFlagsWithRequired(),
				)
				if err != nil {
					return err
				}
				version := desc.GetVersion()
				if mockVersion := knobs.MockTableDescriptorVersionDuringDelete; mockVersion != nil {
					version = *mockVersion
				}
				if version != tableVersion {
					return errors.Newf(
						"table has had a schema change since the job has started at %s, aborting",
						desc.GetModificationTime().GoTime().Format(time.RFC3339),
					)
				}
				tokens, err := deleteRateLimiter.Acquire(ctx, int64(len(deleteBatch)))
				if err != nil {
					return err
				}
				defer tokens.Consume()

				start := timeutil.Now()
				rowCount, err := deleteBuilder.run(ctx, ie, txn, deleteBatch)
				if err != nil {
					return err
				}

				metrics.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
				rangeRowCount += int64(rowCount)
				return nil
			}); err != nil {
				return rangeRowCount, errors.Wrapf(err, "error during row deletion")
			}
			metrics.RowDeletions.Inc(int64(len(deleteBatch)))
		}

		// Step 3. Early exit if necessary.

		// If we selected less than the select batch size, we have selected every
		// row and so we end it here.
		if numExpiredRows < selectBatchSize {
			break
		}
	}
	return rangeRowCount, nil
}

// keyToDatums translates a RKey on a range for a table to the appropriate datums.
func keyToDatums(
	key roachpb.RKey, codec keys.SQLCodec, pkTypes []*types.T, alloc *tree.DatumAlloc,
) (tree.Datums, error) {
	rKey := key.AsRawKey()

	// If any of these errors, that means we reached an "empty" key, which
	// symbolizes the start or end of a range.
	if _, _, err := codec.DecodeTablePrefix(rKey); err != nil {
		return nil, nil //nolint:returnerrcheck
	}
	if _, _, _, err := codec.DecodeIndexPrefix(rKey); err != nil {
		return nil, nil //nolint:returnerrcheck
	}

	// Decode the datums ourselves, instead of using rowenc.DecodeKeyVals.
	// We cannot use rowenc.DecodeKeyVals because we may not have the entire PK
	// as the key for the range (e.g. a PK (a, b) may only be split on (a)).
	rKey, err := codec.StripTenantPrefix(key.AsRawKey())
	if err != nil {
		return nil, errors.Wrapf(err, "error decoding tenant prefix of %x", key)
	}
	rKey, _, _, err = rowenc.DecodePartialTableIDIndexID(key)
	if err != nil {
		return nil, errors.Wrapf(err, "error decoding table/index ID of %x", key)
	}
	encDatums := make([]rowenc.EncDatum, 0, len(pkTypes))
	for len(rKey) > 0 && len(encDatums) < len(pkTypes) {
		i := len(encDatums)
		// We currently assume all PRIMARY KEY columns are ascending, and block
		// creation otherwise.
		enc := descpb.DatumEncoding_ASCENDING_KEY
		var val rowenc.EncDatum
		val, rKey, err = rowenc.EncDatumFromBuffer(pkTypes[i], enc, rKey)
		if err != nil {
			return nil, errors.Wrapf(err, "error decoding EncDatum of %x", key)
		}
		encDatums = append(encDatums, val)
	}

	datums := make(tree.Datums, len(encDatums))
	for i, encDatum := range encDatums {
		if err := encDatum.EnsureDecoded(pkTypes[i], alloc); err != nil {
			return nil, errors.Wrapf(err, "error ensuring encoded of %x", key)
		}
		datums[i] = encDatum.Datum
	}
	return datums, nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
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
