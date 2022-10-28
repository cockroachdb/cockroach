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
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
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

// RowLevelTTLAggMetrics are the row-level TTL job agg metrics.
type RowLevelTTLAggMetrics struct {
	RangeTotalDuration *aggmetric.AggHistogram
	SelectDuration     *aggmetric.AggHistogram
	DeleteDuration     *aggmetric.AggHistogram
	RowSelections      *aggmetric.AggCounter
	RowDeletions       *aggmetric.AggCounter
	NumActiveRanges    *aggmetric.AggGauge
	TotalRows          *aggmetric.AggGauge
	TotalExpiredRows   *aggmetric.AggGauge

	defaultRowLevelMetrics rowLevelTTLMetrics
	mu                     struct {
		syncutil.Mutex
		m map[string]rowLevelTTLMetrics
	}
}

type rowLevelTTLMetrics struct {
	RangeTotalDuration *aggmetric.Histogram
	SelectDuration     *aggmetric.Histogram
	DeleteDuration     *aggmetric.Histogram
	RowSelections      *aggmetric.Counter
	RowDeletions       *aggmetric.Counter
	NumActiveRanges    *aggmetric.Gauge
	TotalRows          *aggmetric.Gauge
	TotalExpiredRows   *aggmetric.Gauge
}

// MetricStruct implements the metric.Struct interface.
func (m *RowLevelTTLAggMetrics) MetricStruct() {}

func (m *RowLevelTTLAggMetrics) metricsWithChildren(children ...string) rowLevelTTLMetrics {
	return rowLevelTTLMetrics{
		RangeTotalDuration: m.RangeTotalDuration.AddChild(children...),
		SelectDuration:     m.SelectDuration.AddChild(children...),
		DeleteDuration:     m.DeleteDuration.AddChild(children...),
		RowSelections:      m.RowSelections.AddChild(children...),
		RowDeletions:       m.RowDeletions.AddChild(children...),
		NumActiveRanges:    m.NumActiveRanges.AddChild(children...),
		TotalRows:          m.TotalRows.AddChild(children...),
		TotalExpiredRows:   m.TotalExpiredRows.AddChild(children...),
	}
}

var invalidPrometheusRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func (m *RowLevelTTLAggMetrics) loadMetrics(labelMetrics bool, relation string) rowLevelTTLMetrics {
	if !labelMetrics {
		return m.defaultRowLevelMetrics
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	relation = invalidPrometheusRe.ReplaceAllString(relation, "_")
	if ret, ok := m.mu.m[relation]; ok {
		return ret
	}
	ret := m.metricsWithChildren(relation)
	m.mu.m[relation] = ret
	return ret
}

func makeRowLevelTTLAggMetrics(histogramWindowInterval time.Duration) metric.Struct {
	sigFigs := 2
	b := aggmetric.MakeBuilder("relation")
	ret := &RowLevelTTLAggMetrics{
		RangeTotalDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.range_total_duration",
				Help:        "Duration for processing a range during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Hour.Nanoseconds(),
			sigFigs,
		),
		SelectDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.select_duration",
				Help:        "Duration for select requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Minute.Nanoseconds(),
			sigFigs,
		),
		DeleteDuration: b.Histogram(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.delete_duration",
				Help:        "Duration for delete requests during row level TTL.",
				Measurement: "nanoseconds",
				Unit:        metric.Unit_NANOSECONDS,
				MetricType:  io_prometheus_client.MetricType_HISTOGRAM,
			},
			histogramWindowInterval,
			time.Minute.Nanoseconds(),
			sigFigs,
		),
		RowSelections: b.Counter(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.rows_selected",
				Help:        "Number of rows selected for deletion by the row level TTL job.",
				Measurement: "num_rows",
				Unit:        metric.Unit_COUNT,
				MetricType:  io_prometheus_client.MetricType_COUNTER,
			},
		),
		RowDeletions: b.Counter(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.rows_deleted",
				Help:        "Number of rows deleted by the row level TTL job.",
				Measurement: "num_rows",
				Unit:        metric.Unit_COUNT,
				MetricType:  io_prometheus_client.MetricType_COUNTER,
			},
		),
		NumActiveRanges: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.num_active_ranges",
				Help:        "Number of active workers attempting to delete for row level TTL.",
				Measurement: "num_active_workers",
				Unit:        metric.Unit_COUNT,
			},
		),
		TotalRows: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.total_rows",
				Help:        "Approximate number of rows on the TTL table.",
				Measurement: "total_rows",
				Unit:        metric.Unit_COUNT,
			},
		),
		TotalExpiredRows: b.Gauge(
			metric.Metadata{
				Name:        "jobs.row_level_ttl.total_expired_rows",
				Help:        "Approximate number of rows that have expired the TTL on the TTL table.",
				Measurement: "total_expired_rows",
				Unit:        metric.Unit_COUNT,
			},
		),
	}
	ret.defaultRowLevelMetrics = ret.metricsWithChildren("default")
	ret.mu.m = make(map[string]rowLevelTTLMetrics)
	return ret
}

var _ jobs.Resumer = (*rowLevelTTLResumer)(nil)

// Resume implements the jobs.Resumer interface.
func (t rowLevelTTLResumer) Resume(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	db := p.ExecCfg().DB
	descsCol := p.ExtendedEvalContext().Descs

	if enabled := jobEnabled.Get(p.ExecCfg().SV()); !enabled {
		return errors.Newf(
			"ttl jobs are currently disabled by CLUSTER SETTING %s",
			jobEnabled.Key(),
		)
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
	var entirePKSpan roachpb.Span
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
		entirePKSpan = desc.PrimaryIndexSpan(p.ExecCfg().Codec)
		ttlSettings = *ttl
		return nil
	}); err != nil {
		return err
	}

	var metrics = p.ExecCfg().JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
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

	statsCloseCh := make(chan struct{})
	ch := make(chan rangeToProcess, rangeConcurrency)
	for i := 0; i < rangeConcurrency; i++ {
		g.GoCtx(func(ctx context.Context) error {
			for r := range ch {
				start := timeutil.Now()
				err := runTTLOnRange(
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
				)
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
			// Do once initially to ensure we have some base statistics.
			fetchStatistics(ctx, p.ExecCfg(), knobs, relationName, details, metrics, aostDuration)
			// Wait until poll interval is reached, or early exit when we are done
			// with the TTL job.
			for {
				select {
				case <-statsCloseCh:
					return nil
				case <-time.After(ttlSettings.RowStatsPollInterval):
					fetchStatistics(ctx, p.ExecCfg(), knobs, relationName, details, metrics, aostDuration)
				}
			}
		})
	}

	distSQLPlanner := p.DistSQLPlanner()
	evalCtx := p.ExtendedEvalContext()
	planCtx, _, err := distSQLPlanner.SetupAllNodesPlanning(ctx, evalCtx, p.ExecCfg())
	if err != nil {
		return err
	}
	spanPartitions, err := distSQLPlanner.PartitionSpans(ctx, planCtx, []roachpb.Span{entirePKSpan})
	if err != nil {
		return err
	}

	// Iterate over every span to feed work for the goroutine processors.
	if err := func() (retErr error) {
		defer func() {
			close(ch)
			close(statsCloseCh)
			retErr = errors.CombineErrors(retErr, g.Wait())
		}()

		for _, spanParititon := range spanPartitions {
			for _, span := range spanParititon.Spans {
				startPK, err := keyToDatums(roachpb.RKey(span.Key), p.ExecCfg().Codec, pkTypes, &alloc)
				if err != nil {
					return err
				}
				endPK, err := keyToDatums(roachpb.RKey(span.EndKey), p.ExecCfg().Codec, pkTypes, &alloc)
				if err != nil {
					return err
				}
				ch <- rangeToProcess{
					startPK: startPK,
					endPK:   endPK,
				}
			}
		}
		return nil
	}(); err != nil {
		return err
	}
	return nil
}

func getSelectBatchSize(sv *settings.Values, ttl catpb.RowLevelTTL) int {
	if bs := ttl.SelectBatchSize; bs != 0 {
		return int(bs)
	}
	return int(defaultSelectBatchSize.Get(sv))
}

func getDeleteBatchSize(sv *settings.Values, ttl catpb.RowLevelTTL) int {
	if bs := ttl.DeleteBatchSize; bs != 0 {
		return int(bs)
	}
	return int(defaultDeleteBatchSize.Get(sv))
}

func getRangeConcurrency(sv *settings.Values, ttl catpb.RowLevelTTL) int {
	if rc := ttl.RangeConcurrency; rc != 0 {
		return int(rc)
	}
	return int(defaultRangeConcurrency.Get(sv))
}

func getDeleteRateLimit(sv *settings.Values, ttl catpb.RowLevelTTL) int64 {
	val := func() int64 {
		if bs := ttl.DeleteRateLimit; bs != 0 {
			return bs
		}
		return defaultDeleteRateLimit.Get(sv)
	}()
	// Put the maximum tokens possible if there is no rate limit.
	if val == 0 {
		return math.MaxInt64
	}
	return val
}

func fetchStatistics(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	knobs sql.TTLTestingKnobs,
	relationName string,
	details jobspb.RowLevelTTLDetails,
	metrics rowLevelTTLMetrics,
	aostDuration time.Duration,
) {
	if err := func() error {
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
				query:  `SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s WHERE ` + colinfo.TTLDefaultExpirationColumnName + ` < $1`,
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
					User:             security.RootUserName(),
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
	}(); err != nil {
		if onStatisticsError := knobs.OnStatisticsError; onStatisticsError != nil {
			onStatisticsError(err)
		}
		log.Warningf(ctx, "failed to get statistics for table id %d: %s", details.TableID, err)
	}
}

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
	selectBatchSize, deleteBatchSize int,
	deleteRateLimiter *quotapool.RateLimiter,
	aost tree.DTimestampTZ,
) error {
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
	)
	deleteBuilder := makeDeleteQueryBuilder(
		details.TableID,
		details.Cutoff,
		pkColumns,
		relationName,
		deleteBatchSize,
	)

	for {
		if f := knobs.OnDeleteLoopStart; f != nil {
			if err := f(); err != nil {
				return err
			}
		}

		// Check the job is enabled on every iteration.
		if enabled := jobEnabled.Get(execCfg.SV()); !enabled {
			return errors.Newf(
				"ttl jobs are currently disabled by CLUSTER SETTING %s",
				jobEnabled.Key(),
			)
		}

		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		start := timeutil.Now()
		expiredRowsPKs, err := selectBuilder.run(ctx, ie)
		metrics.SelectDuration.RecordValue(int64(timeutil.Since(start)))
		if err != nil {
			return errors.Wrapf(err, "error selecting rows to delete")
		}
		metrics.RowSelections.Inc(int64(len(expiredRowsPKs)))

		// Step 2. Delete the rows which have expired.

		for startRowIdx := 0; startRowIdx < len(expiredRowsPKs); startRowIdx += deleteBatchSize {
			until := startRowIdx + deleteBatchSize
			if until > len(expiredRowsPKs) {
				until = len(expiredRowsPKs)
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
				if mockVersion := knobs.MockDescriptorVersionDuringDelete; mockVersion != nil {
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
				batchRowCount, err := deleteBuilder.run(ctx, ie, txn, deleteBatch)
				metrics.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
				metrics.RowDeletions.Inc(batchRowCount)
				return err
			}); err != nil {
				return errors.Wrapf(err, "error during row deletion")
			}
		}

		// Step 3. Early exit if necessary.

		// If we selected less than the select batch size, we have selected every
		// row and so we end it here.
		if len(expiredRowsPKs) < selectBatchSize {
			break
		}
	}
	return nil
}

// keyToDatums translates a RKey on a span for a table to the appropriate datums.
func keyToDatums(
	rKey roachpb.RKey, codec keys.SQLCodec, pkTypes []*types.T, alloc *tree.DatumAlloc,
) (tree.Datums, error) {

	key := rKey.AsRawKey()

	// Decode the datums ourselves, instead of using rowenc.DecodeKeyVals.
	// We cannot use rowenc.DecodeKeyVals because we may not have the entire PK
	// as the key for the span (e.g. a PK (a, b) may only be split on (a)).
	key, err := codec.StripTenantPrefix(key)
	if err != nil {
		// Convert rKey to []byte to prevent hex encoding output of RKey.String().
		return nil, errors.Wrapf(err, "error decoding tenant prefix of %x", []byte(rKey))
	}
	key, _, _, err = rowenc.DecodePartialTableIDIndexID(key)
	if err != nil {
		// Convert rKey to []byte to prevent hex encoding output of RKey.String().
		return nil, errors.Wrapf(err, "error decoding table/index ID of %x", []byte(rKey))
	}
	encDatums := make([]rowenc.EncDatum, 0, len(pkTypes))
	for len(key) > 0 && len(encDatums) < len(pkTypes) {
		i := len(encDatums)
		// We currently assume all PRIMARY KEY columns are ascending, and block
		// creation otherwise.
		enc := descpb.DatumEncoding_ASCENDING_KEY
		var val rowenc.EncDatum
		val, key, err = rowenc.EncDatumFromBuffer(pkTypes[i], enc, key)
		if err != nil {
			// Convert rKey to []byte to prevent hex encoding output of RKey.String().
			return nil, errors.Wrapf(err, "error decoding EncDatum of %x", []byte(rKey))
		}
		encDatums = append(encDatums, val)
	}

	datums := make(tree.Datums, len(encDatums))
	for i, encDatum := range encDatums {
		if err := encDatum.EnsureDecoded(pkTypes[i], alloc); err != nil {
			// Convert rKey to []byte to prevent hex encoding output of RKey.String().
			return nil, errors.Wrapf(err, "error ensuring encoding of %x", []byte(rKey))
		}
		datums[i] = encDatum.Datum
	}
	return datums, nil
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
	jobs.MakeRowLevelTTLMetricsHook = makeRowLevelTTLAggMetrics
}
