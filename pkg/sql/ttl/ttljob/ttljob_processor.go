// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// ttlMaxKVAutoRetry is the maximum number of times a TTL operation will
// automatically retry in the KV layer before reducing the batch size to handle
// contention.
var ttlMaxKVAutoRetry = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.ttl.max_kv_auto_retries",
	"the number of times a TTL operation will automatically retry in the KV layer before reducing the batch size",
	10,
	settings.PositiveInt,
)

// ttlProcessor manages the work managed by a single node for a job run by
// rowLevelTTLResumer. SpanToQueryBounds converts a DistSQL span into
// QueryBounds. The QueryBounds are passed to SelectQueryBuilder and
// DeleteQueryBuilder which manage the state for the SELECT/DELETE loop
// that is run by runTTLOnQueryBounds.
type ttlProcessor struct {
	execinfra.ProcessorBase
	ttlSpec              execinfrapb.TTLSpec
	processorConcurrency int64
	progressUpdater      ttlProgressUpdater
}

var _ execinfra.RowSource = (*ttlProcessor)(nil)
var _ execinfra.Processor = (*ttlProcessor)(nil)

// ttlProgressUpdater abstracts how a TTL processor reports its progress.
// Implementations can either write directly to the job table (legacy) or stream
// metadata back to the coordinator (preferred).
type ttlProgressUpdater interface {
	// InitProgress is called once at the beginning of the TTL processor.
	InitProgress(totalSpanCount int64)
	// UpdateProgress is called to refresh the TTL processor progress.
	UpdateProgress(ctx context.Context, output execinfra.RowReceiver) error
	// OnSpanProcessed is called each time a span has been processed (even partially).
	OnSpanProcessed(spansProcessed, deletedRowCount int64)
	// FinalizeProgress is the final call to update the progress once all spans have been processed.
	FinalizeProgress(ctx context.Context, output execinfra.RowReceiver) error
}

// directJobProgressUpdater handles TTL progress updates by writing directly to
// the jobs table from this processor. This is the legacy model and exists to
// support mixed-version scenarios.
//
// This can be removed once version 25.4 is the minimum supported version.
type directJobProgressUpdater struct {
	// proc references the running TTL processor.
	proc *ttlProcessor

	// updateEvery is the number of spans that must be processed before triggering a progress update.
	updateEvery int64

	// updateEveryDuration is the minimum amount of time that must pass between progress updates.
	updateEveryDuration time.Duration

	// lastUpdated records the time of the last progress update.
	lastUpdated time.Time

	// totalSpanCount is the total number of spans assigned to this processor.
	totalSpanCount int64

	// rowsProcessed is the cumulative number of rows deleted.
	rowsProcessed atomic.Int64

	// rowsProcessedSinceLastUpdate is the number of rows deleted since the last progress update.
	rowsProcessedSinceLastUpdate atomic.Int64

	// spansProcessed is the cumulative number of spans processed.
	spansProcessed atomic.Int64

	// spansProcessedSinceLastUpdate is the number of spans processed since the last progress update.
	spansProcessedSinceLastUpdate atomic.Int64
}

// coordinatorStreamUpdater handles TTL progress updates by flowing the
// information back to the coordinator. The coordinator is then responsible for
// writing that back to the jobs table.
type coordinatorStreamUpdater struct {
	// proc references the running TTL processor.
	proc *ttlProcessor

	// totalSpanCount is the total number of spans assigned to this processor.
	totalSpanCount int64

	// deletedRowCount tracks the cumulative number of rows deleted by this processor.
	deletedRowCount atomic.Int64

	// processedSpanCount tracks the number of spans this processor has reported as processed.
	processedSpanCount atomic.Int64

	// progressLogger is used to control how often we log progress updates that
	// are sent back to the coordinator.
	progressLogger log.EveryN
}

// Start implements the execinfra.RowSource interface.
func (t *ttlProcessor) Start(context.Context) {}

// Run implements the execinfra.Processor interface.
func (t *ttlProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	ctx = t.StartInternal(ctx, "ttl")
	v := execversion.FromContext(ctx)
	// TTL processors support two progress update models. The legacy model (used in V25_2)
	// has each processor write progress directly to the job table. The newer model flows
	// progress metadata back to the coordinator, which handles the job table updates centrally.
	// The selected behavior is gated on the active cluster version.
	// TODO(spilchen): remove directJobProgerssUpdater once 25.4 is the minimum supported version.
	if v == execversion.V25_2 {
		t.progressUpdater = &directJobProgressUpdater{proc: t}
	} else {
		t.progressUpdater = &coordinatorStreamUpdater{proc: t, progressLogger: log.Every(1 * time.Minute)}
	}
	err := t.work(ctx, output)
	if err != nil {
		output.Push(nil, &execinfrapb.ProducerMetadata{Err: err})
	}
	execinfra.SendTraceData(ctx, t.FlowCtx, output)
	output.ProducerDone()
}

func getTableInfo(
	ctx context.Context, db descs.DB, tableID descpb.ID,
) (
	relationName string,
	pkColIDs catalog.TableColMap,
	pkColNames []string,
	pkColTypes []*types.T,
	pkColDirs []catenumpb.IndexColumn_Direction,
	numFamilies int,
	labelMetrics bool,
	err error,
) {
	err = db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
		if err != nil {
			return err
		}

		numFamilies = desc.NumFamilies()
		var buf bytes.Buffer
		primaryIndexDesc := desc.GetPrimaryIndex().IndexDesc()
		pkColNames = make([]string, 0, len(primaryIndexDesc.KeyColumnNames))
		for _, name := range primaryIndexDesc.KeyColumnNames {
			lexbase.EncodeRestrictedSQLIdent(&buf, name, lexbase.EncNoFlags)
			pkColNames = append(pkColNames, buf.String())
			buf.Reset()
		}
		pkColTypes, err = spanutils.GetPKColumnTypes(desc, primaryIndexDesc)
		if err != nil {
			return err
		}
		pkColDirs = primaryIndexDesc.KeyColumnDirections
		pkColIDs = catalog.TableColMap{}
		for i, id := range primaryIndexDesc.KeyColumnIDs {
			pkColIDs.Set(id, i)
		}

		if !desc.HasRowLevelTTL() {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		rowLevelTTL := desc.GetRowLevelTTL()
		labelMetrics = rowLevelTTL.LabelMetrics

		tn, err := descs.GetObjectName(ctx, txn.KV(), txn.Descriptors(), desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}

		relationName = tn.FQString() + "@" + lexbase.EscapeSQLIdent(primaryIndexDesc.Name)
		return nil
	})
	return relationName, pkColIDs, pkColNames, pkColTypes, pkColDirs, numFamilies, labelMetrics, err
}

func (t *ttlProcessor) work(ctx context.Context, output execinfra.RowReceiver) error {
	ttlSpec := t.ttlSpec
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	db := serverCfg.DB
	codec := serverCfg.Codec
	details := ttlSpec.RowLevelTTLDetails
	tableID := details.TableID
	cutoff := details.Cutoff
	ttlExpr := ttlSpec.TTLExpr

	// Note: the ttl-restart test depends on this message to know what nodes are
	// involved in a TTL job.
	log.Dev.Infof(ctx, "TTL processor started processorID=%d tableID=%d", t.ProcessorID, tableID)

	// Each node sets up two rate limiters (one for SELECT, one for DELETE) per
	// table. The limiters apply to all ranges assigned to this processor, whether
	// or not the node is the leaseholder for those ranges.

	selectRateLimit := ttlSpec.SelectRateLimit
	// Default 0 value to "unlimited" in case job started on node <= v23.2.
	// todo(sql-foundations): Remove this in 25.1 for consistency with
	//  deleteRateLimit.
	if selectRateLimit == 0 {
		selectRateLimit = math.MaxInt64
	}
	selectRateLimiter := quotapool.NewRateLimiter(
		"ttl-select",
		quotapool.Limit(selectRateLimit),
		selectRateLimit,
	)

	deleteRateLimit := ttlSpec.DeleteRateLimit
	deleteRateLimiter := quotapool.NewRateLimiter(
		"ttl-delete",
		quotapool.Limit(deleteRateLimit),
		deleteRateLimit,
	)

	relationName, pkColIDs, pkColNames, pkColTypes, pkColDirs, numFamilies, labelMetrics, err := getTableInfo(
		ctx, db, tableID,
	)
	if err != nil {
		return err
	}

	jobRegistry := serverCfg.JobRegistry
	metrics := jobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
		labelMetrics,
		relationName,
	)

	group := ctxgroup.WithContext(ctx)
	totalSpanCount := int64(len(ttlSpec.Spans))
	t.progressUpdater.InitProgress(totalSpanCount)
	t.processorConcurrency = ttlbase.GetProcessorConcurrency(&flowCtx.Cfg.Settings.SV, int64(runtime.GOMAXPROCS(0)))
	if totalSpanCount < t.processorConcurrency {
		t.processorConcurrency = totalSpanCount
	}

	err = func() error {
		boundsChan := make(chan spanutils.QueryBounds, t.processorConcurrency)
		defer close(boundsChan)
		for i := int64(0); i < t.processorConcurrency; i++ {
			group.GoCtx(func(ctx context.Context) error {
				for bounds := range boundsChan {
					start := timeutil.Now()
					selectBuilder := MakeSelectQueryBuilder(
						SelectQueryParams{
							RelationName:      relationName,
							PKColNames:        pkColNames,
							PKColDirs:         pkColDirs,
							PKColTypes:        pkColTypes,
							Bounds:            bounds,
							AOSTDuration:      ttlSpec.AOSTDuration,
							SelectBatchSize:   ttlSpec.SelectBatchSize,
							TTLExpr:           ttlExpr,
							SelectDuration:    metrics.SelectDuration,
							SelectRateLimiter: selectRateLimiter,
						},
						cutoff,
					)
					deleteBuilder := MakeDeleteQueryBuilder(
						DeleteQueryParams{
							RelationName:      relationName,
							PKColNames:        pkColNames,
							DeleteBatchSize:   ttlSpec.DeleteBatchSize,
							TTLExpr:           ttlExpr,
							DeleteDuration:    metrics.DeleteDuration,
							DeleteRateLimiter: deleteRateLimiter,
						},
						cutoff,
					)
					spanDeletedRowCount, err := t.runTTLOnQueryBounds(
						ctx,
						metrics,
						selectBuilder,
						deleteBuilder,
					)
					// Add to totals even on partial success.
					t.progressUpdater.OnSpanProcessed(1 /* spansProcessed */, spanDeletedRowCount)
					if err != nil {
						// Continue until channel is fully read.
						// Otherwise, the keys input will be blocked.
						for bounds = range boundsChan {
						}
						return err
					}
					metrics.SpanTotalDuration.RecordValue(int64(timeutil.Since(start)))
				}
				return nil
			})
		}

		// Iterate over every span to feed work for the goroutine processors.
		kvDB := db.KV()
		var alloc tree.DatumAlloc
		for i, span := range ttlSpec.Spans {
			if bounds, hasRows, err := spanutils.SpanToQueryBounds(
				ctx,
				kvDB,
				codec,
				pkColIDs,
				pkColTypes,
				pkColDirs,
				numFamilies,
				span,
				&alloc,
			); err != nil {
				return errors.Wrapf(err, "SpanToQueryBounds error index=%d span=%s", i, span)
			} else if hasRows {
				// Only process bounds from spans with rows inside them.
				boundsChan <- bounds
			} else {
				// If the span has no rows, we still need to increment the processed
				// count.
				t.progressUpdater.OnSpanProcessed(1 /* spansProcessed */, 0 /* deletedRowCount */)
			}

			if err := t.progressUpdater.UpdateProgress(ctx, output); err != nil {
				return err
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	if err := group.Wait(); err != nil {
		return err
	}
	return t.progressUpdater.FinalizeProgress(ctx, output)
}

// runTTLOnQueryBounds runs the SELECT/DELETE loop for a single DistSQL span.
// spanRowCount should be checked even if the function returns an error
// because it may have partially succeeded.
func (t *ttlProcessor) runTTLOnQueryBounds(
	ctx context.Context,
	metrics rowLevelTTLMetrics,
	selectBuilder SelectQueryBuilder,
	deleteBuilder DeleteQueryBuilder,
) (spanRowCount int64, err error) {
	metrics.NumActiveSpans.Inc(1)
	defer metrics.NumActiveSpans.Dec(1)

	// TODO(#82140): investigate improving row deletion performance with secondary indexes

	ttlSpec := t.ttlSpec
	details := ttlSpec.RowLevelTTLDetails
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	ie := serverCfg.DB.Executor()

	preSelectStatement := ttlSpec.PreSelectStatement
	if preSelectStatement != "" {
		if _, err := ie.ExecEx(
			ctx,
			"pre-select-delete-statement",
			nil, /* txn */
			// This is a test-only knob, so we're ok not specifying custom
			// InternalExecutorOverride.
			sessiondata.NodeUserSessionDataOverride,
			preSelectStatement,
		); err != nil {
			return spanRowCount, err
		}
	}

	settingsValues := &serverCfg.Settings.SV
	for {
		// Check the job is enabled on every iteration.
		if err := ttlbase.CheckJobEnabled(settingsValues); err != nil {
			return spanRowCount, err
		}

		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		expiredRowsPKs, hasNext, err := selectBuilder.Run(ctx, ie)
		if err != nil {
			return spanRowCount, errors.Wrapf(err, "error selecting rows to delete")
		}

		numExpiredRows := len(expiredRowsPKs)
		metrics.RowSelections.Inc(int64(numExpiredRows))

		// Step 2. Delete the rows which have expired.
		deleteBatchSize := deleteBuilder.GetBatchSize()
		for startRowIdx := 0; startRowIdx < numExpiredRows; startRowIdx += deleteBatchSize {
			// We are going to attempt a delete of size deleteBatchSize. But we use
			// retry.Batch to allow retrying with a smaller batch size in case of
			// an error.
			rb := retry.Batch{
				Do: func(ctx context.Context, processed, batchSize int) error {
					until := startRowIdx + processed + batchSize
					if until > numExpiredRows {
						until = numExpiredRows
					}
					deleteBatch := expiredRowsPKs[startRowIdx+processed : until]
					var batchRowCount int64
					do := func(ctx context.Context, txn descs.Txn) error {
						txn.KV().SetDebugName("ttljob-delete-batch")
						// We explicitly specify a low retry limit because this operation is
						// wrapped with its own retry function that will also take care of
						// adjusting the batch size on each retry.
						maxAutoRetries := ttlMaxKVAutoRetry.Get(&flowCtx.Cfg.Settings.SV)
						txn.KV().SetMaxAutoRetries(int(maxAutoRetries))
						if ttlSpec.DisableChangefeedReplication {
							txn.KV().SetOmitInRangefeeds()
						}
						// If we detected a schema change here, the DELETE will not succeed
						// (the SELECT still will because of the AOST). Early exit here.
						desc, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, details.TableID)
						if err != nil {
							return err
						}
						if ttlSpec.PreDeleteChangeTableVersion || desc.GetVersion() != details.TableVersion {
							return errors.Newf(
								"table has had a schema change since the job has started at %s, job will run at the next scheduled time",
								desc.GetModificationTime().GoTime().Format(time.RFC3339),
							)
						}
						batchRowCount, err = deleteBuilder.Run(ctx, txn, deleteBatch)
						if err != nil {
							return err
						}
						return nil
					}
					if err := serverCfg.DB.DescsTxn(
						ctx, do, isql.SteppingEnabled(), isql.WithPriority(admissionpb.BulkLowPri),
					); err != nil {
						return errors.Wrapf(err, "error during row deletion")
					}
					metrics.RowDeletions.Inc(batchRowCount)
					spanRowCount += batchRowCount
					return nil
				},
				IsRetryableError: kv.IsAutoRetryLimitExhaustedError,
				OnRetry: func(err error, nextBatchSize int) error {
					metrics.NumDeleteBatchRetries.Inc(1)
					log.Dev.Infof(ctx,
						"row-level TTL reached the auto-retry limit, reducing batch size to %d rows. Error: %v",
						nextBatchSize, err)
					return nil
				},
			}
			// Adjust the batch size if we are on the final batch.
			deleteBatchSize = min(deleteBatchSize, numExpiredRows-startRowIdx)
			if err := rb.Execute(ctx, deleteBatchSize); err != nil {
				return spanRowCount, err
			}
		}

		// Step 3. Early exit if necessary.

		// If we selected less than the select batch size, we have selected every
		// row and so we end it here.
		if !hasNext {
			break
		}
	}

	return spanRowCount, nil
}

func (t *ttlProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, t.DrainHelper()
}

func newTTLProcessor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, processorID int32, spec execinfrapb.TTLSpec,
) (execinfra.Processor, error) {
	ttlProcessor := &ttlProcessor{
		ttlSpec: spec,
	}
	if err := ttlProcessor.Init(
		ctx,
		ttlProcessor,
		&execinfrapb.PostProcessSpec{},
		[]*types.T{},
		flowCtx,
		processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return ttlProcessor, nil
}

// InitProgress implements the ttlProgressUpdater interface.
func (c *coordinatorStreamUpdater) InitProgress(totalSpanCount int64) {
	c.totalSpanCount = totalSpanCount
}

// OnSpanProcessed implements the ttlProgressUpdater interface.
func (c *coordinatorStreamUpdater) OnSpanProcessed(spansProcessed, deletedRowCount int64) {
	c.deletedRowCount.Add(deletedRowCount)
	c.processedSpanCount.Add(spansProcessed)
}

// UpdateProgress implements the ttlProgressUpdater interface.
func (c *coordinatorStreamUpdater) UpdateProgress(
	ctx context.Context, output execinfra.RowReceiver,
) error {
	nodeID := c.proc.FlowCtx.NodeID.SQLInstanceID()
	progressMsg := &jobspb.RowLevelTTLProcessorProgress{
		ProcessorID:          c.proc.ProcessorID,
		SQLInstanceID:        nodeID,
		ProcessorConcurrency: c.proc.processorConcurrency,
		DeletedRowCount:      c.deletedRowCount.Load(),
		ProcessedSpanCount:   c.processedSpanCount.Load(),
		TotalSpanCount:       c.totalSpanCount,
	}
	progressAny, err := pbtypes.MarshalAny(progressMsg)
	if err != nil {
		return errors.Wrap(err, "unable to marshal TTL processor progress")
	}
	meta := &execinfrapb.ProducerMetadata{
		BulkProcessorProgress: &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
			ProgressDetails: *progressAny,
			NodeID:          nodeID,
			FlowID:          c.proc.FlowCtx.ID,
			ProcessorID:     c.proc.ProcessorID,
			Drained:         c.processedSpanCount.Load() == c.totalSpanCount,
		},
	}
	// Push progress after each span. Throttling is now handled by the coordinator.
	status := output.Push(nil, meta)
	if status != execinfra.NeedMoreRows {
		return errors.Errorf("output receiver rejected progress metadata: %v", status)
	}
	if c.progressLogger.ShouldLog() {
		log.Dev.Infof(ctx, "TTL processor progress: %v", progressMsg)
	}
	return nil
}

// FinalizeProgress implements the ttlProgressUpdater interface.
func (c *coordinatorStreamUpdater) FinalizeProgress(
	ctx context.Context, output execinfra.RowReceiver,
) error {
	return c.UpdateProgress(ctx, output)
}

// InitProgress implements the ttlProgressUpdater interface.
func (d *directJobProgressUpdater) InitProgress(totalSpanCount int64) {
	// Update progress for approximately every 1% of spans processed, at least
	// 60 seconds apart with jitter.
	d.totalSpanCount = totalSpanCount
	d.updateEvery = max(1, totalSpanCount/100)
	d.updateEveryDuration = 60*time.Second + time.Duration(rand.Int63n(10*1000))*time.Millisecond
	d.lastUpdated = timeutil.Now()
}

// OnSpanProcessed implements the ttlProgressUpdater interface.
func (d *directJobProgressUpdater) OnSpanProcessed(spansProcessed, deletedRowCount int64) {
	d.rowsProcessed.Add(deletedRowCount)
	d.rowsProcessedSinceLastUpdate.Add(deletedRowCount)
	d.spansProcessed.Add(spansProcessed)
	d.spansProcessedSinceLastUpdate.Add(spansProcessed)
}

func (d *directJobProgressUpdater) updateFractionCompleted(ctx context.Context) error {
	jobID := d.proc.ttlSpec.JobID
	d.lastUpdated = timeutil.Now()
	spansToAdd := d.spansProcessedSinceLastUpdate.Swap(0)
	rowsToAdd := d.rowsProcessedSinceLastUpdate.Swap(0)

	var deletedRowCount, processedSpanCount, totalSpanCount int64
	var fractionCompleted float32

	err := d.proc.FlowCtx.Cfg.JobRegistry.UpdateJobWithTxn(
		ctx,
		jobID,
		nil, /* txn */
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			rowLevelTTL.JobProcessedSpanCount += spansToAdd
			rowLevelTTL.JobDeletedRowCount += rowsToAdd
			deletedRowCount = rowLevelTTL.JobDeletedRowCount
			processedSpanCount = rowLevelTTL.JobProcessedSpanCount
			totalSpanCount = rowLevelTTL.JobTotalSpanCount

			fractionCompleted = float32(rowLevelTTL.JobProcessedSpanCount) / float32(rowLevelTTL.JobTotalSpanCount)
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: fractionCompleted,
			}

			ju.UpdateProgress(progress)
			return nil
		},
	)
	if err != nil {
		return err
	}
	log.Dev.Infof(
		ctx,
		"TTL fractionCompleted updated processorID=%d tableID=%d deletedRowCount=%d processedSpanCount=%d totalSpanCount=%d fractionCompleted=%.3f",
		d.proc.ProcessorID, d.proc.ttlSpec.RowLevelTTLDetails.TableID, deletedRowCount, processedSpanCount, totalSpanCount, fractionCompleted,
	)
	return nil
}

// UpdateProgress implements the ttlProgressUpdater interface.
func (d *directJobProgressUpdater) UpdateProgress(
	ctx context.Context, _ execinfra.RowReceiver,
) error {
	if d.spansProcessedSinceLastUpdate.Load() >= d.updateEvery &&
		timeutil.Since(d.lastUpdated) >= d.updateEveryDuration {
		if err := d.updateFractionCompleted(ctx); err != nil {
			return err
		}
	}
	return nil
}

// FinalizeProgress implements the ttlProgressUpdater interface.
func (d *directJobProgressUpdater) FinalizeProgress(
	ctx context.Context, _ execinfra.RowReceiver,
) error {
	if err := d.updateFractionCompleted(ctx); err != nil {
		return err
	}

	sqlInstanceID := d.proc.FlowCtx.NodeID.SQLInstanceID()
	jobID := d.proc.ttlSpec.JobID
	return d.proc.FlowCtx.Cfg.JobRegistry.UpdateJobWithTxn(
		ctx,
		jobID,
		nil, /* txn */
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			processorID := d.proc.ProcessorID
			rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, jobspb.RowLevelTTLProcessorProgress{
				ProcessorID:          processorID,
				SQLInstanceID:        sqlInstanceID,
				DeletedRowCount:      d.rowsProcessed.Load(),
				ProcessedSpanCount:   d.spansProcessed.Load(),
				ProcessorConcurrency: d.proc.processorConcurrency,
			})
			var fractionCompleted float32
			if f, ok := progress.Progress.(*jobspb.Progress_FractionCompleted); ok {
				fractionCompleted = f.FractionCompleted
			}
			ju.UpdateProgress(progress)
			log.Dev.VInfof(
				ctx,
				2, /* level */
				"TTL processorRowCount updated processorID=%d sqlInstanceID=%d tableID=%d jobRowCount=%d processorRowCount=%d fractionCompleted=%.3f",
				processorID, sqlInstanceID, d.proc.ttlSpec.RowLevelTTLDetails.TableID, rowLevelTTL.JobDeletedRowCount,
				d.rowsProcessed.Load(), fractionCompleted,
			)
			return nil
		},
	)
}

func init() {
	rowexec.NewTTLProcessor = newTTLProcessor
}
