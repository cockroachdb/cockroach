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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ttlProcessor manages the work managed by a single node for a job run by
// rowLevelTTLResumer. SpanToQueryBounds converts a DistSQL span into
// QueryBounds. The QueryBounds are passed to SelectQueryBuilder and
// DeleteQueryBuilder which manage the state for the SELECT/DELETE loop
// that is run by runTTLOnQueryBounds.
type ttlProcessor struct {
	execinfra.ProcessorBase
	ttlSpec execinfrapb.TTLSpec
}

var _ execinfra.RowSource = (*ttlProcessor)(nil)

func (t *ttlProcessor) Start(ctx context.Context) {
	ctx = t.StartInternal(ctx, "ttl")
	err := t.work(ctx)
	t.MoveToDraining(err)
}

func getTableInfo(
	ctx context.Context, db descs.DB, descsCol *descs.Collection, tableID descpb.ID,
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
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		desc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, tableID)
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
		pkColTypes, err = GetPKColumnTypes(desc, primaryIndexDesc)
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

		tn, err := descs.GetObjectName(ctx, txn.KV(), descsCol, desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}

		relationName = tn.FQString() + "@" + lexbase.EscapeSQLIdent(primaryIndexDesc.Name)
		return nil
	})
	return relationName, pkColIDs, pkColNames, pkColTypes, pkColDirs, numFamilies, labelMetrics, err
}

func (t *ttlProcessor) work(ctx context.Context) error {

	ttlSpec := t.ttlSpec
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	db := serverCfg.DB
	descsCol := flowCtx.Descriptors
	codec := serverCfg.Codec
	details := ttlSpec.RowLevelTTLDetails
	tableID := details.TableID
	cutoff := details.Cutoff
	ttlExpr := ttlSpec.TTLExpr

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
		ctx, db, descsCol, tableID,
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
	processorSpanCount := int64(len(ttlSpec.Spans))
	processorConcurrency := int64(runtime.GOMAXPROCS(0))
	if processorSpanCount < processorConcurrency {
		processorConcurrency = processorSpanCount
	}
	var processorRowCount atomic.Int64
	var spansProccessedSinceLastUpdate atomic.Int64
	var rowsProccessedSinceLastUpdate atomic.Int64

	// Update progress for approximately every 1% of spans processed, at least
	// 60 seconds apart with jitter.
	updateEvery := max(1, processorSpanCount/100)
	updateEveryDuration := 60*time.Second + time.Duration(rand.Int63n(10*1000))*time.Millisecond
	lastUpdated := timeutil.Now()
	updateFractionCompleted := func() error {
		jobID := ttlSpec.JobID
		lastUpdated = timeutil.Now()
		spansToAdd := spansProccessedSinceLastUpdate.Swap(0)
		rowsToAdd := rowsProccessedSinceLastUpdate.Swap(0)

		var deletedRowCount, processedSpanCount, totalSpanCount int64
		var fractionCompleted float32

		err := jobRegistry.UpdateJobWithTxn(
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
		processorID := t.ProcessorID
		log.Infof(
			ctx,
			"TTL fractionCompleted updated processorID=%d tableID=%d deletedRowCount=%d processedSpanCount=%d totalSpanCount=%d fractionCompleted=%.3f",
			processorID, tableID, deletedRowCount, processedSpanCount, totalSpanCount, fractionCompleted,
		)
		return nil
	}

	err = func() error {
		boundsChan := make(chan QueryBounds, processorConcurrency)
		defer close(boundsChan)
		for i := int64(0); i < processorConcurrency; i++ {
			group.GoCtx(func(ctx context.Context) error {
				for bounds := range boundsChan {
					start := timeutil.Now()
					selectBuilder := MakeSelectQueryBuilder(
						SelectQueryParams{
							RelationName:      relationName,
							PKColNames:        pkColNames,
							PKColDirs:         pkColDirs,
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
					spanRowCount, err := t.runTTLOnQueryBounds(
						ctx,
						metrics,
						selectBuilder,
						deleteBuilder,
					)
					// add before returning err in case of partial success
					processorRowCount.Add(spanRowCount)
					rowsProccessedSinceLastUpdate.Add(spanRowCount)
					spansProccessedSinceLastUpdate.Add(1)
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
			if bounds, hasRows, err := SpanToQueryBounds(
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
				spansProccessedSinceLastUpdate.Add(1)
			}

			if spansProccessedSinceLastUpdate.Load() >= updateEvery &&
				timeutil.Since(lastUpdated) >= updateEveryDuration {
				if err := updateFractionCompleted(); err != nil {
					return err
				}
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
	if err := updateFractionCompleted(); err != nil {
		return err
	}

	sqlInstanceID := flowCtx.NodeID.SQLInstanceID()
	jobID := ttlSpec.JobID
	return jobRegistry.UpdateJobWithTxn(
		ctx,
		jobID,
		nil, /* txn */
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			processorID := t.ProcessorID
			rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, jobspb.RowLevelTTLProcessorProgress{
				ProcessorID:          processorID,
				SQLInstanceID:        sqlInstanceID,
				ProcessorRowCount:    processorRowCount.Load(),
				ProcessorSpanCount:   processorSpanCount,
				ProcessorConcurrency: processorConcurrency,
			})
			var fractionCompleted float32
			if f, ok := progress.Progress.(*jobspb.Progress_FractionCompleted); ok {
				fractionCompleted = f.FractionCompleted
			}
			ju.UpdateProgress(progress)
			log.VInfof(
				ctx,
				2, /* level */
				"TTL processorRowCount updated processorID=%d sqlInstanceID=%d tableID=%d jobRowCount=%d processorRowCount=%d fractionCompleted=%.3f",
				processorID, sqlInstanceID, tableID, rowLevelTTL.JobDeletedRowCount, processorRowCount.Load(), fractionCompleted,
			)
			return nil
		},
	)
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

		numExpiredRows := int64(len(expiredRowsPKs))
		metrics.RowSelections.Inc(numExpiredRows)

		// Step 2. Delete the rows which have expired.
		deleteBatchSize := deleteBuilder.DeleteBatchSize
		for startRowIdx := int64(0); startRowIdx < numExpiredRows; startRowIdx += deleteBatchSize {
			until := startRowIdx + deleteBatchSize
			if until > numExpiredRows {
				until = numExpiredRows
			}
			deleteBatch := expiredRowsPKs[startRowIdx:until]
			var batchRowCount int64
			do := func(ctx context.Context, txn isql.Txn) error {
				txn.KV().SetDebugName("ttljob-delete-batch")
				if ttlSpec.DisableChangefeedReplication {
					txn.KV().SetOmitInRangefeeds()
				}
				// If we detected a schema change here, the DELETE will not succeed
				// (the SELECT still will because of the AOST). Early exit here.
				desc, err := flowCtx.Descriptors.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, details.TableID)
				if err != nil {
					return err
				}
				if ttlSpec.PreDeleteChangeTableVersion || desc.GetVersion() != details.TableVersion {
					return errors.Newf(
						"table has had a schema change since the job has started at %s, aborting",
						desc.GetModificationTime().GoTime().Format(time.RFC3339),
					)
				}
				batchRowCount, err = deleteBuilder.Run(ctx, txn, deleteBatch)
				if err != nil {
					return err
				}
				return nil
			}
			if err := serverCfg.DB.Txn(
				ctx, do, isql.SteppingEnabled(), isql.WithPriority(admissionpb.BulkLowPri),
			); err != nil {
				return spanRowCount, errors.Wrapf(err, "error during row deletion")
			}
			metrics.RowDeletions.Inc(batchRowCount)
			spanRowCount += batchRowCount
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

// SpanToQueryBounds converts the span output of the DistSQL planner to
// QueryBounds to generate SELECT statements.
func SpanToQueryBounds(
	ctx context.Context,
	kvDB *kv.DB,
	codec keys.SQLCodec,
	pkColIDs catalog.TableColMap,
	pkColTypes []*types.T,
	pkColDirs []catenumpb.IndexColumn_Direction,
	numFamilies int,
	span roachpb.Span,
	alloc *tree.DatumAlloc,
) (bounds QueryBounds, hasRows bool, _ error) {
	partialStartKey := span.Key
	partialEndKey := span.EndKey
	startKeyValues, err := kvDB.Scan(ctx, partialStartKey, partialEndKey, int64(numFamilies))
	if err != nil {
		return bounds, false, errors.Wrapf(err, "scan error startKey=%x endKey=%x", []byte(partialStartKey), []byte(partialEndKey))
	}
	// If span has 0 rows then return early - it will not be processed.
	if len(startKeyValues) == 0 {
		return bounds, false, nil
	}
	endKeyValues, err := kvDB.ReverseScan(ctx, partialStartKey, partialEndKey, int64(numFamilies))
	if err != nil {
		return bounds, false, errors.Wrapf(err, "reverse scan error startKey=%x endKey=%x", []byte(partialStartKey), []byte(partialEndKey))
	}
	// If span has 0 rows then return early - it will not be processed. This is
	// checked again here because the calls to Scan and ReverseScan are
	// non-transactional so the row could have been deleted between the calls.
	if len(endKeyValues) == 0 {
		return bounds, false, nil
	}
	bounds.Start, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, startKeyValues, alloc)
	if err != nil {
		return bounds, false, errors.Wrapf(err, "decode startKeyValues error on %+v", startKeyValues)
	}
	bounds.End, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, endKeyValues, alloc)
	if err != nil {
		return bounds, false, errors.Wrapf(err, "decode endKeyValues error on %+v", endKeyValues)
	}
	return bounds, true, nil
}

// GetPKColumnTypes returns tableDesc's primary key column types.
func GetPKColumnTypes(
	tableDesc catalog.TableDescriptor, indexDesc *descpb.IndexDescriptor,
) ([]*types.T, error) {
	pkColTypes := make([]*types.T, 0, len(indexDesc.KeyColumnIDs))
	for i, id := range indexDesc.KeyColumnIDs {
		col, err := catalog.MustFindColumnByID(tableDesc, id)
		if err != nil {
			return nil, errors.Wrapf(err, "column index=%d", i)
		}
		pkColTypes = append(pkColTypes, col.GetType())
	}
	return pkColTypes, nil
}

func init() {
	rowexec.NewTTLProcessor = newTTLProcessor
}
