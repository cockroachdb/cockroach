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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type ttlProcessor struct {
	execinfra.ProcessorBase
	ttlSpec execinfrapb.TTLSpec

	// ttlProcessorOverride allows the job to override fields that would normally
	// come from the DistSQL processor for 22.1 compatibility.
	ttlProcessorOverride *ttlProcessorOverride
}

type ttlProcessorOverride struct {
	descsCol       *descs.Collection
	db             *kv.DB
	codec          keys.SQLCodec
	jobRegistry    *jobs.Registry
	sqlInstanceID  base.SQLInstanceID
	settingsValues *settings.Values
	ie             sqlutil.InternalExecutor
}

func (t *ttlProcessor) getWorkFields() (
	*descs.Collection,
	*kv.DB,
	keys.SQLCodec,
	*jobs.Registry,
	base.SQLInstanceID,
) {
	tpo := t.ttlProcessorOverride
	if tpo != nil {
		return tpo.descsCol, tpo.db, tpo.codec, tpo.jobRegistry, tpo.sqlInstanceID
	}
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	return flowCtx.Descriptors, serverCfg.DB, serverCfg.Codec, serverCfg.JobRegistry, flowCtx.NodeID.SQLInstanceID()
}

func (t *ttlProcessor) getSpanFields() (
	*settings.Values,
	sqlutil.InternalExecutor,
	*kv.DB,
	*descs.Collection,
) {
	tpo := t.ttlProcessorOverride
	if tpo != nil {
		return tpo.settingsValues, tpo.ie, tpo.db, tpo.descsCol
	}
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	return &serverCfg.Settings.SV, serverCfg.Executor, serverCfg.DB, flowCtx.Descriptors
}

func (t *ttlProcessor) Start(ctx context.Context) {
	ctx = t.StartInternal(ctx, "ttl")
	err := t.work(ctx)
	t.MoveToDraining(err)
}

func (t *ttlProcessor) work(ctx context.Context) error {

	ttlSpec := t.ttlSpec
	descsCol, db, codec, jobRegistry, sqlInstanceID := t.getWorkFields()
	details := ttlSpec.RowLevelTTLDetails

	deleteRateLimit := ttlSpec.DeleteRateLimit
	deleteRateLimiter := quotapool.NewRateLimiter(
		"ttl-delete",
		quotapool.Limit(deleteRateLimit),
		deleteRateLimit,
	)

	processorRowCount := int64(0)

	var relationName string
	var pkColumns []string
	var pkTypes []*types.T
	var labelMetrics bool
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

		primaryIndexDesc := desc.GetPrimaryIndex().IndexDesc()
		pkColumns = primaryIndexDesc.KeyColumnNames
		for _, id := range primaryIndexDesc.KeyColumnIDs {
			col, err := desc.FindColumnWithID(id)
			if err != nil {
				return err
			}
			pkTypes = append(pkTypes, col.GetType())
		}

		if !desc.HasRowLevelTTL() {
			return errors.Newf("unable to find TTL on table %s", desc.GetName())
		}

		rowLevelTTL := desc.GetRowLevelTTL()
		labelMetrics = rowLevelTTL.LabelMetrics

		tn, err := descs.GetTableNameByDesc(ctx, txn, descsCol, desc)
		if err != nil {
			return errors.Wrapf(err, "error fetching table relation name for TTL")
		}

		relationName = tn.FQString()
		return nil
	}); err != nil {
		return err
	}

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
	err := func() error {
		spanChan := make(chan spanToProcess, processorConcurrency)
		defer close(spanChan)
		for i := int64(0); i < processorConcurrency; i++ {
			group.GoCtx(func(ctx context.Context) error {
				for spanToProcess := range spanChan {
					start := timeutil.Now()
					spanRowCount, err := t.runTTLOnSpan(
						ctx,
						metrics,
						spanToProcess,
						pkColumns,
						relationName,
						deleteRateLimiter,
					)
					// add before returning err in case of partial success
					atomic.AddInt64(&processorRowCount, spanRowCount)
					metrics.SpanTotalDuration.RecordValue(int64(timeutil.Since(start)))
					if err != nil {
						// Continue until channel is fully read.
						// Otherwise, the keys input will be blocked.
						for spanToProcess = range spanChan {
						}
						return err
					}
				}
				return nil
			})
		}

		// Iterate over every span to feed work for the goroutine processors.
		var alloc tree.DatumAlloc
		for _, span := range ttlSpec.Spans {
			startPK, err := keyToDatums(span.Key, codec, pkTypes, &alloc)
			if err != nil {
				return err
			}
			endPK, err := keyToDatums(span.EndKey, codec, pkTypes, &alloc)
			if err != nil {
				return err
			}
			spanChan <- spanToProcess{
				startPK: startPK,
				endPK:   endPK,
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

	jobID := ttlSpec.JobID
	return jobRegistry.UpdateJobWithTxn(
		ctx,
		jobID,
		nil,  /* txn */
		true, /* useReadLock */
		func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			rowLevelTTL.JobRowCount += processorRowCount
			processorID := t.ProcessorID
			rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, jobspb.RowLevelTTLProcessorProgress{
				ProcessorID:          processorID,
				SQLInstanceID:        sqlInstanceID,
				ProcessorRowCount:    processorRowCount,
				ProcessorSpanCount:   processorSpanCount,
				ProcessorConcurrency: processorConcurrency,
			})
			ju.UpdateProgress(progress)
			log.VInfof(
				ctx,
				2, /* level */
				"TTL processorRowCount updated jobID=%d processorID=%d sqlInstanceID=%d tableID=%d jobRowCount=%d processorRowCount=%d",
				jobID, processorID, sqlInstanceID, details.TableID, rowLevelTTL.JobRowCount, processorRowCount,
			)
			return nil
		},
	)
}

// spanRowCount should be checked even if the function returns an error because it may have partially succeeded
func (t *ttlProcessor) runTTLOnSpan(
	ctx context.Context,
	metrics rowLevelTTLMetrics,
	spanToProcess spanToProcess,
	pkColumns []string,
	relationName string,
	deleteRateLimiter *quotapool.RateLimiter,
) (spanRowCount int64, err error) {
	metrics.NumActiveSpans.Inc(1)
	defer metrics.NumActiveSpans.Dec(1)

	// TODO(#82140): investigate improving row deletion performance with secondary indexes

	ttlSpec := t.ttlSpec
	details := ttlSpec.RowLevelTTLDetails
	tableID := details.TableID
	cutoff := details.Cutoff
	ttlExpr := ttlSpec.TTLExpr
	settingsValues, ie, db, descsCol := t.getSpanFields()

	selectBatchSize := ttlSpec.SelectBatchSize

	aostDuration := ttlSpec.AOSTDuration
	if aostDuration == 0 {
		// Read AOST in case of mixed 22.2.0/22.2.1+ cluster where the job started on a 22.2.0 node.
		//lint:ignore SA1019 execinfrapb.TTLSpec.AOST is deprecated
		aost := ttlSpec.AOST
		if !aost.IsZero() {
			aostDuration = aost.Sub(details.Cutoff)
		}
	}

	selectBuilder := makeSelectQueryBuilder(
		tableID,
		cutoff,
		pkColumns,
		relationName,
		spanToProcess,
		aostDuration,
		selectBatchSize,
		ttlExpr,
	)
	deleteBatchSize := ttlSpec.DeleteBatchSize
	deleteBuilder := makeDeleteQueryBuilder(
		tableID,
		cutoff,
		pkColumns,
		relationName,
		deleteBatchSize,
		ttlExpr,
	)

	preSelectStatement := ttlSpec.PreSelectStatement
	if preSelectStatement != "" {
		if _, err := ie.ExecEx(
			ctx,
			"pre-select-delete-statement",
			nil, /* txn */
			sessiondata.InternalExecutorOverride{
				User: username.RootUserName(),
			},
			preSelectStatement,
		); err != nil {
			return spanRowCount, err
		}
	}

	for {
		// Check the job is enabled on every iteration.
		if err := checkEnabled(settingsValues); err != nil {
			return spanRowCount, err
		}

		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		start := timeutil.Now()
		expiredRowsPKs, err := selectBuilder.run(ctx, ie)
		metrics.SelectDuration.RecordValue(int64(timeutil.Since(start)))
		if err != nil {
			return spanRowCount, errors.Wrapf(err, "error selecting rows to delete")
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
				// If we detected a schema change here, the DELETE will not succeed
				// (the SELECT still will because of the AOST). Early exit here.
				desc, err := descsCol.GetImmutableTableByID(
					ctx,
					txn,
					details.TableID,
					tree.ObjectLookupFlagsWithRequired(),
				)
				if err != nil {
					return err
				}
				if ttlSpec.PreDeleteChangeTableVersion || desc.GetVersion() != details.TableVersion {
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
				if err != nil {
					return err
				}

				metrics.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
				metrics.RowDeletions.Inc(batchRowCount)
				spanRowCount += batchRowCount
				return nil
			}); err != nil {
				return spanRowCount, errors.Wrapf(err, "error during row deletion")
			}
		}

		// Step 3. Early exit if necessary.

		// If we selected less than the select batch size, we have selected every
		// row and so we end it here.
		if numExpiredRows < selectBatchSize {
			break
		}
	}

	return spanRowCount, nil
}

func (t *ttlProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, t.DrainHelper()
}

func newTTLProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.TTLSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ttlProcessor := &ttlProcessor{
		ttlSpec: spec,
	}
	if err := ttlProcessor.Init(
		ttlProcessor,
		&execinfrapb.PostProcessSpec{},
		[]*types.T{},
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return ttlProcessor, nil
}

func init() {
	rowexec.NewTTLProcessor = newTTLProcessor
}
