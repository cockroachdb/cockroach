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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
}

func (t *ttlProcessor) Start(ctx context.Context) {
	ctx = t.StartInternal(ctx, "ttl")
	err := t.work(ctx)
	t.MoveToDraining(err)
}

func (t *ttlProcessor) work(ctx context.Context) error {

	ttlSpec := t.ttlSpec
	flowCtx := t.FlowCtx
	descsCol := flowCtx.Descriptors
	serverCfg := flowCtx.Cfg
	db := serverCfg.DB
	codec := serverCfg.Codec
	details := ttlSpec.RowLevelTTLDetails
	rangeConcurrency := ttlSpec.RangeConcurrency

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

	metrics := serverCfg.JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
		labelMetrics,
		relationName,
	)

	group := ctxgroup.WithContext(ctx)
	err := func() error {
		rangeChan := make(chan rangeToProcess, rangeConcurrency)
		defer close(rangeChan)
		for i := int64(0); i < rangeConcurrency; i++ {
			group.GoCtx(func(ctx context.Context) error {
				for rangeToProcess := range rangeChan {
					start := timeutil.Now()
					rangeRowCount, err := t.runTTLOnRange(
						ctx,
						metrics,
						rangeToProcess,
						pkColumns,
						relationName,
						deleteRateLimiter,
					)
					// add before returning err in case of partial success
					atomic.AddInt64(&processorRowCount, rangeRowCount)
					metrics.RangeTotalDuration.RecordValue(int64(timeutil.Since(start)))
					if err != nil {
						// Continue until channel is fully read.
						// Otherwise, the keys input will be blocked.
						for rangeToProcess = range rangeChan {
						}
						return err
					}
				}
				return nil
			})
		}

		// Iterate over every range to feed work for the goroutine processors.
		var alloc tree.DatumAlloc
		for _, span := range ttlSpec.Spans {
			startPK, err := keyToDatums(roachpb.RKey(span.Key), codec, pkTypes, &alloc)
			if err != nil {
				return err
			}
			endPK, err := keyToDatums(roachpb.RKey(span.EndKey), codec, pkTypes, &alloc)
			if err != nil {
				return err
			}
			rangeChan <- rangeToProcess{
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
	return serverCfg.JobRegistry.UpdateJobWithTxn(
		ctx,
		jobID,
		nil,  /* txn */
		true, /* useReadLock */
		func(_ *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			rowLevelTTL := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL
			rowLevelTTL.JobRowCount += processorRowCount
			processorID := t.ProcessorID
			sqlInstanceID := flowCtx.NodeID.SQLInstanceID()
			rowLevelTTL.ProcessorProgresses = append(rowLevelTTL.ProcessorProgresses, jobspb.RowLevelTTLProcessorProgress{
				ProcessorID:       processorID,
				SQLInstanceID:     sqlInstanceID,
				ProcessorRowCount: processorRowCount,
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

// rangeRowCount should be checked even if the function returns an error because it may have partially succeeded
func (t *ttlProcessor) runTTLOnRange(
	ctx context.Context,
	metrics rowLevelTTLMetrics,
	rangeToProcess rangeToProcess,
	pkColumns []string,
	relationName string,
	deleteRateLimiter *quotapool.RateLimiter,
) (rangeRowCount int64, err error) {
	metrics.NumActiveRanges.Inc(1)
	defer metrics.NumActiveRanges.Dec(1)

	// TODO(#82140): investigate improving row deletion performance with secondary indexes

	ttlSpec := t.ttlSpec
	details := ttlSpec.RowLevelTTLDetails
	tableID := details.TableID
	cutoff := details.Cutoff
	ttlExpr := ttlSpec.TTLExpr
	flowCtx := t.FlowCtx
	serverCfg := flowCtx.Cfg
	ie := serverCfg.Executor

	selectBatchSize := ttlSpec.SelectBatchSize
	selectBuilder := makeSelectQueryBuilder(
		tableID,
		cutoff,
		pkColumns,
		relationName,
		rangeToProcess,
		ttlSpec.AOST,
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
			return rangeRowCount, err
		}
	}

	for {
		// Check the job is enabled on every iteration.
		if err := checkEnabled(&serverCfg.Settings.SV); err != nil {
			return rangeRowCount, err
		}

		// Step 1. Fetch some rows we want to delete using a historical
		// SELECT query.
		start := timeutil.Now()
		expiredRowsPKs, err := selectBuilder.run(ctx, ie)
		metrics.SelectDuration.RecordValue(int64(timeutil.Since(start)))
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
			if err := serverCfg.DB.TxnWithSteppingEnabled(ctx, sessiondatapb.TTLLow, func(ctx context.Context, txn *kv.Txn) error {
				// If we detected a schema change here, the DELETE will not succeed
				// (the SELECT still will because of the AOST). Early exit here.
				desc, err := flowCtx.Descriptors.GetImmutableTableByID(
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
				rangeRowCount += batchRowCount
				return nil
			}); err != nil {
				return rangeRowCount, errors.Wrapf(err, "error during row deletion")
			}
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

	// Decode the datums ourselves, instead of using rowenc.DecodeKeyVals.
	// We cannot use rowenc.DecodeKeyVals because we may not have the entire PK
	// as the key for the range (e.g. a PK (a, b) may only be split on (a)).
	rKey, err := codec.StripTenantPrefix(rKey)
	if err != nil {
		return nil, errors.Wrapf(err, "error decoding tenant prefix of %x", key)
	}
	rKey, _, _, err = rowenc.DecodePartialTableIDIndexID(rKey)
	if err != nil {
		return nil, errors.Wrapf(err, "error decoding table/index ID of key=%x", key)
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
