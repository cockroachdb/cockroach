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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
}

func (ttl *ttlProcessor) Start(ctx context.Context) {
	ctx = ttl.StartInternal(ctx, "ttl")
	err := ttl.work(ctx)
	ttl.MoveToDraining(err)
}

func (ttl *ttlProcessor) work(ctx context.Context) error {

	ttlSpec := ttl.ttlSpec
	flowCtx := ttl.FlowCtx
	descsCol := flowCtx.Descriptors
	serverCfg := flowCtx.Cfg
	db := serverCfg.DB
	codec := serverCfg.Codec
	details := ttlSpec.RowLevelTTLDetails
	ttlExpr := ttlSpec.TTLExpr
	rangeConcurrency := ttlSpec.RangeConcurrency
	selectBatchSize := ttlSpec.SelectBatchSize
	deleteBatchSize := ttlSpec.DeleteBatchSize

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
					rangeRowCount, err := runTTLOnRange(
						ctx,
						details,
						db,
						serverCfg.Executor,
						serverCfg.Settings,
						descsCol,
						metrics,
						rangeToProcess,
						pkColumns,
						relationName,
						selectBatchSize,
						deleteBatchSize,
						deleteRateLimiter,
						ttlSpec.AOST,
						ttlExpr,
						ttlSpec.PreDeleteChangeTableVersion,
						ttlSpec.PreSelectStatement,
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
		ri := kvcoord.MakeRangeIterator(serverCfg.DistSender)
		for _, span := range ttlSpec.Spans {
			rangeSpan := span
			ri.Seek(ctx, roachpb.RKey(span.Key), kvcoord.Ascending)
			for done := false; ri.Valid() && !done; ri.Next(ctx) {
				// Send range info to each goroutine worker.
				rangeDesc := ri.Desc()
				var nextRange rangeToProcess
				// A single range can contain multiple tables or indexes.
				// If this is the case, the rangeDesc.StartKey would be less than span.Key
				// or the rangeDesc.EndKey would be greater than the span.EndKey, meaning
				// the range contains the start or the end of the range respectively.
				// Trying to decode keys outside the PK range will lead to a decoding error.
				// As such, only populate nextRange.startPK and nextRange.endPK if this is the case
				// (by default, a 0 element startPK or endPK means the beginning or end).
				if rangeDesc.StartKey.AsRawKey().Compare(span.Key) > 0 {
					var err error
					nextRange.startPK, err = keyToDatums(rangeDesc.StartKey, codec, pkTypes, &alloc)
					if err != nil {
						return errors.Wrapf(
							err,
							"error decoding starting PRIMARY KEY for range ID %d (start key %x, table start key %x)",
							rangeDesc.RangeID,
							rangeDesc.StartKey.AsRawKey(),
							span.Key,
						)
					}
				}
				if rangeDesc.EndKey.AsRawKey().Compare(span.EndKey) < 0 {
					rangeSpan.Key = rangeDesc.EndKey.AsRawKey()
					var err error
					nextRange.endPK, err = keyToDatums(rangeDesc.EndKey, codec, pkTypes, &alloc)
					if err != nil {
						return errors.Wrapf(
							err,
							"error decoding ending PRIMARY KEY for range ID %d (end key %x, table end key %x)",
							rangeDesc.RangeID,
							rangeDesc.EndKey.AsRawKey(),
							span.EndKey,
						)
					}
				} else {
					done = true
				}
				rangeChan <- nextRange
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
			existingRowCount := progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL.RowCount
			progress.Details.(*jobspb.Progress_RowLevelTTL).RowLevelTTL.RowCount += processorRowCount
			ju.UpdateProgress(progress)
			log.VInfof(
				ctx,
				2, /* level */
				"TTL processorRowCount updated jobID=%d processorID=%d tableID=%d existingRowCount=%d processorRowCount=%d progress=%s",
				jobID, ttl.ProcessorID, details.TableID, existingRowCount, processorRowCount, progress,
			)
			return nil
		},
	)
}

// rangeRowCount should be checked even if the function returns an error because it may have partially succeeded
func runTTLOnRange(
	ctx context.Context,
	details jobspb.RowLevelTTLDetails,
	db *kv.DB,
	ie sqlutil.InternalExecutor,
	settings *cluster.Settings,
	descsCol *descs.Collection,
	metrics rowLevelTTLMetrics,
	rangeToProcess rangeToProcess,
	pkColumns []string,
	relationName string,
	selectBatchSize, deleteBatchSize int64,
	deleteRateLimiter *quotapool.RateLimiter,
	aost time.Time,
	ttlExpr catpb.Expression,
	preDeleteChangeTableVersion bool,
	preSelectStatement string,
) (rangeRowCount int64, err error) {
	metrics.NumActiveRanges.Inc(1)
	defer metrics.NumActiveRanges.Dec(1)

	// TODO(#76914): look at using a dist sql flow job, utilize any existing index
	// on crdb_internal_expiration.

	tableID := details.TableID
	cutoff := details.Cutoff
	selectBuilder := makeSelectQueryBuilder(
		tableID,
		cutoff,
		pkColumns,
		relationName,
		rangeToProcess,
		aost,
		selectBatchSize,
		ttlExpr,
	)
	deleteBuilder := makeDeleteQueryBuilder(
		tableID,
		cutoff,
		pkColumns,
		relationName,
		deleteBatchSize,
		ttlExpr,
	)

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
		if err := checkEnabled(&settings.SV); err != nil {
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
				desc, err := descsCol.GetImmutableTableByID(
					ctx,
					txn,
					details.TableID,
					tree.ObjectLookupFlagsWithRequired(),
				)
				if err != nil {
					return err
				}
				if preDeleteChangeTableVersion || desc.GetVersion() != details.TableVersion {
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
				rangeRowCount += int64(batchRowCount)
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

func (ttl *ttlProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return nil, ttl.DrainHelper()
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
