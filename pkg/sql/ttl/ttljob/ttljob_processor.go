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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func (t rowLevelTTLResumer) work(
	ctx context.Context,
	db *kv.DB,
	rangeConcurrency int64,
	execCfg *sql.ExecutorConfig,
	details jobspb.RowLevelTTLDetails,
	descsCol *descs.Collection,
	knobs sql.TTLTestingKnobs,
	tableVersion descpb.DescriptorVersion,
	selectBatchSize int64,
	deleteBatchSize int64,
	deleteRateLimit int64,
	aost time.Time,
	ttlExpr catpb.Expression,
) (int64, error) {

	deleteRateLimiter := quotapool.NewRateLimiter(
		"ttl-delete",
		quotapool.Limit(deleteRateLimit),
		deleteRateLimit,
	)

	rowCount := int64(0)

	var relationName string
	var pkColumns []string
	var pkTypes []*types.T
	var rangeSpan, entirePKSpan roachpb.Span
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
		entirePKSpan = desc.PrimaryIndexSpan(execCfg.Codec)
		rangeSpan = entirePKSpan
		return nil
	}); err != nil {
		return rowCount, err
	}

	metrics := execCfg.JobRegistry.MetricsStruct().RowLevelTTL.(*RowLevelTTLAggMetrics).loadMetrics(
		labelMetrics,
		relationName,
	)

	group := ctxgroup.WithContext(ctx)
	if err := func() error {
		rangeChan := make(chan rangeToProcess, rangeConcurrency)
		defer close(rangeChan)
		for i := int64(0); i < rangeConcurrency; i++ {
			group.GoCtx(func(ctx context.Context) error {
				for r := range rangeChan {
					start := timeutil.Now()
					rangeRowCount, err := runTTLOnRange(
						ctx,
						execCfg,
						details,
						descsCol,
						knobs,
						metrics,
						tableVersion,
						r.startPK,
						r.endPK,
						pkColumns,
						relationName,
						selectBatchSize,
						deleteBatchSize,
						deleteRateLimiter,
						aost,
						ttlExpr,
					)
					// add before returning err in case of partial success
					atomic.AddInt64(&rowCount, rangeRowCount)
					metrics.RangeTotalDuration.RecordValue(int64(timeutil.Since(start)))
					if err != nil {
						// Continue until channel is fully read.
						// Otherwise, the keys input will be blocked.
						for r = range rangeChan {
						}
						return err
					}
				}
				return nil
			})
		}

		// Iterate over every range to feed work for the goroutine processors.
		var alloc tree.DatumAlloc
		ri := kvcoord.MakeRangeIterator(execCfg.DistSender)
		ri.Seek(ctx, roachpb.RKey(entirePKSpan.Key), kvcoord.Ascending)
		for done := false; ri.Valid() && !done; ri.Next(ctx) {
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
				var err error
				nextRange.startPK, err = keyToDatums(rangeDesc.StartKey, execCfg.Codec, pkTypes, &alloc)
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
				var err error
				nextRange.endPK, err = keyToDatums(rangeDesc.EndKey, execCfg.Codec, pkTypes, &alloc)
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
			rangeChan <- nextRange
		}
		return nil
	}(); err != nil {
		return rowCount, err
	}

	return rowCount, group.Wait()
}

// rangeRowCount should be checked even if the function returns an error because it may have partially succeeded
func runTTLOnRange(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	details jobspb.RowLevelTTLDetails,
	descsCol *descs.Collection,
	knobs sql.TTLTestingKnobs,
	metrics rowLevelTTLMetrics,
	tableVersion descpb.DescriptorVersion,
	startPK tree.Datums,
	endPK tree.Datums,
	pkColumns []string,
	relationName string,
	selectBatchSize, deleteBatchSize int64,
	deleteRateLimiter *quotapool.RateLimiter,
	aost time.Time,
	ttlExpr catpb.Expression,
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
		ttlExpr,
	)
	deleteBuilder := makeDeleteQueryBuilder(
		details.TableID,
		details.Cutoff,
		pkColumns,
		relationName,
		deleteBatchSize,
		ttlExpr,
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
				desc, err := descsCol.GetImmutableTableByID(
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
