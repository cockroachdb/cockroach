// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// DirectScansEnabled is a cluster setting that controls whether the KV
// projection pushdown infrastructure can be used.
var DirectScansEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.distsql.direct_columnar_scans.enabled",
	"set to true to enable the 'direct' columnar scans in the KV layer",
	directScansEnabledDefault,
)

var directScansEnabledDefault = util.ConstantWithMetamorphicTestBool(
	"direct-scans-enabled",
	// TODO(yuzefovich, 23.1): update the default to 'true' for multi-tenant
	// setups.
	false,
)

// cFetcherWrapper implements the storage.CFetcherWrapper interface. See a large
// comment in storage/col_mvcc.go for more details.
type cFetcherWrapper struct {
	fetcher *cFetcher
	// sawBatch indicates whether at least one non-zero length batch was read
	// from the fetcher.
	sawBatch bool
	// removeLastRow indicates whether the last row in the last batch returned
	// by the fetcher needs to be removed. This is the case when one of the
	// BatchRequest's limits was reached forcing us to trim the partial row.
	removeLastRow bool

	// adapter is a utility struct needed to wrap a call to fetcher.NextBatch
	// with a panic-catcher.
	adapter struct {
		ctx   context.Context
		batch coldata.Batch
		err   error
	}

	// startKey is only used as an additional detail for some error messages.
	startKey roachpb.Key

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer
	buf        bytes.Buffer
}

var _ storage.CFetcherWrapper = &cFetcherWrapper{}

func init() {
	storage.GetCFetcherWrapper = newCFetcherWrapper
}

func (c *cFetcherWrapper) nextBatchAdapter() {
	c.adapter.batch, c.adapter.err = c.fetcher.NextBatch(c.adapter.ctx)
}

// NextBatch implements the storage.CFetcherWrapper interface.
func (c *cFetcherWrapper) NextBatch(ctx context.Context) ([]byte, error) {
	// cFetcher propagates some errors as "internal" panics, so we have to wrap
	// a call to cFetcher.NextBatch with a panic-catcher.
	c.adapter.ctx = ctx
	if err := colexecerror.CatchVectorizedRuntimeError(c.nextBatchAdapter); err != nil {
		// Most likely this error indicates that a memory limit was reached by
		// the wrapped cFetcher, so we want to augment it with an additional
		// detail about the start key.
		return nil, storage.IncludeStartKeyIntoErr(c.startKey, err)
	}
	if c.adapter.err != nil {
		// If an error is propagated in a "regular" fashion, as a return
		// parameter, then we don't include the start key - the pebble MVCC
		// scanner has already done so if needed.
		return nil, c.adapter.err
	}
	if buildutil.CrdbTestBuild {
		if c.fetcher.machine.mustRemoveLastRow && !c.removeLastRow {
			return nil, errors.AssertionFailedf(
				"unexpectedly set a NULL value in non-nullable column but didn't remove that row",
			)
		}
	}
	batchLength := c.adapter.batch.Length()
	if c.removeLastRow {
		if buildutil.CrdbTestBuild {
			if !c.sawBatch && batchLength == 0 {
				// At the moment, the direct scans always use AllowEmpty=false
				// option of the BatchRequest, so we should never get into a
				// state where we're removing the very first row of the
				// response.
				return nil, errors.AssertionFailedf(
					"unexpectedly zero-length first batch with removeLastRow=true",
				)
			}
		}
		if batchLength > 0 {
			c.adapter.batch.SetLength(batchLength - 1)
			batchLength--
		}
	}
	if batchLength == 0 {
		return nil, nil
	}
	c.sawBatch = true
	data, err := c.converter.BatchToArrow(ctx, c.adapter.batch)
	if err != nil {
		return nil, err
	}
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, batchLength)
	if err != nil {
		return nil, err
	}
	return c.buf.Bytes(), nil
}

// Close implements the storage.CFetcherWrapper interface.
func (c *cFetcherWrapper) Close(ctx context.Context) {
	if c.fetcher != nil {
		c.fetcher.Close(ctx)
		c.fetcher.Release()
		c.fetcher = nil
	}
}

// ContinuesFirstRow implements the storage.CFetcherWrapper interface.
//
// ContinuesFirstRow returns true if the given key belongs to the same SQL row
// as the very first KV pair seen by the wrapped cFetcher (or if none KVs have
// been seen yet). If either key is not a valid SQL row key, returns false.
func (c *cFetcherWrapper) ContinuesFirstRow(key roachpb.Key) bool {
	return !c.sawBatch && // haven't yet returned a batch with at least one row, and
		c.fetcher.machine.rowIdx == 0 && // are populating the first row in the batch, and
		(c.fetcher.machine.state[0] == stateInitFetch || // this is the first KV in the current row, or
			// this KV is part of the current row
			c.fetcher.machine.state[0] == stateFetchNextKVWithUnfinishedRow && !c.fetcher.keyFromNewRow(key))
}

// MaybeTrimPartialLastRow implements the storage.CFetcherWrapper interface.
//
// MaybeTrimPartialLastRow "removes" the last KV pairs from the batch that are
// part of the same SQL row as the given key, returning the earliest key
// removed. Note that the actual removal is delayed until the control flow is
// returned to cFetcherWrapper.NextBatch.
func (c *cFetcherWrapper) MaybeTrimPartialLastRow(nextKey roachpb.Key) (roachpb.Key, error) {
	if c.fetcher.machine.state[0] == stateInitFetch {
		// This is the first KV in the current row, so we don't need to remove
		// the last row from the batch and need to resume the scan from the
		// given key.
		c.removeLastRow = false
		return nextKey, nil
	}
	// We have at least one KV decoded into the current row. Check whether the
	// next key is part of the same row.
	if c.fetcher.keyFromNewRow(nextKey) {
		// The given key is the first KV of the next row, so we don't need to
		// remove anything and will resume from this key.
		c.removeLastRow = false
		return nextKey, nil
	}
	// The given key is part of the current last row, so we need to remove that
	// row and will resume the fetch from the first key in that row.
	c.removeLastRow = true
	return c.fetcher.machine.firstKeyInRow, nil
}

// LastRowHasFinalColumnFamily implements the storage.CFetcherWrapper interface.
//
// LastRowHasFinalColumnFamily returns true if the last key read by the cFetcher
// is the maximum column family ID of the row, i.e. we know that the row is
// complete.
func (c *cFetcherWrapper) LastRowHasFinalColumnFamily(reverse bool) bool {
	// This method is called after the KV has been put into
	// storage.singleResults but before storage.NextKVer.NextKV() returned to
	// cFetcher.
	key, _, ok := enginepb.SplitMVCCKey(c.fetcher.nextKVer.GetLastEncodedKey())
	if !ok {
		return false
	}
	colFamilyID, err := keys.DecodeFamilyKey(key)
	if err != nil {
		return false
	}
	if reverse {
		return colFamilyID == 0
	}
	return descpb.FamilyID(colFamilyID) == c.fetcher.table.spec.MaxFamilyID
}

func newCFetcherWrapper(
	ctx context.Context,
	fetcherAccount *mon.BoundAccount,
	converterAccount *mon.BoundAccount,
	fetchSpec *fetchpb.IndexFetchSpec,
	nextKVer storage.NextKVer,
	startKey roachpb.Key,
) (_ storage.CFetcherWrapper, retErr error) {
	// At the moment, we always serialize the columnar batches, so it is safe to
	// handle enum types without proper hydration - we just treat them as bytes
	// values, and it is the responsibility of the ColBatchDirectScan to hydrate
	// the type correctly when deserializing the batches.
	const allowUnhydratedEnums = true
	tableArgs, err := populateTableArgs(ctx, fetchSpec, nil /* typeResolver */, allowUnhydratedEnums)
	if err != nil {
		return nil, err
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	defer func() {
		if retErr != nil {
			fetcher.Release()
		}
	}()
	// This memory limit determines the maximum footprint of a single batch
	// produced by the cFetcher. The main limiting behavior is actually driven
	// by the pebbleMVCCScanner (which respects TargetSize and
	// MaxSpanRequestKeys limits of the BatchRequest), so we just have a
	// reasonable default here.
	const memoryLimit = execinfra.DefaultMemoryLimit
	// We will allow a NULL value in a non-nullable column if we have multiple
	// column families because we need to have an ability to trim the last
	// partial row without hitting an error.
	allowNullsInNonNullableOnLastRowInBatch := fetchSpec.MaxKeysPerRow > 1
	// TODO(yuzefovich, 23.1): think through estimatedRowCount (#94850) and
	// traceKV arguments.
	fetcher.cFetcherArgs = cFetcherArgs{
		memoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
		allowNullsInNonNullableOnLastRowInBatch,
	}

	// We don't need to provide the eval context here since we will only decode
	// bytes into datums and then serialize them, without ever comparing datums
	// (at least until we implement the filter pushdown).
	allocator := colmem.NewAllocator(ctx, fetcherAccount, coldataext.NewExtendedColumnFactoryNoEvalCtx())
	if err = fetcher.Init(allocator, nextKVer, tableArgs); err != nil {
		return nil, err
	}
	wrapper := cFetcherWrapper{
		fetcher:  fetcher,
		startKey: startKey,
	}
	wrapper.converter, err = colserde.NewArrowBatchConverter(tableArgs.typs, colserde.BatchToArrowOnly, converterAccount)
	if err != nil {
		return nil, err
	}
	wrapper.serializer, err = colserde.NewRecordBatchSerializer(tableArgs.typs)
	if err != nil {
		return nil, err
	}
	return &wrapper, nil
}
