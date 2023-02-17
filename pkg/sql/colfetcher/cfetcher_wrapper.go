// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	// adapter is a utility struct needed to wrap a call to fetcher.NextBatch
	// with a panic-catcher.
	adapter struct {
		ctx   context.Context
		batch coldata.Batch
		err   error
	}

	// startKey is only used as an additional detail for some error messages.
	startKey roachpb.Key

	converterAcc *mon.BoundAccount
	// TODO(yuzefovich): consider extracting a serializer component that would
	// be also reused by the colrpc.Outbox.
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
	if c.adapter.batch.Length() == 0 {
		return nil, nil
	}
	data, err := c.converter.BatchToArrow(ctx, c.adapter.batch)
	if err != nil {
		return nil, err
	}
	oldBufCap := c.buf.Cap()
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, c.adapter.batch.Length())
	if err != nil {
		return nil, err
	}
	if newBufCap := c.buf.Cap(); newBufCap > oldBufCap {
		// Account for the capacity of the buffer since we're reusing it across
		// NextBatch calls. Note that it is ok to reuse the memory account of
		// the ArrowBatchConverter for this since the converter only grows /
		// shrinks the account according to its own usage and never relies on
		// the total used value.
		if err = c.converterAcc.Grow(ctx, int64(newBufCap-oldBufCap)); err != nil {
			return nil, err
		}
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
	if c.converter != nil {
		c.converter.Release(ctx)
		c.converter = nil
	}
	c.buf = bytes.Buffer{}
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
	// TODO(yuzefovich, 23.1): think through estimatedRowCount (#94850) and
	// traceKV arguments.
	fetcher.cFetcherArgs = cFetcherArgs{
		memoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
		false, /* collectStats */
	}

	// We don't need to provide the eval context here since we will only decode
	// bytes into datums and then serialize them, without ever comparing datums
	// (at least until we implement the filter pushdown).
	allocator := colmem.NewAllocator(ctx, fetcherAccount, coldataext.NewExtendedColumnFactoryNoEvalCtx())
	if err = fetcher.Init(allocator, nextKVer, tableArgs); err != nil {
		return nil, err
	}
	// TODO(yuzefovich, 23.1): consider pooling the allocations of some objects.
	wrapper := cFetcherWrapper{
		fetcher:      fetcher,
		startKey:     startKey,
		converterAcc: converterAccount,
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
