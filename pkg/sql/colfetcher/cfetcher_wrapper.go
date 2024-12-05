// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colfetcher

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// DirectScansEnabled is a cluster setting that controls whether the KV
// projection pushdown infrastructure can be used.
var DirectScansEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.distsql.direct_columnar_scans.enabled",
	"set to true to enable the 'direct' columnar scans in the KV layer",
	directScansEnabledDefault,
)

var directScansEnabledDefault = metamorphic.ConstantWithTestBool(
	"direct-scans-enabled",
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

	acc                *mon.BoundAccount
	detachedFetcherAcc *mon.BoundAccount
	detachedFetcherMon *mon.BytesMonitor

	// startKey is only used as an additional detail for some error messages.
	startKey roachpb.Key

	// serialize indicates whether the batches must be serialized.
	serialize bool

	// Fields below are only used when serializing the batches.
	// TODO(yuzefovich): consider extracting a serializer component that would
	// be also reused by the colrpc.Outbox.
	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer
	buf        bytes.Buffer
}

var _ storage.CFetcherWrapper = &cFetcherWrapper{}

func init() {
	storage.GetCFetcherWrapper = newCFetcherWrapper
	kvpb.DeserializeColumnarBatchesFromArrow = deserializeColumnarBatchesFromArrow
}

func (c *cFetcherWrapper) nextBatchAdapter() {
	c.adapter.batch, c.adapter.err = c.fetcher.NextBatch(c.adapter.ctx)
}

// NextBatch implements the storage.CFetcherWrapper interface.
func (c *cFetcherWrapper) NextBatch(ctx context.Context) (_ []byte, _ coldata.Batch, retErr error) {
	// By default, we always include the start key into the error to provide
	// more context.
	includeStartKeyIntoErr := true
	defer func() {
		if retErr != nil && includeStartKeyIntoErr {
			retErr = storage.IncludeStartKeyIntoErr(c.startKey, retErr)
		}
	}()
	prevBatchMemUsage := c.detachedFetcherAcc.Used()
	// cFetcher propagates some errors as "internal" panics, so we have to wrap
	// a call to cFetcher.NextBatch with a panic-catcher.
	c.adapter.ctx = ctx
	if err := colexecerror.CatchVectorizedRuntimeError(c.nextBatchAdapter); err != nil {
		return nil, nil, err
	}
	if c.adapter.err != nil {
		// If an error is propagated in a "regular" fashion, as a return
		// parameter, then we don't include the start key - the pebble MVCC
		// scanner has already done so if needed.
		includeStartKeyIntoErr = false
		return nil, nil, c.adapter.err
	}
	if c.adapter.batch.Length() == 0 {
		return nil, nil, nil
	}
	if !c.serialize {
		// Perform the accounting for this batch. Note that when we're not
		// serializing the response, the cFetcher always allocates a new batch,
		// so we always grow the account by the footprint of the batch.
		if err := c.acc.Grow(ctx, c.detachedFetcherAcc.Used()); err != nil {
			return nil, nil, err
		}
		return nil, c.adapter.batch, nil
	}
	// Update the memory account based on possibly changed footprint of the
	// batch (which the cFetcher reuses).
	if err := c.acc.Resize(ctx, prevBatchMemUsage, c.detachedFetcherAcc.Used()); err != nil {
		return nil, nil, err
	}
	data, err := c.converter.BatchToArrow(ctx, c.adapter.batch)
	if err != nil {
		return nil, nil, err
	}
	oldBufCap := c.buf.Cap()
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, c.adapter.batch.Length())
	if err != nil {
		return nil, nil, err
	}
	if newBufCap := c.buf.Cap(); newBufCap > oldBufCap {
		// Account for the capacity of the buffer since we're reusing it across
		// NextBatch calls.
		if err = c.acc.Grow(ctx, int64(newBufCap-oldBufCap)); err != nil {
			return nil, nil, err
		}
	}
	// Since we reuse the buffer across NextBatch calls, make a copy after
	// performing the memory accounting for it.
	serializedBatch := c.buf.Bytes()
	if err = c.acc.Grow(ctx, int64(len(serializedBatch))); err != nil {
		return nil, nil, err
	}
	b := make([]byte, len(serializedBatch))
	copy(b, serializedBatch)
	return b, nil, nil
}

// Close implements the storage.CFetcherWrapper interface.
func (c *cFetcherWrapper) Close(ctx context.Context) {
	if c.fetcher != nil {
		c.fetcher.Close(ctx)
		c.fetcher.Release()
		c.fetcher = nil
	}
	if c.detachedFetcherMon != nil {
		c.detachedFetcherAcc.Close(ctx)
		c.detachedFetcherAcc = nil
		c.detachedFetcherMon.Stop(ctx)
		c.detachedFetcherMon = nil
	}
	if c.converter != nil {
		c.converter.Close(ctx)
		c.converter = nil
	}
	c.buf = bytes.Buffer{}
}

func newCFetcherWrapper(
	ctx context.Context,
	st *cluster.Settings,
	acc *mon.BoundAccount,
	fetchSpec *fetchpb.IndexFetchSpec,
	nextKVer storage.NextKVer,
	startKey roachpb.Key,
	mustSerialize bool,
) (_ storage.CFetcherWrapper, retErr error) {
	// At the moment, we always serialize the columnar batches if they contain
	// enums, so it is safe to handle enum types without proper hydration - we
	// just treat them as bytes values, and it is the responsibility of the
	// ColBatchDirectScan to hydrate the type correctly when deserializing the
	// batches.
	const allowUnhydratedEnums = true
	tableArgs, err := populateTableArgs(ctx, fetchSpec, nil /* typeResolver */, allowUnhydratedEnums)
	if err != nil {
		return nil, err
	}
	if buildutil.CrdbTestBuild {
		for _, t := range tableArgs.typs {
			if t.UserDefined() && t.Family() != types.EnumFamily {
				return nil, errors.AssertionFailedf("non-enum UDTs are unsupported")
			}
		}
	}
	if !mustSerialize {
		// Check whether we have an enum return type in which case we still must
		// serialize the response.
		for _, t := range tableArgs.typs {
			if t.Family() == types.EnumFamily {
				mustSerialize = true
				break
			}
		}
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
	// Since we're using the cFetcher on the KV server side, we don't collect
	// any statistics on it (these stats are about the SQL layer).
	const collectStats = false
	// We cannot reuse batches if we're not serializing the response.
	alwaysReallocate := !mustSerialize
	// TODO(yuzefovich, 23.1): think through estimatedRowCount (#94850) and
	// traceKV arguments.
	fetcher.cFetcherArgs = cFetcherArgs{
		memoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
		collectStats,
		alwaysReallocate,
	}

	// This memory monitor is not connected to the memory accounting system
	// since it's only used by the cFetcher to track the size of its batch, and
	// the cFetcherWrapper is responsible for performing the correct accounting
	// against the memory account provided by the caller.
	detachedFetcherMon := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("cfetcher-wrapper-detached-monitor"),
		Settings: st,
	})
	detachedFetcherMon.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
	detachedFetcherAcc := detachedFetcherMon.MakeBoundAccount()

	// We don't need to provide the eval context here since we will only decode
	// bytes into datums and then serialize them, without ever comparing datums
	// (at least until we implement the filter pushdown).
	allocator := colmem.NewAllocator(ctx, &detachedFetcherAcc, coldataext.NewExtendedColumnFactoryNoEvalCtx())
	if err = fetcher.Init(allocator, nextKVer, tableArgs); err != nil {
		return nil, err
	}
	// TODO(yuzefovich, 23.1): consider pooling the allocations of some objects.
	wrapper := cFetcherWrapper{
		fetcher:            fetcher,
		startKey:           startKey,
		serialize:          mustSerialize,
		acc:                acc,
		detachedFetcherAcc: &detachedFetcherAcc,
		detachedFetcherMon: detachedFetcherMon,
	}
	if mustSerialize {
		// Note that it is ok to use the same memory account for the
		// ArrowBatchConverter as for everything else since the converter only
		// grows / shrinks the account according to its own usage and never
		// relies on the total used value.
		wrapper.converter, err = colserde.NewArrowBatchConverter(tableArgs.typs, colserde.BatchToArrowOnly, acc)
		if err != nil {
			return nil, err
		}
		wrapper.serializer, err = colserde.NewRecordBatchSerializer(tableArgs.typs)
		if err != nil {
			return nil, err
		}
	}
	return &wrapper, nil
}

func deserializeColumnarBatchesFromArrow(
	ctx context.Context, serializedColBatches [][]byte, req *kvpb.BatchRequest,
) ([]coldata.Batch, error) {
	allocator := colmem.NewAllocator(
		ctx,
		// This allocator is not connected to the memory accounting system since
		// the accounting for these batches will be done by the SQL client, so
		// we pass a standalone unlimited account here.
		mon.NewStandaloneUnlimitedAccount(), /* unlimitedAcc */
		// It'll be the responsibility of the SQL client to update the
		// datum-backed vectors with the eval context, so we use the factory
		// with no eval context.
		coldataext.NewExtendedColumnFactoryNoEvalCtx(),
	)

	// At the moment, we always serialize the columnar batches if they contain
	// enums, so it is safe to handle enum types without proper hydration - we
	// just treat them as bytes values, and it is the responsibility of the SQL
	// client to hydrate the types correctly when deserializing the batches.
	const allowUnhydratedEnums = true
	tableArgs, err := populateTableArgs(ctx, req.IndexFetchSpec, nil /* typeResolver */, allowUnhydratedEnums)
	if err != nil {
		return nil, err
	}

	result := make([]coldata.Batch, 0, len(serializedColBatches))
	var d colexecutils.Deserializer
	if err = d.Init(allocator, tableArgs.typs, true /* alwaysReallocate */); err != nil {
		return nil, err
	}
	defer d.Close(ctx)
	if err = colexecerror.CatchVectorizedRuntimeError(func() {
		for _, serializedBatch := range serializedColBatches {
			result = append(result, d.Deserialize(serializedBatch))
		}
	}); err != nil {
		return nil, err
	}
	return result, nil
}
