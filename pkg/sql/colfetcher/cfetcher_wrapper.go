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

	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/colserde"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

type cFetcherWrapper struct {
	fetcher       *cFetcher
	sawBatch      bool
	removeLastRow bool

	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer
	buf        bytes.Buffer
}

var _ storage.CFetcherWrapper = &cFetcherWrapper{}

func init() {
	storage.GetCFetcherWrapper = newCFetcherWrapper
}

func (c *cFetcherWrapper) NextBatch(ctx context.Context) ([]byte, error) {
	batch, err := c.fetcher.NextBatch(ctx)
	if err != nil {
		return nil, err
	}
	if l := batch.Length(); c.removeLastRow && l > 0 {
		batch.SetLength(l - 1)
	}
	if batch.Length() == 0 {
		return nil, nil
	}
	c.sawBatch = true
	data, err := c.converter.BatchToArrow(batch)
	if err != nil {
		return nil, err
	}
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, batch.Length())
	if err != nil {
		return nil, err
	}
	return c.buf.Bytes(), nil
}

// ContinuesFirstRow returns true if the given key belongs to the same SQL row
// as the first KV pair in the result (or if the result is empty). If either
// key is not a valid SQL row key, returns false.
func (c *cFetcherWrapper) ContinuesFirstRow(key roachpb.Key) bool {
	return !c.sawBatch && // haven't yet returned a batch with at least one row, and
		c.fetcher.machine.rowIdx == 0 && // are populating the first row in the batch, and
		(c.fetcher.machine.state[0] == stateInitFetch || // this is the first KV in the current row, or
			// this KV is part of the current row
			c.fetcher.machine.state[0] == stateFetchNextKVWithUnfinishedRow && !c.fetcher.keyFromNewRow(key))
}

// MaybeTrimPartialLastRow removes the last KV pairs from the result that are
// part of the same SQL row as the given key, returning the earliest key
// removed.
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

// LastRowHasFinalColumnFamily returns true if the last key in the result is the
// maximum column family ID of the row, i.e. we know that the row is complete.
func (c *cFetcherWrapper) LastRowHasFinalColumnFamily(reverse bool) bool {
	// This method is called after the KV has been put into singleResults but
	// before NextKV() returned to cFetcher, so we have to peek into the key via
	// the "key provider".
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

func (c *cFetcherWrapper) Close(ctx context.Context) {
	if c.fetcher != nil {
		c.fetcher.Close(ctx)
		c.fetcher.Release()
		c.fetcher = nil
	}
}

func newCFetcherWrapper(
	ctx context.Context,
	acc *mon.BoundAccount,
	fetchSpec *fetchpb.IndexFetchSpec,
	nextKVer storage.NextKVer,
) (_ storage.CFetcherWrapper, retErr error) {
	// TODO: typeResolver.
	tableArgs, err := populateTableArgs(ctx, fetchSpec, nil /* typeResolver */, true /* allowUnhydratedEnums */)
	if err != nil {
		return nil, err
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	defer func() {
		if retErr != nil {
			fetcher.Release()
		}
	}()
	allowNullsInNonNullableOnLastRowInBatch := fetchSpec.MaxKeysPerRow > 1
	// TODO: args.
	fetcher.cFetcherArgs = cFetcherArgs{
		execinfra.DefaultMemoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
		false, /* allocateFreshBatches */
		allowNullsInNonNullableOnLastRowInBatch,
	}

	// We don't need to provide the eval context here since we will only decode
	// bytes into datums and then serialize them, without ever comparing datums
	// (at least until we implement the filter pushdown).
	allocator := colmem.NewAllocator(ctx, acc, coldataext.NewExtendedColumnFactoryNoEvalCtx())
	if err = fetcher.Init(allocator, nextKVer, tableArgs); err != nil {
		return nil, err
	}
	wrapper := cFetcherWrapper{}
	wrapper.fetcher = fetcher
	wrapper.converter, err = colserde.NewArrowBatchConverter(tableArgs.typs)
	if err != nil {
		return nil, err
	}
	wrapper.serializer, err = colserde.NewRecordBatchSerializer(tableArgs.typs)
	if err != nil {
		return nil, err
	}
	return &wrapper, nil
}
