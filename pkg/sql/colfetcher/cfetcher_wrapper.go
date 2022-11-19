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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

type cFetcherWrapper struct {
	fetcher    *cFetcher
	converter  *colserde.ArrowBatchConverter
	serializer *colserde.RecordBatchSerializer
	buf        bytes.Buffer
}

var _ storage.CFetcherWrapper = &cFetcherWrapper{}

func init() {
	storage.GetCFetcherWrapper = newCFetcherWrapper
}

func (c *cFetcherWrapper) NextBatch(
	ctx context.Context, serialize bool,
) ([]byte, coldata.Batch, error) {
	batch, err := c.fetcher.NextBatch(ctx)
	if err != nil {
		return nil, nil, err
	}
	if batch.Length() == 0 {
		return nil, nil, nil
	}
	if !serialize {
		return nil, batch, nil
	}
	data, err := c.converter.BatchToArrow(batch)
	if err != nil {
		return nil, nil, err
	}
	c.buf.Reset()
	_, _, err = c.serializer.Serialize(&c.buf, data, batch.Length())
	if err != nil {
		return nil, nil, err
	}
	return c.buf.Bytes(), nil, nil
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
	indexFetchSpec proto.Message,
	nextKVer storage.NextKVer,
) (_ storage.CFetcherWrapper, retErr error) {
	fetchSpec, ok := indexFetchSpec.(*descpb.IndexFetchSpec)
	if !ok {
		return nil, errors.AssertionFailedf("expected an IndexFetchSpec, but found a %T", indexFetchSpec)
	}
	// TODO: typeResolver.
	tableArgs, err := populateTableArgs(ctx, fetchSpec, nil /* typeResolver */)
	if err != nil {
		return nil, err
	}

	fetcher := cFetcherPool.Get().(*cFetcher)
	defer func() {
		if retErr != nil {
			fetcher.Release()
		}
	}()
	// TODO: args.
	fetcher.cFetcherArgs = cFetcherArgs{
		execinfra.DefaultMemoryLimit,
		0,     /* estimatedRowCount */
		false, /* traceKV */
		true,  /* singleUse */
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
