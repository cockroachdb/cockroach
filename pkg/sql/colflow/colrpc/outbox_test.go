// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOutboxCatchesPanics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input    = colexecbase.NewBatchBuffer()
		typs     = []*types.T{types.Int}
		rpcLayer = makeMockFlowStreamRPCLayer()
	)
	outbox, err := NewOutbox(testAllocator, input, typs, nil /* metadataSources */, nil /* toClose */)
	require.NoError(t, err)

	// This test relies on the fact that BatchBuffer panics when there are no
	// batches to return. Verify this assumption.
	require.Panics(t, func() { input.Next(ctx) })

	// The actual test verifies that the Outbox handles input execution tree
	// panics by not panicking and returning.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)
		wg.Done()
	}()

	inboxMemAccount := testMemMonitor.MakeBoundAccount()
	defer inboxMemAccount.Close(ctx)
	inbox, err := NewInbox(
		colmem.NewAllocator(ctx, &inboxMemAccount, coldata.StandardColumnFactory), typs, execinfrapb.StreamID(0),
	)
	require.NoError(t, err)

	streamHandlerErrCh := handleStream(ctx, inbox, rpcLayer.server, func() { close(rpcLayer.server.csChan) })

	// The outbox will be sending the panic as metadata eagerly. This Next call
	// is valid, but should return a zero-length batch, indicating that the caller
	// should call DrainMeta.
	require.True(t, inbox.Next(ctx).Length() == 0)

	// Expect the panic as an error in DrainMeta.
	meta := inbox.DrainMeta(ctx)

	require.True(t, len(meta) == 1)
	require.True(t, testutils.IsError(meta[0].Err, "runtime error: index out of range"), meta[0])

	require.NoError(t, <-streamHandlerErrCh)
	wg.Wait()
}

func TestOutboxDrainsMetadataSources(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var (
		input = colexecbase.NewBatchBuffer()
		typs  = []*types.T{types.Int}
	)

	// Define common function that returns both an Outbox and a pointer to a
	// uint32 that is set atomically when the outbox drains a metadata source.
	newOutboxWithMetaSources := func(allocator *colmem.Allocator) (*Outbox, *uint32, error) {
		var sourceDrained uint32
		outbox, err := NewOutbox(allocator, input, typs, []execinfrapb.MetadataSource{
			execinfrapb.CallbackMetadataSource{
				DrainMetaCb: func(context.Context) []execinfrapb.ProducerMetadata {
					atomic.StoreUint32(&sourceDrained, 1)
					return nil
				},
			},
		}, nil /* toClose */)
		if err != nil {
			return nil, nil, err
		}
		return outbox, &sourceDrained, nil
	}

	t.Run("AfterSuccessfulRun", func(t *testing.T) {
		rpcLayer := makeMockFlowStreamRPCLayer()
		outboxMemAccount := testMemMonitor.MakeBoundAccount()
		defer outboxMemAccount.Close(ctx)
		outbox, sourceDrained, err := newOutboxWithMetaSources(
			colmem.NewAllocator(ctx, &outboxMemAccount, coldata.StandardColumnFactory),
		)
		require.NoError(t, err)

		b := testAllocator.NewMemBatch(typs)
		b.SetLength(0)
		input.Add(b, typs)

		// Close the csChan to unblock the Recv goroutine (we don't need it for this
		// test).
		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})

	// This is similar to TestOutboxCatchesPanics, but focuses on verifying that
	// the Outbox drains its metadata sources even after an error.
	t.Run("AfterOutboxError", func(t *testing.T) {
		// This test, similar to TestOutboxCatchesPanics, relies on the fact that
		// a BatchBuffer panics when there are no batches to return.
		require.Panics(t, func() { input.Next(ctx) })

		rpcLayer := makeMockFlowStreamRPCLayer()
		outbox, sourceDrained, err := newOutboxWithMetaSources(testAllocator)
		require.NoError(t, err)

		close(rpcLayer.client.csChan)
		outbox.runWithStream(ctx, rpcLayer.client, nil /* cancelFn */)

		require.True(t, atomic.LoadUint32(sourceDrained) == 1)
	})
}
