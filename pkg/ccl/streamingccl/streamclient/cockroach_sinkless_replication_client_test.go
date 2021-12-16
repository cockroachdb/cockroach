// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"     // Ensure changefeed init hooks run.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer" // Ensure we can start replication stream.
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// channelFeedSource wraps the eventsCh returned from a client. It expects that
// no errors are returned from the client.
type channelFeedSource struct {
	t               *testing.T
	cancelIngestion context.CancelFunc
	subscription    Subscription
}

var _ streamingtest.FeedSource = (*channelFeedSource)(nil)

// Next implements the streamingtest.FeedSource interface.
func (f *channelFeedSource) Next() (streamingccl.Event, bool) {
	event, haveMoreRows := <-f.subscription.Events()
	if !haveMoreRows {
		// Err is set after Events channel is closed.
		require.NoError(f.t, f.subscription.Err())
		return nil, false
	}
	return event, haveMoreRows
}

// Close implements the streamingtest.FeedSource interface.
func (f *channelFeedSource) Close(ctx context.Context) {
	f.cancelIngestion()
}

func TestSinklessReplicationClient(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)
	h, cleanup := streamingtest.NewReplicationHelper(t, base.TestServerArgs{})
	defer cleanup()

	ctx := context.Background()

	h.Tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	t1 := catalogkv.TestingGetTableDescriptor(h.SysServer.DB(), h.Tenant.Codec, "d", "t1")

	client := &sinklessReplicationClient{remote: &h.PGUrl}
	defer client.Close()

	id, err := client.Create(ctx, h.Tenant.ID)
	require.NoError(t, err)

	top, err := client.Plan(ctx, id)
	require.NoError(t, err)
	require.Equal(t, 1, len(top))
	token := top[0].SubscriptionToken

	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)

	startTime := h.SysServer.Clock().Now()
	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

	t.Run("replicate_existing_tenant", func(t *testing.T) {
		clientCtx, cancelIngestion := context.WithCancel(ctx)
		sub, err := client.Subscribe(clientCtx, id, token, startTime)
		require.NoError(t, err)
		feedSource := &channelFeedSource{cancelIngestion: cancelIngestion, subscription: sub}
		feed := streamingtest.MakeReplicationFeed(t, feedSource)

		cg := ctxgroup.WithContext(clientCtx)
		cg.GoCtx(sub.Receive)
		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, t1, 42, "привет", "world")
		firstObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, t1, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)

		feed.ObserveResolved(ctx, secondObserved.Value.Timestamp)
		cancelIngestion()
		require.Error(t, cg.Wait(), "context canceled")
	})

	t.Run("stream-address-disconnects", func(t *testing.T) {
		clientCtx, cancelIngestion := context.WithCancel(ctx)
		sub, err := client.Subscribe(clientCtx, id, token, startTime)
		require.NoError(t, err)
		feedSource := &channelFeedSource{subscription: sub}
		feed := streamingtest.MakeReplicationFeed(t, feedSource)

		cg := ctxgroup.WithContext(ctx)
		cg.GoCtx(sub.Receive)
		h.SysServer.Stopper().Stop(clientCtx)

		require.True(t, feed.ObserveGeneration(clientCtx))
		cancelIngestion()
	})
}
