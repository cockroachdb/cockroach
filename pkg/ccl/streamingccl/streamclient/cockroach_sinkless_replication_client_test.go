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

	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"     // Ensure changefeed init hooks run.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamproducer" // Ensure we can start replication stream.
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// channelFeedSource wraps the eventsCh returned from a client. It expects that
// no errors are returned from the client.
type channelFeedSource struct {
	t       *testing.T
	eventCh chan streamingccl.Event
	errCh   chan error
}

var _ streamingtest.FeedSource = (*channelFeedSource)(nil)

// Next implements the streamingtest.FeedSource interface.
func (f *channelFeedSource) Next() (streamingccl.Event, bool) {
	// First check for any errors.
	select {
	case err := <-f.errCh:
		require.NoError(f.t, err)
		return nil, false
	default:
	}

	event, haveMoreRows := <-f.eventCh
	return event, haveMoreRows
}

// Close implements the streamingtest.FeedSource interface.
func (f *channelFeedSource) Close() {
	close(f.eventCh)
}

func TestSinklessReplicationClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := streamingtest.NewReplicationHelper(t)
	defer cleanup()

	h.Tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	t1 := catalogkv.TestingGetTableDescriptor(h.SysServer.DB(), h.Tenant.Codec, "d", "t1")

	pgURL := h.PGUrl
	q := pgURL.Query()
	q.Set(TenantID, h.Tenant.ID.String())
	pgURL.RawQuery = q.Encode()

	sa := streamingccl.StreamAddress(pgURL.String())

	client := &sinklessReplicationClient{}
	top, err := client.GetTopology(sa)
	require.NoError(t, err)
	require.Equal(t, 1, len(top.Partitions))
	pa := top.Partitions[0]
	require.Equal(t, streamingccl.PartitionAddress(pgURL.String()), pa)

	ctx := context.Background()

	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)

	startTime := h.SysServer.Clock().Now()
	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
	h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

	t.Run("replicate_existing_tenant", func(t *testing.T) {
		clientCtx, cancelIngestion := context.WithCancel(ctx)
		eventCh, errCh, err := client.ConsumePartition(clientCtx, pa, startTime)
		require.NoError(t, err)
		feedSource := &channelFeedSource{eventCh: eventCh, errCh: errCh}
		feed := streamingtest.MakeReplicationFeed(t, feedSource)

		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, t1, 42, "привет", "world")
		firstObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, t1, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)

		feed.ObserveResolved(secondObserved.Value.Timestamp)
		cancelIngestion()
	})
}
