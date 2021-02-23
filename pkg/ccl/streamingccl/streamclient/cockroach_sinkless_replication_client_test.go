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
	"time"

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

type clientFeedProvider struct {
	t       *testing.T
	msg     streamingccl.Event
	eventCh chan streamingccl.Event
}

func (f *clientFeedProvider) Next() (streamingccl.Event, bool) {
	event, haveMoreRows := <-f.eventCh
	return event, haveMoreRows
}

func (f *clientFeedProvider) Close() {
	close(f.eventCh)
}

func makeFeed(t *testing.T, eventCh chan streamingccl.Event) *streamingtest.ReplciationFeed {
	return streamingtest.MakeReplicaitonFeed(t, &clientFeedProvider{
		t:       t,
		eventCh: eventCh,
	})
}

func TestCoreChangefeedClient(t *testing.T) {
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

	descr := catalogkv.TestingGetTableDescriptor(h.SysServer.DB(), h.Tenant.Codec, "d", "t1")

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

	eventCh, err := client.ConsumePartition(ctx, pa, time.Unix(0, startTime.WallTime))
	require.NoError(t, err)
	feed := makeFeed(t, eventCh)

	// We should observe 2 versions of this key: one with ("привет", "world"), and a later
	// version ("привет", "мир")
	expected := streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42, "привет", "world")
	firstObserved := feed.ObserveKey(expected.Key)
	require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

	expected = streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42, "привет", "мир")
	secondObserved := feed.ObserveKey(expected.Key)
	require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)

	feed.ObserveResolved(secondObserved.Value.Timestamp)

	// TODO: Stream a tenant that doesn't exist.
}
