// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"     // Ensure changefeed init hooks run.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/require"
)

// pgConnReplicationFeedSource yields replicationMessages from the replication stream.
type pgConnReplicationFeedSource struct {
	t      *testing.T
	conn   *pgx.Conn
	rows   *pgx.Rows
	cancel func()
}

var _ streamingtest.FeedSource = (*pgConnReplicationFeedSource)(nil)

// Close implements the streamingtest.FeedSource interface. It closes underlying
// sql connection.
func (f *pgConnReplicationFeedSource) Close() {
	f.cancel()
	f.rows.Close()
	require.NoError(f.t, f.conn.Close())
}

// Next implements the streamingtest.FeedSource interface.
func (f *pgConnReplicationFeedSource) Next() (streamingccl.Event, bool) {
	haveMoreRows := f.rows.Next()
	if !haveMoreRows {
		// The event doesn't matter since we always expect more rows.
		return nil, haveMoreRows
	}

	var ignoreTopic gosql.NullString
	var k, v []byte
	require.NoError(f.t, f.rows.Scan(&ignoreTopic, &k, &v))

	var event streamingccl.Event
	if len(k) == 0 {
		var resolved hlc.Timestamp
		require.NoError(f.t, protoutil.Unmarshal(v, &resolved))
		event = streamingccl.MakeCheckpointEvent(resolved)
	} else {
		var val roachpb.Value
		require.NoError(f.t, protoutil.Unmarshal(v, &val))
		event = streamingccl.MakeKVEvent(roachpb.KeyValue{Key: k, Value: val})
	}
	require.NotNil(f.t, event, "could not parse event")
	return event, haveMoreRows
}

// startReplication starts replication stream, specified as query and its args.
func startReplication(
	t *testing.T, r *streamingtest.ReplicationHelper, create string, args ...interface{},
) *streamingtest.ReplicationFeed {
	sink := r.PGUrl
	sink.RawQuery = r.PGUrl.Query().Encode()

	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(sink.String())
	require.NoError(t, err)

	conn, err := pgx.Connect(pgxConfig)
	require.NoError(t, err)

	queryCtx, cancel := context.WithCancel(context.Background())
	rows, err := conn.QueryEx(queryCtx, `SET enable_experimental_stream_replication = true`, nil, args...)
	require.NoError(t, err)
	rows.Close()
	rows, err = conn.QueryEx(queryCtx, create, nil, args...)
	require.NoError(t, err)
	feedSource := &pgConnReplicationFeedSource{
		t:      t,
		conn:   conn,
		rows:   rows,
		cancel: cancel,
	}
	return streamingtest.MakeReplicationFeed(t, feedSource)
}

func TestReplicationStreamTenant(t *testing.T) {
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

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d`, h.Tenant.ID.ToUint64())

	t.Run("cannot-stream-tenant-from-tenant", func(t *testing.T) {
		_, err := h.Tenant.SQL.DB.ExecContext(context.Background(), `SET enable_experimental_stream_replication = true`)
		require.NoError(t, err)
		// Cannot replicate stream from inside the tenant
		_, err = h.Tenant.SQL.DB.ExecContext(context.Background(), streamTenantQuery)
		require.True(t, testutils.IsError(err, "only the system tenant can backup other tenants"), err)
	})

	descr := catalogkv.TestingGetTableDescriptor(h.SysServer.DB(), h.Tenant.Codec, "d", "t1")

	t.Run("stream-tenant", func(t *testing.T) {
		feed := startReplication(t, h, streamTenantQuery)
		defer feed.Close()

		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42)
		firstObserved := feed.ObserveKey(expected.Key)

		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		// Periodically, resolved timestamps should be published.
		// Observe resolved timestamp that's higher than the previous value timestamp.
		feed.ObserveResolved(firstObserved.Value.Timestamp)

		// Update our row.
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42, nil, "world")

		// Observe its changes.
		secondObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
		require.True(t, firstObserved.Value.Timestamp.Less(secondObserved.Value.Timestamp))
	})

	t.Run("stream-tenant-with-cursor", func(t *testing.T) {
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		beforeUpdateTS := h.SysServer.Clock().Now()
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

		feed := startReplication(t, h, fmt.Sprintf(
			"%s WITH cursor='%s'", streamTenantQuery, beforeUpdateTS.AsOfSystemTime()))
		defer feed.Close()

		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42, "привет", "world")
		firstObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, descr, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
	})
}
