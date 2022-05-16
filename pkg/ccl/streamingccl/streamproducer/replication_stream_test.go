// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"     // Ensure changefeed init hooks run.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamingtest"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// pgConnReplicationFeedSource yields replicationMessages from the replication stream.
type pgConnReplicationFeedSource struct {
	t      *testing.T
	conn   *pgx.Conn
	rows   pgx.Rows
	codec  pgConnEventDecoder
	cancel func()
}

var _ streamingtest.FeedSource = (*pgConnReplicationFeedSource)(nil)

type pgConnEventDecoder interface {
	decode()
	pop() streamingccl.Event
}

type eventDecoderFactory func(t *testing.T, rows pgx.Rows) pgConnEventDecoder

// Close implements the streamingtest.FeedSource interface. It closes underlying
// sql connection.
func (f *pgConnReplicationFeedSource) Close(ctx context.Context) {
	f.cancel()
	f.rows.Close()
	require.NoError(f.t, f.conn.Close(ctx))
}

type partitionStreamDecoder struct {
	t    *testing.T
	rows pgx.Rows
	e    streampb.StreamEvent
}

func makePartitionStreamDecoder(t *testing.T, rows pgx.Rows) pgConnEventDecoder {
	return &partitionStreamDecoder{
		t:    t,
		rows: rows,
	}
}

func (d *partitionStreamDecoder) pop() streamingccl.Event {
	if d.e.Checkpoint != nil {
		// TODO(yevgeniy): Fix checkpoint handling and support backfill checkpoints.
		// For now, check that we only have one span in the checkpoint, and use that timestamp.
		require.Equal(d.t, 1, len(d.e.Checkpoint.Spans))
		event := streamingccl.MakeCheckpointEvent(d.e.Checkpoint.Spans[0].Timestamp)
		d.e.Checkpoint = nil
		return event
	}

	if d.e.Batch != nil {
		event := streamingccl.MakeKVEvent(d.e.Batch.KeyValues[0])
		d.e.Batch.KeyValues = d.e.Batch.KeyValues[1:]
		if len(d.e.Batch.KeyValues) == 0 {
			d.e.Batch = nil
		}
		return event
	}

	return nil
}

func (d *partitionStreamDecoder) decode() {
	var data []byte
	require.NoError(d.t, d.rows.Scan(&data))
	var streamEvent streampb.StreamEvent
	require.NoError(d.t, protoutil.Unmarshal(data, &streamEvent))
	if streamEvent.Checkpoint == nil && streamEvent.Batch == nil {
		d.t.Fatalf("unexpected event type")
	}
	d.e = streamEvent
}

// Next implements the streamingtest.FeedSource interface.
func (f *pgConnReplicationFeedSource) Next() (streamingccl.Event, bool) {
	if e := f.codec.pop(); e != nil {
		return e, true
	}

	if !f.rows.Next() {
		// The event doesn't matter since we always expect more rows.
		return nil, false
	}

	f.codec.decode()
	e := f.codec.pop()
	require.NotNil(f.t, e)
	return e, true
}

// startReplication starts replication stream, specified as query and its args.
func startReplication(
	t *testing.T,
	r *streamingtest.ReplicationHelper,
	codecFactory eventDecoderFactory,
	create string,
	args ...interface{},
) (*pgConnReplicationFeedSource, *streamingtest.ReplicationFeed) {
	sink := r.PGUrl
	sink.RawQuery = r.PGUrl.Query().Encode()

	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConfig(sink.String())
	require.NoError(t, err)

	queryCtx, cancel := context.WithCancel(context.Background())
	conn, err := pgx.ConnectConfig(queryCtx, pgxConfig)
	require.NoError(t, err)

	rows, err := conn.Query(queryCtx, `SET enable_experimental_stream_replication = true`)
	require.NoError(t, err)
	rows.Close()

	rows, err = conn.Query(queryCtx, `SET avoid_buffering = true`)
	require.NoError(t, err)
	rows.Close()
	rows, err = conn.Query(queryCtx, create, args...)
	require.NoError(t, err)
	feedSource := &pgConnReplicationFeedSource{
		t:      t,
		conn:   conn,
		rows:   rows,
		codec:  codecFactory(t, rows),
		cancel: cancel,
	}
	return feedSource, streamingtest.MakeReplicationFeed(t, feedSource)
}

func TestReplicationStreamInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}

	h, cleanup := streamingtest.NewReplicationHelper(t, serverArgs, serverutils.TestTenantID())
	defer cleanup()

	checkStreamStatus := func(t *testing.T, streamID string, expectedStreamStatus streampb.StreamReplicationStatus_StreamStatus) {
		hlcTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		status, rawStatus := &streampb.StreamReplicationStatus{}, make([]byte, 0)
		row := h.SysDB.QueryRow(t, "SELECT crdb_internal.replication_stream_progress($1, $2)", streamID, hlcTime.String())

		row.Scan(&rawStatus)
		require.NoError(t, protoutil.Unmarshal(rawStatus, status))
		expectedStatus := streampb.StreamReplicationStatus{StreamStatus: expectedStreamStatus}
		// A running stream is expected to report the current protected timestamp for the replicating spans.
		if expectedStatus.StreamStatus == streampb.StreamReplicationStatus_STREAM_ACTIVE {
			require.Equal(t, hlcTime, *status.ProtectedTimestamp)
		}
		require.Equal(t, expectedStreamStatus, status.StreamStatus)
	}

	// Makes the stream time out really soon
	h.SysDB.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '10ms'")
	h.SysDB.Exec(t, "SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1ms'")
	t.Run("failed-after-timeout", func(t *testing.T) {
		rows := h.SysDB.QueryStr(t, "SELECT crdb_internal.start_replication_stream($1)", h.Tenant.ID.ToUint64())
		streamID := rows[0][0]

		h.SysDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %s", streamID),
			[][]string{{"failed"}})
		checkStreamStatus(t, streamID, streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})

	// Make sure the stream does not time out within the test timeout
	h.SysDB.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '500s'")
	t.Run("continuously-running-within-timeout", func(t *testing.T) {
		rows := h.SysDB.QueryStr(t, "SELECT crdb_internal.start_replication_stream($1)", h.Tenant.ID.ToUint64())
		streamID := rows[0][0]

		h.SysDB.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %s", streamID),
			[][]string{{"running"}})

		// Ensures the job is continuously running for 3 seconds.
		testDuration, now := 3*time.Second, timeutil.Now()
		for start, end := now, now.Add(testDuration); start.Before(end); start = start.Add(300 * time.Millisecond) {
			h.SysDB.CheckQueryResults(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %s", streamID),
				[][]string{{"running"}})
			checkStreamStatus(t, streamID, streampb.StreamReplicationStatus_STREAM_ACTIVE)
		}

		// Get a replication stream spec
		spec, rawSpec := &streampb.ReplicationStreamSpec{}, make([]byte, 0)
		row := h.SysDB.QueryRow(t, "SELECT crdb_internal.replication_stream_spec($1)", streamID)
		row.Scan(&rawSpec)
		require.NoError(t, protoutil.Unmarshal(rawSpec, spec))

		// Ensures the processor spec tracks the tenant span
		require.Equal(t, 1, len(spec.Partitions))
		require.Equal(t, 1, len(spec.Partitions[0].PartitionSpec.Spans))
		tenantPrefix := keys.MakeTenantPrefix(h.Tenant.ID)
		require.Equal(t, roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.PrefixEnd()},
			spec.Partitions[0].PartitionSpec.Spans[0])
	})

	t.Run("nonexistent-replication-stream-has-inactive-status", func(t *testing.T) {
		checkStreamStatus(t, "123", streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})
}

func encodeSpec(t *testing.T, h *streamingtest.ReplicationHelper, startFrom hlc.Timestamp, tables ...string) []byte {
	var spans []roachpb.Span
	for _, table := range tables {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			h.SysServer.DB(), h.Tenant.Codec, "d", table)
		spans = append(spans, desc.PrimaryIndexSpan(h.Tenant.Codec))
	}

	spec := &streampb.StreamPartitionSpec{
		StartFrom: startFrom,
		Spans:     spans,
		Config: streampb.StreamPartitionSpec_ExecutionConfig{
			MinCheckpointFrequency: 10 * time.Millisecond,
		},
	}

	opaqueSpec, err := protoutil.Marshal(spec)
	require.NoError(t, err)
	return opaqueSpec
}

func TestStreamPartition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := streamingtest.NewReplicationHelper(t, base.TestServerArgs{}, serverutils.TestTenantID())
	defer cleanup()

	h.Tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
INSERT INTO d.t1 (i) VALUES (42);
USE d;
`)

	ctx := context.Background()
	rows := h.SysDB.QueryStr(t, "SELECT crdb_internal.start_replication_stream($1)", h.Tenant.ID.ToUint64())
	streamID := rows[0][0]

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`
	t1Descr := desctestutils.TestingGetPublicTableDescriptor(h.SysServer.DB(), h.Tenant.Codec, "d", "t1")

	t.Run("stream-table", func(t *testing.T) {
		_, feed := startReplication(t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, hlc.Timestamp{}, "t1"))
		defer feed.Close(ctx)

		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, t1Descr, 42)
		firstObserved := feed.ObserveKey(ctx, expected.Key)

		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		// Periodically, resolved timestamps should be published.
		// Observe resolved timestamp that's higher than the previous value timestamp.
		feed.ObserveResolved(ctx, firstObserved.Value.Timestamp)

		// Update our row.
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, t1Descr, 42, nil, "world")

		// Observe its changes.
		secondObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
		require.True(t, firstObserved.Value.Timestamp.Less(secondObserved.Value.Timestamp))
	})

	t.Run("stream-table-with-cursor", func(t *testing.T) {
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		beforeUpdateTS := h.SysServer.Clock().Now()
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
		h.Tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

		_, feed := startReplication(t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, beforeUpdateTS, "t1"))
		defer feed.Close(ctx)

		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := streamingtest.EncodeKV(t, h.Tenant.Codec, t1Descr, 42, "привет", "world")
		firstObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		expected = streamingtest.EncodeKV(t, h.Tenant.Codec, t1Descr, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
	})

	t.Run("stream-batches-events", func(t *testing.T) {
		h.Tenant.SQL.Exec(t, `
CREATE TABLE t2(
 i INT PRIMARY KEY, 
 a STRING, 
 b STRING,
 INDEX (a,b),   -- Just to have a bit more data in the table
 FAMILY fb (b)
)
`)
		addRows := func(start, n int) {
			// Insert few more rows into the table.  We expect
			for i := start; i < n; i++ {
				h.Tenant.SQL.Exec(t, "INSERT INTO t2 (i, a, b) VALUES ($1, $2, $3)",
					i, fmt.Sprintf("i=%d", i), fmt.Sprintf("10-i=%d", 10-i))
			}
		}

		// Add few rows.
		addRows(0, 10)

		source, feed := startReplication(t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, hlc.Timestamp{}, "t1"))
		defer feed.Close(ctx)

		// Few more rows after feed started.
		addRows(100, 200)

		// By default, we batch up to 1MB of data.
		// Verify we see event batches w/ more than 1 message.
		// TODO(yevgeniy): Extend testing libraries to support batch events and span checkpoints.
		codec := source.codec.(*partitionStreamDecoder)
		for {
			require.True(t, source.rows.Next())
			source.codec.decode()
			if codec.e.Batch != nil && len(codec.e.Batch.KeyValues) > 0 {
				break
			}
		}
	})
}

func TestStreamAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := streamingtest.NewReplicationHelper(t, base.TestServerArgs{}, serverutils.TestTenantID())
	defer cleanup()

	h.Tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
INSERT INTO d.t1 (i) VALUES (1);
USE d;
`)

	ctx := context.Background()
	rows := h.SysDB.QueryStr(t, "SELECT crdb_internal.start_replication_stream($1)", h.Tenant.ID.ToUint64())
	streamID := rows[0][0]

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`

	dataSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if _, err := w.Write([]byte("42,42\n")); err != nil {
				t.Logf("failed to write: %s", err.Error())
			}
		}
	}))
	defer dataSrv.Close()

	testAddSSTable := func(t *testing.T, initialScan bool, addSSTableBeforeRangefeed bool, table string) {
		// Make any import operation to be a AddSSTable operation instead of kv writes.
		h.SysDB.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1';")

		var startTime time.Time
		h.Tenant.SQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, n INT)", table))
		h.Tenant.SQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&startTime)
		startHlcTime := hlc.Timestamp{WallTime: startTime.UnixNano()}
		if initialScan {
			startHlcTime = hlc.Timestamp{}
		}
		if addSSTableBeforeRangefeed {
			h.Tenant.SQL.Exec(t, fmt.Sprintf("IMPORT INTO %s CSV DATA ($1)", table), dataSrv.URL)
		}
		source, feed := startReplication(t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, startHlcTime, table))
		defer feed.Close(ctx)
		if !addSSTableBeforeRangefeed {
			h.Tenant.SQL.Exec(t, fmt.Sprintf("IMPORT INTO %s CSV DATA ($1)", table), dataSrv.URL)
		}

		codec := source.codec.(*partitionStreamDecoder)
		for {
			require.True(t, source.rows.Next())
			source.codec.decode()
			if codec.e.Batch != nil {
				if len(codec.e.Batch.Ssts) > 0 {
					require.Equal(t, 1, len(codec.e.Batch.Ssts))
					require.Regexp(t, "/Tenant/10/Table/.*42.*", codec.e.Batch.Ssts[0].Span.String())
				} else if len(codec.e.Batch.KeyValues) > 0 {
					require.LessOrEqual(t, 1, len(codec.e.Batch.KeyValues))
					require.Regexp(t, "/Tenant/10/Table/.*42.*", codec.e.Batch.KeyValues[0].Key.String())
				}
				break
			}
		}
	}

	tableNum := 1
	for _, initialScan := range []bool{true, false} {
		for _, addSSTableBeforeRangefeed := range []bool{true, false} {
			testAddSSTable(t, initialScan, addSSTableBeforeRangefeed, fmt.Sprintf("x%d", tableNum))
			tableNum++
		}
	}
}
