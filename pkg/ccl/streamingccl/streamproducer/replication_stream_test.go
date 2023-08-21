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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"     // Ensure changefeed init hooks run.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// pgConnReplicationFeedSource yields replicationMessages from the replication stream.
type pgConnReplicationFeedSource struct {
	t      *testing.T
	conn   *pgx.Conn
	cancel func()
	mu     struct {
		syncutil.Mutex

		rows  pgx.Rows
		codec pgConnEventDecoder
	}
}

var _ replicationtestutils.FeedSource = (*pgConnReplicationFeedSource)(nil)

type pgConnEventDecoder interface {
	decode()
	pop() streamingccl.Event
}

type eventDecoderFactory func(t *testing.T, rows pgx.Rows) pgConnEventDecoder

// Close implements the streamingtest.FeedSource interface. It closes underlying
// sql connection.
func (f *pgConnReplicationFeedSource) Close(ctx context.Context) {
	f.cancel()

	f.mu.Lock()
	f.mu.rows.Close()
	f.mu.Unlock()
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
		require.Equal(d.t, 1, len(d.e.Checkpoint.ResolvedSpans))
		event := streamingccl.MakeCheckpointEvent(d.e.Checkpoint.ResolvedSpans)
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
	f.mu.Lock()
	defer f.mu.Unlock()
	// First check if there exists a decoded event.
	if e := f.mu.codec.pop(); e != nil {
		return e, true
	}

	if !f.mu.rows.Next() {
		// The event doesn't matter since we always expect more rows.
		return nil, false
	}

	f.mu.codec.decode()
	e := f.mu.codec.pop()
	require.NotNil(f.t, e)
	return e, true
}

// Error implements the streamingtest.FeedSource interface.
func (f *pgConnReplicationFeedSource) Error() error {
	var err error
	f.mu.Lock()
	err = f.mu.rows.Err()
	f.mu.Unlock()
	return err
}

// startReplication starts replication stream, specified as query and its args.
func startReplication(
	ctx context.Context,
	t *testing.T,
	r *replicationtestutils.ReplicationHelper,
	codecFactory eventDecoderFactory,
	create string,
	args ...interface{},
) (*pgConnReplicationFeedSource, *replicationtestutils.ReplicationFeed) {
	sink := r.PGUrl
	sink.RawQuery = r.PGUrl.Query().Encode()

	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConfig(sink.String())
	require.NoError(t, err)

	queryCtx, cancel := context.WithCancel(ctx)
	conn, err := pgx.ConnectConfig(queryCtx, pgxConfig)
	require.NoError(t, err)

	rows, err := conn.Query(queryCtx, `SET CLUSTER SETTING cross_cluster_replication.enabled = true;`)
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
		cancel: cancel,
	}
	feedSource.mu.rows = rows
	feedSource.mu.codec = codecFactory(t, rows)
	return feedSource, replicationtestutils.MakeReplicationFeed(t, feedSource)
}

func testStreamReplicationStatus(
	t *testing.T,
	runner *sqlutils.SQLRunner,
	streamID streampb.StreamID,
	streamStatus streampb.StreamReplicationStatus_StreamStatus,
) {
	checkStreamStatus := func(t *testing.T, frontier hlc.Timestamp,
		expectedStreamStatus streampb.StreamReplicationStatus) {
		status, rawStatus := &streampb.StreamReplicationStatus{}, make([]byte, 0)
		row := runner.QueryRow(t, "SELECT crdb_internal.replication_stream_progress($1, $2)",
			streamID, frontier.String())
		row.Scan(&rawStatus)
		require.NoError(t, protoutil.Unmarshal(rawStatus, status))
		require.Equal(t, expectedStreamStatus, *status)
	}

	updatedFrontier := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	expectedStreamStatus := streampb.StreamReplicationStatus{
		StreamStatus: streamStatus,
	}
	// Send a heartbeat first, the protected timestamp should get updated.
	if streamStatus == streampb.StreamReplicationStatus_STREAM_ACTIVE {
		expectedStreamStatus.ProtectedTimestamp = &updatedFrontier
	}
	checkStreamStatus(t, updatedFrontier, expectedStreamStatus)
	// Send a query.
	// The expected protected timestamp is still 'updatedFrontier' as the protected
	// timestamp doesn't get updated when this is a query.
	checkStreamStatus(t, hlc.MaxTimestamp, expectedStreamStatus)
}

func TestReplicationStreamInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	serverArgs := base.TestServerArgs{
		// This test fails when run from within a test tenant. This is likely
		// due to the lack of support for tenant streaming, but more
		// investigation is required. Tracked with #76378.
		DefaultTestTenant: base.TODOTestTenantDisabled,
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	}

	h, cleanup := replicationtestutils.NewReplicationHelper(t, serverArgs)
	defer cleanup()
	testTenantName := roachpb.TenantName("test-tenant")
	srcTenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	// Makes the stream time out really soon
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness.timeout = '10ms'")
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1ms'")
	t.Run("failed-after-timeout", func(t *testing.T) {
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID

		h.SysSQL.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
			[][]string{{"failed"}})
		testStreamReplicationStatus(t, h.SysSQL, streamID, streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})

	// Make sure the stream does not time out within the test timeout
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness.timeout = '500s'")
	t.Run("continuously-running-within-timeout", func(t *testing.T) {
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID

		h.SysSQL.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
			[][]string{{"running"}})

		// Ensures the job is continuously running for 3 seconds.
		testDuration, now := 3*time.Second, timeutil.Now()
		for start, end := now, now.Add(testDuration); start.Before(end); start = start.Add(300 * time.Millisecond) {
			h.SysSQL.CheckQueryResults(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
				[][]string{{"running"}})
			testStreamReplicationStatus(t, h.SysSQL, streamID, streampb.StreamReplicationStatus_STREAM_ACTIVE)
		}

		// Get a replication stream spec
		spec, rawSpec := &streampb.ReplicationStreamSpec{}, make([]byte, 0)
		row := h.SysSQL.QueryRow(t, "SELECT crdb_internal.replication_stream_spec($1)", streamID)
		row.Scan(&rawSpec)
		require.NoError(t, protoutil.Unmarshal(rawSpec, spec))

		// Ensures the processor spec tracks the tenant span
		require.Equal(t, 1, len(spec.Partitions))
		require.Equal(t, 1, len(spec.Partitions[0].PartitionSpec.Spans))
		tenantPrefix := keys.MakeTenantPrefix(srcTenant.ID)
		require.Equal(t, roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.PrefixEnd()},
			spec.Partitions[0].PartitionSpec.Spans[0])
	})

	t.Run("nonexistent-replication-stream-has-inactive-status", func(t *testing.T) {
		testStreamReplicationStatus(t, h.SysSQL, streampb.StreamID(123), streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})
}

func spansForTables(db *kv.DB, codec keys.SQLCodec, tables ...string) []roachpb.Span {
	spans := make([]roachpb.Span, 0, len(tables))
	for _, table := range tables {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			db, codec, "d", table)
		spans = append(spans, desc.PrimaryIndexSpan(codec))
	}
	return spans
}

func encodeSpec(
	t *testing.T,
	h *replicationtestutils.ReplicationHelper,
	srcTenant replicationtestutils.TenantState,
	initialScanTime hlc.Timestamp,
	previousReplicatedTime hlc.Timestamp,
	tables ...string,
) []byte {
	spans := spansForTables(h.SysServer.DB(), srcTenant.Codec, tables...)
	return encodeSpecForSpans(t, initialScanTime, previousReplicatedTime, spans)
}

func encodeSpecForSpans(
	t *testing.T,
	initialScanTime hlc.Timestamp,
	previousReplicatedTime hlc.Timestamp,
	spans []roachpb.Span,
) []byte {
	spec := &streampb.StreamPartitionSpec{
		InitialScanTimestamp:        initialScanTime,
		PreviousReplicatedTimestamp: previousReplicatedTime,
		Spans:                       spans,
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
	h, cleanup := replicationtestutils.NewReplicationHelper(t,
		base.TestServerArgs{
			// Test fails within a test tenant. More investigation is required.
			// Tracked with #76378.
			DefaultTestTenant: base.TODOTestTenantDisabled,
		})
	defer cleanup()
	testTenantName := roachpb.TenantName("test-tenant")
	srcTenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	srcTenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
INSERT INTO d.t1 (i) VALUES (42);
USE d;
`)

	ctx := context.Background()
	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	streamID := replicationProducerSpec.StreamID
	initialScanTimestamp := replicationProducerSpec.ReplicationStartTime

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`
	t1Descr := desctestutils.TestingGetPublicTableDescriptor(h.SysServer.DB(), srcTenant.Codec, "d", "t1")

	t.Run("stream-table-cursor-error", func(t *testing.T) {

		srcTenant.SQL.Exec(t, `
CREATE TABLE d.t2(i int primary key, a string, b string);
INSERT INTO d.t2 (i) VALUES (42);
`)
		_, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp, hlc.Timestamp{}, "t2"))
		defer feed.Close(ctx)
		t2Descr := desctestutils.TestingGetPublicTableDescriptor(h.SysServer.DB(), srcTenant.Codec, "d", "t2")
		expected := replicationtestutils.EncodeKV(t, srcTenant.Codec, t2Descr, 42)
		feed.ObserveKey(ctx, expected.Key)

		subscribedSpan := h.TableSpan(srcTenant.Codec, "t2")
		// Send a ClearRange to trigger rows cursor to return internal error from rangefeed.
		_, err := kv.SendWrapped(ctx, h.SysServer.DB().NonTransactionalSender(), &kvpb.ClearRangeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    subscribedSpan.Key,
				EndKey: subscribedSpan.EndKey,
			},
		})
		require.Nil(t, err)

		feed.ObserveError(ctx, func(err error) bool {
			return strings.Contains(err.Error(), "unexpected MVCC history mutation")
		})
	})

	t.Run("stream-table", func(t *testing.T) {
		_, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
				hlc.Timestamp{}, "t1"))
		defer feed.Close(ctx)

		expected := replicationtestutils.EncodeKV(t, srcTenant.Codec, t1Descr, 42)
		firstObserved := feed.ObserveKey(ctx, expected.Key)

		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		// Periodically, resolved timestamps should be published.
		// Observe resolved timestamp that's higher than the previous value timestamp.
		feed.ObserveResolved(ctx, firstObserved.Value.Timestamp)

		// Update our row.
		srcTenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		expected = replicationtestutils.EncodeKV(t, srcTenant.Codec, t1Descr, 42, nil, "world")

		// Observe its changes.
		secondObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
		require.True(t, firstObserved.Value.Timestamp.Less(secondObserved.Value.Timestamp))
	})

	t.Run("stream-table-with-cursor", func(t *testing.T) {
		srcTenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		beforeUpdateTS := h.SysServer.Clock().Now()
		srcTenant.SQL.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
		srcTenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

		_, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
				beforeUpdateTS, "t1"))
		defer feed.Close(ctx)

		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := replicationtestutils.EncodeKV(t, srcTenant.Codec, t1Descr, 42, "привет", "world")
		firstObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)

		expected = replicationtestutils.EncodeKV(t, srcTenant.Codec, t1Descr, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(ctx, expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
	})

	t.Run("stream-batches-events", func(t *testing.T) {
		srcTenant.SQL.Exec(t, `
CREATE TABLE t3(
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
				srcTenant.SQL.Exec(t, "INSERT INTO t3 (i, a, b) VALUES ($1, $2, $3)",
					i, fmt.Sprintf("i=%d", i), fmt.Sprintf("10-i=%d", 10-i))
			}
		}

		// Add few rows.
		addRows(0, 10)

		source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
				hlc.Timestamp{}, "t1"))
		defer feed.Close(ctx)

		// Few more rows after feed started.
		addRows(100, 200)

		// By default, we batch up to 1MB of data.
		// Verify we see event batches w/ more than 1 message.
		// TODO(yevgeniy): Extend testing libraries to support batch events and span checkpoints.
		source.mu.Lock()
		defer source.mu.Unlock()
		codec := source.mu.codec.(*partitionStreamDecoder)
		for {
			require.True(t, source.mu.rows.Next())
			source.mu.codec.decode()
			if codec.e.Batch != nil && len(codec.e.Batch.KeyValues) > 0 {
				break
			}
		}
	})
}

func TestStreamAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		// Test hangs when run within the default test tenant. Tracked with
		// #76378.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer cleanup()
	testTenantName := roachpb.TenantName("test-tenant")
	srcTenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	srcTenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
INSERT INTO d.t1 (i) VALUES (1);
USE d;
`)

	ctx := context.Background()
	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	streamID := replicationProducerSpec.StreamID

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
		h.SysSQL.Exec(t, "SET CLUSTER SETTING kv.bulk_io_write.small_write_size = '1';")

		var clockTime time.Time
		srcTenant.SQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, n INT)", table))
		srcTenant.SQL.QueryRow(t, "SELECT clock_timestamp()").Scan(&clockTime)

		var previousReplicatedTime hlc.Timestamp
		initialScanTimestamp := hlc.Timestamp{WallTime: clockTime.UnixNano()}
		if !initialScan {
			previousReplicatedTime = hlc.Timestamp{WallTime: clockTime.UnixNano()}
		}
		if addSSTableBeforeRangefeed {
			srcTenant.SQL.Exec(t, fmt.Sprintf("IMPORT INTO %s CSV DATA ($1)", table), dataSrv.URL)
		}
		source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
				previousReplicatedTime, table))
		defer feed.Close(ctx)
		if !addSSTableBeforeRangefeed {
			srcTenant.SQL.Exec(t, fmt.Sprintf("IMPORT INTO %s CSV DATA ($1)", table), dataSrv.URL)
		}

		source.mu.Lock()
		defer source.mu.Unlock()
		codec := source.mu.codec.(*partitionStreamDecoder)
		for {
			require.True(t, source.mu.rows.Next())
			source.mu.codec.decode()
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

func TestCompleteStreamReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	h, cleanup := replicationtestutils.NewReplicationHelper(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			DefaultTestTenant: base.TODOTestTenantDisabled,
		})
	defer cleanup()
	srcTenantID := serverutils.TestTenantID()
	testTenantName := roachpb.TenantName("test-tenant")
	_, cleanupTenant := h.CreateTenant(t, srcTenantID, testTenantName)
	defer cleanupTenant()

	// Make the producer job times out fast and fastly tracks ingestion cutover signal.
	h.SysSQL.ExecMultiple(t,
		"SET CLUSTER SETTING stream_replication.job_liveness.timeout = '2s';",
		"SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';")

	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	timedOutStreamID := replicationProducerSpec.StreamID
	jobutils.WaitForJobToFail(t, h.SysSQL, jobspb.JobID(timedOutStreamID))

	// Makes the producer job not easily time out.
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness.timeout = '10m';")
	testCompleteStreamReplication := func(t *testing.T, successfulIngestion bool) {
		// Verify no error when completing a timed out replication stream.
		h.SysSQL.Exec(t, "SELECT crdb_internal.complete_replication_stream($1, $2)",
			timedOutStreamID, successfulIngestion)

		// Create a new replication stream and complete it.
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID
		jobutils.WaitForJobToRun(t, h.SysSQL, jobspb.JobID(streamID))
		h.SysSQL.Exec(t, "SELECT crdb_internal.complete_replication_stream($1, $2)",
			streamID, successfulIngestion)

		if successfulIngestion {
			jobutils.WaitForJobToSucceed(t, h.SysSQL, jobspb.JobID(streamID))
		} else {
			jobutils.WaitForJobToFail(t, h.SysSQL, jobspb.JobID(streamID))
		}
		// Verify protected timestamp record gets released.
		jr := h.SysServer.JobRegistry().(*jobs.Registry)
		pj, err := jr.LoadJob(ctx, jobspb.JobID(streamID))
		require.NoError(t, err)
		payload := pj.Payload()
		ptp := h.SysServer.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
		require.ErrorIs(t, h.SysServer.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err = ptp.WithTxn(txn).GetRecord(ctx, payload.GetStreamReplication().ProtectedTimestampRecordID)
			return err
		}), protectedts.ErrNotExists)
	}

	for _, tc := range []struct {
		testName            string
		successfulIngestion bool
	}{
		{"complete-with-successful-ingestion", true},
		{"complete-without-successful-ingestion", false},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			testCompleteStreamReplication(t, tc.successfulIngestion)
		})
	}
}

func TestStreamDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "disabled under stress and race")

	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		// Test hangs when run within the default test tenant. Tracked with
		// #76378.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer cleanup()
	testTenantName := roachpb.TenantName("test-tenant")
	srcTenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	srcTenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key, a string, b string);
CREATE TABLE d.t3(i int primary key, a string, b string);
INSERT INTO d.t1 (i) VALUES (1);
INSERT INTO d.t2 (i) VALUES (1);
INSERT INTO d.t3 (i) VALUES (1);
USE d;
`)

	ctx := context.Background()
	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	streamID := replicationProducerSpec.StreamID
	initialScanTimestamp := replicationProducerSpec.ReplicationStartTime
	streamResumeTimestamp := h.SysServer.Clock().Now()

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`
	// Only subscribe to table t1 and t2, not t3.
	// We start the stream at a resume timestamp to avoid any initial scan.
	spans := spansForTables(h.SysServer.DB(), srcTenant.Codec, "t1", "t2")
	spec := encodeSpecForSpans(t, initialScanTimestamp, streamResumeTimestamp, spans)

	source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
		streamPartitionQuery, streamID, spec)
	defer feed.Close(ctx)
	codec := source.mu.codec.(*partitionStreamDecoder)

	// We wait for the frontier to advance because we want to
	// ensure that we encounter the range deletes during the
	// rangefeed's steady state rather than the catchup scan.
	//
	// The representation of the range deletes we send is slightly
	// different if we encounter them during the catchup scan.
	//
	// NB: It is _still_ possible that we encounter the range
	// deletes during a catchup scan if we hit a rangefeed restart
	// during the test.
	f, err := span.MakeFrontier(spans...)
	require.NoError(t, err)
	for f.Frontier().IsEmpty() {
		t.Logf("waiting for frontier to advance to a non-zero timestamp")
		source.mu.Lock()
		source.mu.rows.Next()
		source.mu.codec.decode()
		if codec.e.Checkpoint != nil {
			for _, rs := range codec.e.Checkpoint.ResolvedSpans {
				_, err := f.Forward(rs.Span, rs.Timestamp)
				require.NoError(t, err)
			}
		}
		source.mu.Unlock()
	}
	t.Logf("frontier advanced to a %s", f.Frontier())

	t1Span, t2Span, t3Span := h.TableSpan(srcTenant.Codec, "t1"),
		h.TableSpan(srcTenant.Codec, "t2"), h.TableSpan(srcTenant.Codec, "t3")
	// Range deleted is outside the subscribed spans
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t2Span.EndKey, t3Span.Key))
	// Range is t1s - t2e, emitting 2 events, t1s - t1e and t2s - t2e.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.Key, t2Span.EndKey))
	// Range is t1e - t2sn, emitting t2s - t2sn.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.EndKey, t2Span.Key.Next()))

	// Expected DelRange events. We store these and the received
	// del ranges in maps to account for possible duplicate
	// delivery.
	expectedDelRanges := make(map[string]struct{})
	expectedDelRanges[roachpb.Span{Key: t1Span.Key, EndKey: t1Span.EndKey}.String()] = struct{}{}
	expectedDelRanges[roachpb.Span{Key: t2Span.Key, EndKey: t2Span.EndKey}.String()] = struct{}{}
	expectedDelRanges[roachpb.Span{Key: t2Span.Key, EndKey: t2Span.Key.Next()}.String()] = struct{}{}

	receivedDelRanges := make(map[string]struct{})
	for {
		source.mu.Lock()
		require.True(t, source.mu.rows.Next())
		source.mu.codec.decode()
		if codec.e.Batch != nil {
			for _, dr := range codec.e.Batch.DelRanges {
				receivedDelRanges[dr.Span.String()] = struct{}{}
			}
		}
		source.mu.Unlock()
		if len(receivedDelRanges) >= 3 {
			break
		}
	}

	require.Equal(t, expectedDelRanges, receivedDelRanges)

	// Adding a SSTable that contains DeleteRange
	batchHLCTime := h.SysServer.Clock().Now()
	batchHLCTime.Logical = 0
	ts := int(batchHLCTime.WallTime)
	data, start, end := storageutils.MakeSST(t, h.SysServer.ClusterSettings(), []interface{}{
		storageutils.PointKV(string(t2Span.Key), ts, "5"),
		// Delete range from t1s - t2s, emitting t1s - t1e.
		storageutils.RangeKV(string(t1Span.Key), string(t2Span.Key), ts, ""),
		// Delete range from t1e - t2enn, emitting t2s - t2e.
		storageutils.RangeKV(string(t1Span.EndKey), string(t2Span.EndKey.Next().Next()), ts, ""),
		// Delete range for t2sn - t2en, which overlaps the range above on t2s - t2e, emitting nothing.
		storageutils.RangeKV(string(t2Span.Key.Next()), string(t2Span.EndKey.Next()), ts, ""),
		// Delete range for t3s - t3e, emitting nothing.
		storageutils.RangeKV(string(t3Span.Key), string(t3Span.EndKey), ts, ""),
	})
	require.Equal(t, t1Span.Key, start)
	require.Equal(t, t3Span.EndKey, end)

	expectedDelRanges = make(map[string]struct{})
	expectedDelRanges[t1Span.String()] = struct{}{}
	expectedDelRanges[t2Span.String()] = struct{}{}

	// Using same batch ts so that this SST can be emitted through rangefeed.
	_, _, _, err = h.SysServer.DB().AddSSTableAtBatchTimestamp(ctx, start, end, data, false,
		false, hlc.Timestamp{}, nil, false, batchHLCTime)
	require.NoError(t, err)

	receivedDelRanges = make(map[string]struct{})
	receivedKVs := make([]roachpb.KeyValue, 0)
	for {
		source.mu.Lock()
		require.True(t, source.mu.rows.Next())
		source.mu.codec.decode()
		if codec.e.Batch != nil {
			require.Empty(t, codec.e.Batch.Ssts)
			receivedKVs = append(receivedKVs, codec.e.Batch.KeyValues...)
			for _, dr := range codec.e.Batch.DelRanges {
				receivedDelRanges[dr.Span.String()] = struct{}{}
			}
		}
		source.mu.Unlock()

		if len(receivedDelRanges) >= 2 && len(receivedKVs) >= 1 {
			break
		}
	}

	require.Equal(t, t2Span.Key, receivedKVs[0].Key)
	require.Equal(t, batchHLCTime, receivedKVs[0].Value.Timestamp)
	require.Equal(t, expectedDelRanges, receivedDelRanges)
}

func TestStreamSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Use a dummy span config table to avoid dealing with the default span configs set on the tenant.
	const dummySpanConfigurationsName = "dummy_span_configurations"
	dummyFQN := tree.NewTableNameWithSchema("d", catconstants.PublicSchemaName, dummySpanConfigurationsName)

	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Streaming: &sql.StreamingTestingKnobs{
				MockSpanConfigTableName: dummyFQN,
			},
		},
	})
	defer cleanup()

	h.SysSQL.Exec(t, `
CREATE DATABASE d;
USE d;`)
	h.SysSQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyFQN))

	tenantID := roachpb.MustMakeTenantID(uint64(10))
	tenantName := roachpb.TenantName("app")

	_, tenantCleanup := h.CreateTenant(t, tenantID, tenantName)
	defer tenantCleanup()

	tenantCodec := keys.MakeSQLCodec(tenantID)
	accessor := spanconfigkvaccessor.New(
		h.SysServer.DB(),
		h.SysServer.InternalExecutor().(isql.Executor),
		h.SysServer.ClusterSettings(),
		h.SysServer.Clock(),
		dummyFQN.String(),
		nil, /* knobs */
	)

	const streamSpanConfigsQuery = `SELECT * FROM crdb_internal.setup_span_configs_stream($1)`
	source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
		streamSpanConfigsQuery, tenantName)
	defer feed.Close(ctx)

	makeRecord := func(targetSpan roachpb.Span, ttl int) spanconfig.Record {
		return replicationtestutils.MakeSpanConfigRecord(t, targetSpan, ttl)
	}

	makeTableSpan := func(highTableID uint32) roachpb.Span {
		syntheticTableSpanPrefix := tenantCodec.TablePrefix(highTableID)
		return roachpb.Span{Key: syntheticTableSpanPrefix, EndKey: syntheticTableSpanPrefix.PrefixEnd()}
	}

	irrelevantTenant := keys.MakeSQLCodec(roachpb.MustMakeTenantID(231))
	t1Span, t2Span, t3Span := makeTableSpan(1001), makeTableSpan(1002), makeTableSpan(1003)
	t123Span := roachpb.Span{Key: t1Span.Key, EndKey: t3Span.EndKey}
	expectedSpanConfigs := makeSpanConfigUpdateRecorder()

	for ts, toApply := range []struct {
		updates  []spanconfig.Record
		deletes  []spanconfig.Target
		expected []spanconfig.Record
	}{
		{
			// This irrelevant span should not surface.
			updates:  []spanconfig.Record{makeRecord(irrelevantTenant.TenantSpan(), 3)},
			expected: []spanconfig.Record{},
		},
		{
			updates:  []spanconfig.Record{makeRecord(t1Span, 2)},
			expected: []spanconfig.Record{makeRecord(t1Span, 2)},
		},
		{
			// Update the Span.
			updates:  []spanconfig.Record{makeRecord(t1Span, 3)},
			expected: []spanconfig.Record{makeRecord(t1Span, 3)},
		},
		{
			// Add Two new records at the same time
			updates:  []spanconfig.Record{makeRecord(t2Span, 2), makeRecord(t3Span, 5)},
			expected: []spanconfig.Record{makeRecord(t2Span, 2), makeRecord(t3Span, 5)},
		},
		{
			// Merge these Records
			deletes:  []spanconfig.Target{spanconfig.MakeTargetFromSpan(t2Span), spanconfig.MakeTargetFromSpan(t3Span)},
			updates:  []spanconfig.Record{makeRecord(t123Span, 10)},
			expected: []spanconfig.Record{makeRecord(t2Span, 0), makeRecord(t3Span, 0), makeRecord(t123Span, 10)},
		},
		{
			// Split the records
			updates:  []spanconfig.Record{makeRecord(t1Span, 1), makeRecord(t2Span, 2), makeRecord(t3Span, 3)},
			expected: []spanconfig.Record{makeRecord(t1Span, 1), makeRecord(t2Span, 2), makeRecord(t3Span, 3)},
		},
		{
			// Merge these Records again, but include a delete of the t1Span. The
			// delete of the t1Span does not get replicated because the t123span
			// update is the latest operation on that span config row in the
			// accessor.UpdateSpanConfigRecords transaction.
			deletes: []spanconfig.Target{
				spanconfig.MakeTargetFromSpan(t1Span),
				spanconfig.MakeTargetFromSpan(t2Span),
				spanconfig.MakeTargetFromSpan(t3Span)},
			updates:  []spanconfig.Record{makeRecord(t123Span, 10)},
			expected: []spanconfig.Record{makeRecord(t2Span, 0), makeRecord(t3Span, 0), makeRecord(t123Span, 10)},
		},
		{
			// Split the records, but include a delete of the t123 span. The delete
			// does not get replicated because the t1 update is the latest operation
			// on that row in the accessor.UpdateSpaConfigRecords
			// transaction.
			deletes:  []spanconfig.Target{spanconfig.MakeTargetFromSpan(t123Span)},
			updates:  []spanconfig.Record{makeRecord(t1Span, 1), makeRecord(t2Span, 2), makeRecord(t3Span, 3)},
			expected: []spanconfig.Record{makeRecord(t1Span, 1), makeRecord(t2Span, 2), makeRecord(t3Span, 3)},
		},
	} {
		toApply := toApply
		require.NoError(t, accessor.UpdateSpanConfigRecords(ctx, toApply.deletes, toApply.updates,
			hlc.MinTimestamp,
			hlc.MaxTimestamp), "failed on update %d (index starts at 0)", ts)
		for _, record := range toApply.expected {
			require.True(t, expectedSpanConfigs.maybeAddNewRecord(replicationtestutils.RecordToEntry(record), int64(ts)))
		}
	}
	receivedSpanConfigs := makeSpanConfigUpdateRecorder()
	codec := source.mu.codec.(*partitionStreamDecoder)
	updateCount := 0
	for {
		source.mu.Lock()
		require.True(t, source.mu.rows.Next())
		source.mu.codec.decode()
		if codec.e.Batch != nil {
			for _, cfg := range codec.e.Batch.SpanConfigs {
				if receivedSpanConfigs.maybeAddNewRecord(cfg.SpanConfig, cfg.Timestamp.WallTime) {
					updateCount++
				}
			}
		}
		source.mu.Unlock()
		if updateCount == len(expectedSpanConfigs.allUpdates) {
			break
		}
	}
	require.Equal(t, expectedSpanConfigs.String(), receivedSpanConfigs.String())
}

type spanConfigUpdateRecorder struct {
	allUpdates     map[string]struct{}
	orderedUpdates []sortedSpanConfigUpdates
	latestTime     int64
}

func (s *spanConfigUpdateRecorder) maybeAddNewRecord(
	record roachpb.SpanConfigEntry, ts int64,
) bool {
	stringedUpdate := record.String() + fmt.Sprintf(" ts:%d", ts)
	if _, ok := s.allUpdates[stringedUpdate]; !ok {
		s.allUpdates[stringedUpdate] = struct{}{}
	} else {
		return false
	}
	if ts > s.latestTime {
		s.latestTime = ts
		s.orderedUpdates = append(s.orderedUpdates, make(sortedSpanConfigUpdates, 0))
	}
	idx := len(s.orderedUpdates) - 1
	s.orderedUpdates[idx] = append(s.orderedUpdates[idx], record)
	sort.Sort(s.orderedUpdates[idx])
	return true
}

func (s *spanConfigUpdateRecorder) String() string {
	var b strings.Builder
	for i, updates := range s.orderedUpdates {
		b.WriteString(fmt.Sprintf("\n Ts %d:", i))
		for _, update := range updates {
			b.WriteString(fmt.Sprintf(" %s: ttl %d,", update.Target.GetSpan(), update.Config.GCPolicy.TTLSeconds))
		}
	}
	return b.String()
}

func makeSpanConfigUpdateRecorder() spanConfigUpdateRecorder {
	return spanConfigUpdateRecorder{
		orderedUpdates: make([]sortedSpanConfigUpdates, 0),
		allUpdates:     make(map[string]struct{}),
	}
}

type sortedSpanConfigUpdates []roachpb.SpanConfigEntry

func (a sortedSpanConfigUpdates) Len() int      { return len(a) }
func (a sortedSpanConfigUpdates) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortedSpanConfigUpdates) Less(i, j int) bool {
	return a[i].Target.GetSpan().Key.Compare(a[j].Target.GetSpan().Key) < 0
}
