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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
		DisableDefaultTestTenant: true,
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
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '10ms'")
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1ms'")
	t.Run("failed-after-timeout", func(t *testing.T) {
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID

		h.SysSQL.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
			[][]string{{"failed"}})
		testStreamReplicationStatus(t, h.SysSQL, streamID, streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})

	// Make sure the stream does not time out within the test timeout
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '500s'")
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

func encodeSpec(
	t *testing.T,
	h *replicationtestutils.ReplicationHelper,
	srcTenant replicationtestutils.TenantState,
	initialScanTime hlc.Timestamp,
	previousHighWater hlc.Timestamp,
	tables ...string,
) []byte {
	var spans []roachpb.Span
	for _, table := range tables {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			h.SysServer.DB(), srcTenant.Codec, "d", table)
		spans = append(spans, desc.PrimaryIndexSpan(srcTenant.Codec))
	}

	spec := &streampb.StreamPartitionSpec{
		InitialScanTimestamp:       initialScanTime,
		PreviousHighWaterTimestamp: previousHighWater,
		Spans:                      spans,
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
			DisableDefaultTestTenant: true,
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
		DisableDefaultTestTenant: true,
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

		var previousHighWater hlc.Timestamp
		initialScanTimestamp := hlc.Timestamp{WallTime: clockTime.UnixNano()}
		if !initialScan {
			previousHighWater = hlc.Timestamp{WallTime: clockTime.UnixNano()}
		}
		if addSSTableBeforeRangefeed {
			srcTenant.SQL.Exec(t, fmt.Sprintf("IMPORT INTO %s CSV DATA ($1)", table), dataSrv.URL)
		}
		source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
			streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
				previousHighWater, table))
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
			DisableDefaultTestTenant: true,
		})
	defer cleanup()
	srcTenantID := serverutils.TestTenantID()
	testTenantName := roachpb.TenantName("test-tenant")
	_, cleanupTenant := h.CreateTenant(t, srcTenantID, testTenantName)
	defer cleanupTenant()

	// Make the producer job times out fast and fastly tracks ingestion cutover signal.
	h.SysSQL.ExecMultiple(t,
		"SET CLUSTER SETTING stream_replication.job_liveness_timeout = '2s';",
		"SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';")

	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	timedOutStreamID := replicationProducerSpec.StreamID
	jobutils.WaitForJobToFail(t, h.SysSQL, jobspb.JobID(timedOutStreamID))

	// Makes the producer job not easily time out.
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.job_liveness_timeout = '10m';")
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
			jobutils.WaitForJobToCancel(t, h.SysSQL, jobspb.JobID(streamID))
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

func sortDelRanges(receivedDelRanges []kvpb.RangeFeedDeleteRange) {
	sort.Slice(receivedDelRanges, func(i, j int) bool {
		if !receivedDelRanges[i].Timestamp.Equal(receivedDelRanges[j].Timestamp) {
			return receivedDelRanges[i].Timestamp.Compare(receivedDelRanges[j].Timestamp) < 0
		}
		if !receivedDelRanges[i].Span.Key.Equal(receivedDelRanges[j].Span.Key) {
			return receivedDelRanges[i].Span.Key.Compare(receivedDelRanges[j].Span.Key) < 0
		}
		return receivedDelRanges[i].Span.EndKey.Compare(receivedDelRanges[j].Span.EndKey) < 0
	})
}

func TestStreamDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 93568)
	skip.UnderStressRace(t, "disabled under stress and race")

	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		// Test hangs when run within the default test tenant. Tracked with
		// #76378.
		DisableDefaultTestTenant: true,
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

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`
	// Only subscribe to table t1 and t2, not t3.
	source, feed := startReplication(ctx, t, h, makePartitionStreamDecoder,
		streamPartitionQuery, streamID, encodeSpec(t, h, srcTenant, initialScanTimestamp,
			hlc.Timestamp{}, "t1", "t2"))
	defer feed.Close(ctx)

	// TODO(casper): Replace with DROP TABLE once drop table uses the MVCC-compatible DelRange
	t1Span, t2Span, t3Span := h.TableSpan(srcTenant.Codec, "t1"),
		h.TableSpan(srcTenant.Codec, "t2"), h.TableSpan(srcTenant.Codec, "t3")
	// Range deleted is outside the subscribed spans
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t2Span.EndKey, t3Span.Key))
	// Range is t1s - t2e, emitting 2 events, t1s - t1e and t2s - t2e.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.Key, t2Span.EndKey))
	// Range is t1e - t2sn, emitting t2s - t2sn.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.EndKey, t2Span.Key.Next()))

	// Expected DelRange spans after sorting.
	expectedDelRangeSpan1 := roachpb.Span{Key: t1Span.Key, EndKey: t1Span.EndKey}
	expectedDelRangeSpan2 := roachpb.Span{Key: t2Span.Key, EndKey: t2Span.EndKey}
	expectedDelRangeSpan3 := roachpb.Span{Key: t2Span.Key, EndKey: t2Span.Key.Next()}

	codec := source.mu.codec.(*partitionStreamDecoder)
	receivedDelRanges := make([]kvpb.RangeFeedDeleteRange, 0, 3)
	for {
		source.mu.Lock()
		require.True(t, source.mu.rows.Next())
		source.mu.codec.decode()
		if codec.e.Batch != nil {
			receivedDelRanges = append(receivedDelRanges, codec.e.Batch.DelRanges...)
		}
		source.mu.Unlock()
		if len(receivedDelRanges) == 3 {
			break
		}
	}

	sortDelRanges(receivedDelRanges)
	require.Equal(t, expectedDelRangeSpan1, receivedDelRanges[0].Span)
	require.Equal(t, expectedDelRangeSpan2, receivedDelRanges[1].Span)
	require.Equal(t, expectedDelRangeSpan3, receivedDelRanges[2].Span)

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
	expectedDelRange1 := kvpb.RangeFeedDeleteRange{Span: t1Span, Timestamp: batchHLCTime}
	expectedDelRange2 := kvpb.RangeFeedDeleteRange{Span: t2Span, Timestamp: batchHLCTime}
	require.Equal(t, t1Span.Key, start)
	require.Equal(t, t3Span.EndKey, end)

	// Using same batch ts so that this SST can be emitted through rangefeed.
	_, _, _, err := h.SysServer.DB().AddSSTableAtBatchTimestamp(ctx, start, end, data, false,
		false, hlc.Timestamp{}, nil, false, batchHLCTime)
	require.NoError(t, err)

	receivedDelRanges = receivedDelRanges[:0]
	receivedKVs := make([]roachpb.KeyValue, 0)
	for {
		source.mu.Lock()
		require.True(t, source.mu.rows.Next())
		source.mu.codec.decode()
		if codec.e.Batch != nil {
			require.Empty(t, codec.e.Batch.Ssts)
			receivedKVs = append(receivedKVs, codec.e.Batch.KeyValues...)
			receivedDelRanges = append(receivedDelRanges, codec.e.Batch.DelRanges...)
		}
		source.mu.Unlock()

		if len(receivedDelRanges) == 2 && len(receivedKVs) == 1 {
			break
		}
	}

	sortDelRanges(receivedDelRanges)
	require.Equal(t, t2Span.Key, receivedKVs[0].Key)
	require.Equal(t, batchHLCTime, receivedKVs[0].Value.Timestamp)
	require.Equal(t, expectedDelRange1, receivedDelRanges[0])
	require.Equal(t, expectedDelRange2, receivedDelRanges[1])
}
