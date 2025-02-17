// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer_test

import (
	"bytes"
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
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
	"github.com/jackc/pgx/v5"
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
	pop() crosscluster.Event
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

func (d *partitionStreamDecoder) pop() crosscluster.Event {
	if d.e.Checkpoint != nil {
		// TODO(yevgeniy): Fix checkpoint handling and support backfill checkpoints.
		// For now, check that we only have one span in the checkpoint, and use that timestamp.
		require.Equal(d.t, 1, len(d.e.Checkpoint.ResolvedSpans))
		event := crosscluster.MakeCheckpointEvent(d.e.Checkpoint)
		d.e.Checkpoint = nil
		return event
	}

	if d.e.Batch != nil {
		event := crosscluster.MakeKVEvent(d.e.Batch.KVs[0:1])
		d.e.Batch.KVs = d.e.Batch.KVs[1:]
		if len(d.e.Batch.KVs) == 0 {
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
func (f *pgConnReplicationFeedSource) Next() (crosscluster.Event, bool) {
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

	rows, err := conn.Query(queryCtx, `SET avoid_buffering = true`)
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
}

func TestReplicationStreamInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	serverArgs := base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
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
	h.SysSQL.Exec(t, "SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1ms'")
	t.Run("failed-after-timeout", func(t *testing.T) {
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID
		jobutils.WaitForJobToRun(t, h.SysSQL, jobspb.JobID(streamID))
		h.SysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='1ms'`, testTenantName))
		jobutils.WaitForJobToFail(t, h.SysSQL, jobspb.JobID(streamID))
		testStreamReplicationStatus(t, h.SysSQL, streamID, streampb.StreamReplicationStatus_STREAM_INACTIVE)
	})

	// Make sure the stream does not time out within the test timeout
	t.Run("continuously-running-within-timeout", func(t *testing.T) {
		replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
		streamID := replicationProducerSpec.StreamID

		h.SysSQL.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
			[][]string{{"running"}})
		h.SysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='1hr'`, testTenantName))
		// Ensures the job is continuously running for 3 seconds.
		testDuration, now := 3*time.Second, timeutil.Now()
		for start, end := now, now.Add(testDuration); start.Before(end); start = start.Add(300 * time.Millisecond) {
			h.SysSQL.CheckQueryResults(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
				[][]string{{"running"}})
			testStreamReplicationStatus(t, h.SysSQL, streamID, streampb.StreamReplicationStatus_STREAM_ACTIVE)
		}

		// Get a replication stream spec.
		spec, rawSpec := &streampb.ReplicationStreamSpec{}, make([]byte, 0)
		row := h.SysSQL.QueryRow(t, "SELECT crdb_internal.replication_stream_spec($1)", streamID)
		row.Scan(&rawSpec)
		require.NoError(t, protoutil.Unmarshal(rawSpec, spec))

		// Ensures the processor spec tracks the tenant span.
		require.Equal(t, 1, len(spec.Partitions))
		require.Equal(t, 1, len(spec.Partitions[0].SourcePartition.Spans))
		require.Equal(t, keys.MakeTenantSpan(srcTenant.ID), spec.Partitions[0].SourcePartition.Spans[0])
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
	var progress []jobspb.ResolvedSpan
	for _, span := range spans {
		progress = append(progress, jobspb.ResolvedSpan{Span: span, Timestamp: previousReplicatedTime})
	}
	spec := &streampb.StreamPartitionSpec{
		InitialScanTimestamp:        initialScanTime,
		PreviousReplicatedTimestamp: previousReplicatedTime,
		Spans:                       spans,
		Progress:                    progress,
		WrappedEvents:               true,
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
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
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

		stats := feed.ObserveRangeStats(ctx)
		require.Greater(t, stats.RangeCount, int64(0))

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
			if codec.e.Batch != nil && len(codec.e.Batch.KVs) > 0 {
				break
			}
		}
	})
}

func TestStreamAddSSTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
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
				} else if len(codec.e.Batch.KVs) > 0 {
					require.LessOrEqual(t, 1, len(codec.e.Batch.KVs))
					require.Regexp(t, "/Tenant/10/Table/.*42.*", codec.e.Batch.KVs[0].KeyValue.Key.String())
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
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		})
	defer cleanup()
	srcTenantID := serverutils.TestTenantID()
	testTenantName := roachpb.TenantName("test-tenant")
	_, cleanupTenant := h.CreateTenant(t, srcTenantID, testTenantName)
	defer cleanupTenant()

	h.SysSQL.ExecMultiple(t,
		"SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '2s';")

	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	timedOutStreamID := replicationProducerSpec.StreamID
	jobutils.WaitForJobToRun(t, h.SysSQL, jobspb.JobID(timedOutStreamID))
	h.SysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='1ms'`, testTenantName))
	jobutils.WaitForJobToFail(t, h.SysSQL, jobspb.JobID(timedOutStreamID))

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
		h.SysSQL.Exec(t, fmt.Sprintf(`ALTER TENANT '%s' SET REPLICATION EXPIRATION WINDOW ='100ms'`, testTenantName))

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

	skip.UnderRace(t, "disabled under race")

	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
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

	// normalizeRangeKeys pushes all range keys into an in-memory pebble
	// engine and then reads them back out. The goal here is to account for
	// the fact that we may see a logical equivalent set of range keys on
	// the replication stream that doesn't exactly match the keys above if
	// we happen to get the range keys during the catchup scan.
	normalizeRangeKeys := func(in []roachpb.Span) []roachpb.Span {
		var (
			// We don't currently care about timestamps in this
			// test, so we write at a known ts.
			startTS = hlc.Timestamp{WallTime: 1}
			writeTS = hlc.Timestamp{WallTime: 2}
			endTS   = hlc.Timestamp{WallTime: 10}
			sp      = srcTenant.Codec.TenantSpan()
		)
		engine := storage.NewDefaultInMemForTesting()
		defer engine.Close()
		for _, key := range in {
			rk := storage.MVCCRangeKey{
				StartKey:  key.Key,
				EndKey:    key.EndKey,
				Timestamp: writeTS,
			}
			require.NoError(t, engine.PutRawMVCCRangeKey(rk, []byte{}))
		}
		require.NoError(t, engine.Flush())

		var sstFile bytes.Buffer
		_, _, err := storage.MVCCExportToSST(ctx,
			cluster.MakeTestingClusterSettings(), engine,
			storage.MVCCExportOptions{
				StartKey:           storage.MVCCKey{Key: sp.Key, Timestamp: startTS},
				EndKey:             sp.EndKey,
				StartTS:            startTS,
				EndTS:              endTS,
				ExportAllRevisions: true,
			}, &sstFile)
		require.NoError(t, err, "failed to export expected data")
		keys, rKeys := storageutils.KeysFromSST(t, sstFile.Bytes())
		require.Equal(t, 0, len(keys), "unexpected point keys")
		rKeySpans := make([]roachpb.Span, 0, len(rKeys))
		for _, rk := range rKeys {
			rKeySpans = append(rKeySpans, rk.Bounds)
			require.Equal(t, 1, len(rk.Versions))
		}
		return rKeySpans
	}

	consumeUntilTimestamp := func(ts hlc.Timestamp) ([]streampb.StreamEvent_KV, []roachpb.Span) {
		t.Logf("consuming until %s", ts)
		receivedKVs := make([]streampb.StreamEvent_KV, 0)
		receivedDelRangeSpans := make([]roachpb.Span, 0)
		f, err := span.MakeFrontier(spans...)
		require.NoError(t, err)
		for f.Frontier().Less(ts) {
			source.mu.Lock()
			source.mu.rows.Next()
			source.mu.codec.decode()
			if codec.e.Checkpoint != nil {
				for _, rs := range codec.e.Checkpoint.ResolvedSpans {
					_, err := f.Forward(rs.Span, rs.Timestamp)
					if err != nil {
						source.mu.Unlock()
					}
					require.NoError(t, err)
					t.Logf("%s current frontier %s", timeutil.Now(), f.Frontier())
				}
			} else if codec.e.Batch != nil {
				receivedKVs = append(receivedKVs, codec.e.Batch.KVs...)
				for _, dr := range codec.e.Batch.DelRanges {
					receivedDelRangeSpans = append(receivedDelRangeSpans, dr.Span)
				}
			}
			source.mu.Unlock()
		}
		t.Logf("consumer done")
		return receivedKVs, receivedDelRangeSpans
	}

	t1Span, t2Span, t3Span := h.TableSpan(srcTenant.Codec, "t1"),
		h.TableSpan(srcTenant.Codec, "t2"), h.TableSpan(srcTenant.Codec, "t3")
	// Range deleted is outside the subscribed spans
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t2Span.EndKey, t3Span.Key))
	// Range is t1s - t2e, emitting 2 events, t1s - t1e and t2s - t2e.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.Key, t2Span.EndKey))
	// Range is t1e - t2sn, emitting t2s - t2sn.
	require.NoError(t, h.SysServer.DB().DelRangeUsingTombstone(ctx, t1Span.EndKey, t2Span.Key.Next()))

	// NB: We won't see this in our normalized form.
	// {Key: t2Span.Key, EndKey: t2Span.Key.Next()},
	expectedDelRanges := []roachpb.Span{t1Span, t2Span}

	_, actualDelRangeSpans := consumeUntilTimestamp(h.SysServer.Clock().Now())
	require.Equal(t, expectedDelRanges, normalizeRangeKeys(actualDelRangeSpans))

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

	// Using same batch ts so that this SST can be emitted through rangefeed.
	_, _, _, err := h.SysServer.DB().AddSSTableAtBatchTimestamp(ctx, start, end, data, false,
		hlc.Timestamp{}, nil, false, batchHLCTime)
	require.NoError(t, err)

	receivedKVs, receivedDelRangeSpans := consumeUntilTimestamp(batchHLCTime)
	require.Equal(t, t2Span.Key, receivedKVs[0].KeyValue.Key)
	require.True(t, batchHLCTime.LessEq(receivedKVs[0].KeyValue.Value.Timestamp))
	require.Equal(t, expectedDelRanges, normalizeRangeKeys(receivedDelRangeSpans))
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
		func() {
			// This codeblock is wrapped in an anonymous function to ensure the source
			// gets unlocked if an assertion fails. Else, the test can hang.
			source.mu.Lock()
			defer source.mu.Unlock()
			require.True(t, source.mu.rows.Next())
			source.mu.codec.decode()
			if codec.e.Batch != nil {
				require.Greater(t, len(codec.e.Batch.SpanConfigs), 0, "a non empty batch had zero span config updates")
				for _, cfg := range codec.e.Batch.SpanConfigs {
					if receivedSpanConfigs.maybeAddNewRecord(cfg.SpanConfig, cfg.Timestamp.WallTime) {
						updateCount++
					}
				}
			}
			if codec.e.Checkpoint != nil {
				require.Equal(t, 1, len(codec.e.Checkpoint.ResolvedSpans))
				// The frontier in the checkpoint must be greater or equal to the commit
				// timestamp associated with the latest event.
				require.LessOrEqual(t, receivedSpanConfigs.latestTime, codec.e.Checkpoint.ResolvedSpans[0].Timestamp.WallTime)
			}
		}()
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
