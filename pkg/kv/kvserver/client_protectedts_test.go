// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestamps is an end-to-end test for protected timestamps.
// It works by writing a lot of data and waiting for the GC heuristic to allow
// for GC. Because of this, it's very slow and expensive. It should
// potentially be made cheaper by injecting hooks to force GC.
// TODO(pavelkalinnikov): use the GCHint for this.
//
// Probably this test should always be skipped until it is made cheaper,
// nevertheless it's a useful test.
func TestProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// This test is too slow to run with race.
	skip.UnderRace(t)
	skip.UnderShort(t)

	args := base.TestClusterArgs{}
	args.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		DisableGCQueue:            true,
		DisableLastProcessedCheck: true,
	}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)
	s0 := tc.Server(0)

	conn := tc.ServerConn(0)
	_, err := conn.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'") // speeds up the test
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'") // speeds up the test
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.enqueue_in_replicate_queue_on_span_config_update.enabled = true") // speeds up the test
	require.NoError(t, err)

	const tableRangeMaxBytes = 64 << 20
	_, err = conn.Exec("ALTER TABLE foo CONFIGURE ZONE USING "+
		"gc.ttlseconds = 1, range_max_bytes = $1, range_min_bytes = 1<<10;", tableRangeMaxBytes)
	require.NoError(t, err)

	rRand, _ := randutil.NewTestRand()
	upsertUntilBackpressure := func() {
		for {
			_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)",
				randutil.RandBytes(rRand, 1<<15))
			if testutils.IsError(err, "backpressure") {
				break
			}
			require.NoError(t, err)
		}
	}
	const processedPattern = `(?s)shouldQueue=true.*processing replica.*GC score after GC`
	processedRegexp := regexp.MustCompile(processedPattern)

	waitForTableSplit := func() {
		testutils.SucceedsSoon(t, func() error {
			count := 0
			if err := conn.QueryRow(
				"SELECT count(*) FROM [SHOW RANGES FROM TABLE foo]").Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				return errors.New("waiting for table split")
			}
			return nil
		})
	}

	getTableStartKey := func() roachpb.Key {
		row := conn.QueryRow(`
SELECT raw_start_key
FROM [SHOW RANGES FROM TABLE foo WITH KEYS]
ORDER BY raw_start_key ASC LIMIT 1`)

		var startKey roachpb.Key
		require.NoError(t, row.Scan(&startKey))
		return startKey
	}

	getTableID := func() descpb.ID {
		var tableID descpb.ID
		require.NoError(t,
			conn.QueryRow(`SELECT id FROM system.namespace WHERE name = 'foo'`).Scan(&tableID))
		return tableID
	}

	getStoreAndReplica := func() (*kvserver.Store, *kvserver.Replica) {
		startKey := getTableStartKey()
		// There's only one server, so there's no point searching for which server
		// the leaseholder is on, it could only be on s0.
		return getFirstStoreReplica(t, s0, startKey)
	}

	waitForRangeMaxBytes := func(maxBytes int64) {
		testutils.SucceedsSoon(t, func() error {
			_, r := getStoreAndReplica()
			if r.GetMaxBytes(ctx) != maxBytes {
				return errors.New("waiting for range_max_bytes to be applied")
			}
			return nil
		})
	}

	gcSoon := func() {
		testutils.SucceedsSoon(t, func() error {
			upsertUntilBackpressure()
			s, repl := getStoreAndReplica()
			traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, s.GetStoreConfig().Tracer(), "trace-enqueue")
			_, err := s.Enqueue(traceCtx, "mvccGC", repl, false /* skipShouldQueue */, false /* async */)
			require.NoError(t, err)
			trace := rec()
			if !processedRegexp.MatchString(trace.String()) {
				return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
			}
			return nil
		})
	}

	thresholdRE := regexp.MustCompile(`(?s).*Threshold:(?P<threshold>[^\s]*)`)
	thresholdFromTrace := func(trace tracingpb.Recording) hlc.Timestamp {
		threshStr := string(thresholdRE.ExpandString(nil, "$threshold",
			trace.String(), thresholdRE.FindStringSubmatchIndex(trace.String())))
		thresh, err := hlc.ParseTimestamp(threshStr)
		require.NoError(t, err)
		return thresh
	}

	waitForTableSplit()
	waitForRangeMaxBytes(tableRangeMaxBytes)

	beforeWrites := s0.Clock().Now()
	gcSoon()

	pts := ptstorage.New(s0.ClusterSettings(), nil)
	ptsWithDB := ptstorage.WithDatabase(pts, s0.InternalDB().(isql.DB))
	ptsRec := ptpb.Record{
		ID:        uuid.MakeV4().GetBytes(),
		Timestamp: s0.Clock().Now(),
		Mode:      ptpb.PROTECT_AFTER,
		Target:    ptpb.MakeSchemaObjectsTarget([]descpb.ID{getTableID()}),
	}
	require.NoError(t, ptsWithDB.Protect(ctx, &ptsRec))
	// Verify that the record did indeed make its way down into KV where the
	// replica can read it from.
	ptsReader := tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().ProtectedTimestampReader
	ptutil.TestingWaitForProtectedTimestampToExistOnSpans(
		ctx, t, s0, ptsReader, ptsRec.Timestamp, ptsRec.DeprecatedSpans,
	)
	upsertUntilBackpressure()
	// We need to be careful choosing a time. We're a little limited because the
	// ttl is defined in seconds and we need to wait for the threshold to be
	// 2x the threshold with the scale factor as time since threshold. The
	// gc threshold we'll be able to set precedes this timestamp which we'll
	// put in the record below.
	afterWrites := s0.Clock().Now().Add(2*time.Second.Nanoseconds(), 0)
	s, repl := getStoreAndReplica()
	// The protectedts record will prevent us from aging the MVCC garbage bytes
	// past the oldest record so shouldQueue should be false. Verify that.
	traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, s.GetStoreConfig().Tracer(), "trace-enqueue")
	_, err = s.Enqueue(traceCtx, "mvccGC", repl, false /* skipShouldQueue */, false /* async */)
	require.NoError(t, err)
	require.Regexp(t, "(?s)shouldQueue=false", rec().String())

	// If we skipShouldQueue then gc will run but it should only run up to the
	// timestamp of our record at the latest.
	traceCtx, rec = tracing.ContextWithRecordingSpan(ctx, s.GetStoreConfig().Tracer(), "trace-enqueue")
	_, err = s.Enqueue(traceCtx, "mvccGC", repl, true /* skipShouldQueue */, false /* async */)
	require.NoError(t, err)
	trace := rec()
	require.Regexp(t, "(?s)handled \\d+ incoming point keys; deleted \\d+", trace.String())
	thresh := thresholdFromTrace(trace)
	require.Truef(t, thresh.Less(ptsRec.Timestamp), "threshold: %v, protected %v %q", thresh, ptsRec.Timestamp, trace)

	// Make a new record that is doomed to fail.
	failedRec := ptsRec
	failedRec.ID = uuid.MakeV4().GetBytes()
	failedRec.Timestamp = beforeWrites
	failedRec.Timestamp.Logical = 0
	require.NoError(t, ptsWithDB.Protect(ctx, &failedRec))
	_, err = ptsWithDB.GetRecord(ctx, failedRec.ID.GetUUID())
	require.NoError(t, err)

	// Verify that the record did indeed make its way down into KV where the
	// replica can read it from. We then verify (below) that the failed record
	// does not affect the ability to GC.
	ptutil.TestingWaitForProtectedTimestampToExistOnSpans(
		ctx, t, s0, ptsReader, failedRec.Timestamp, failedRec.DeprecatedSpans,
	)

	// Add a new record that is after the old record.
	laterRec := ptsRec
	laterRec.ID = uuid.MakeV4().GetBytes()
	laterRec.Timestamp = afterWrites
	laterRec.Timestamp.Logical = 0
	require.NoError(t, ptsWithDB.Protect(ctx, &laterRec))
	ptutil.TestingWaitForProtectedTimestampToExistOnSpans(
		ctx, t, s0, ptsReader, laterRec.Timestamp, laterRec.DeprecatedSpans,
	)

	// Release the record that had succeeded and ensure that GC eventually
	// happens up to the protected timestamp of the new record.
	require.NoError(t, ptsWithDB.Release(ctx, ptsRec.ID.GetUUID()))
	testutils.SucceedsSoon(t, func() error {
		traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, s.GetStoreConfig().Tracer(), "trace-enqueue")
		_, err = s.Enqueue(traceCtx, "mvccGC", repl, false /* skipShouldQueue */, false /* async */)
		require.NoError(t, err)
		trace := rec()
		if !processedRegexp.MatchString(trace.String()) {
			return errors.Errorf("%q does not match %q", trace.String(), processedRegexp)
		}
		thresh := thresholdFromTrace(trace)
		require.Truef(t, ptsRec.Timestamp.Less(thresh), "%v >= %v",
			ptsRec.Timestamp, thresh)
		require.Truef(t, thresh.Less(laterRec.Timestamp), "%v >= %v",
			thresh, laterRec.Timestamp)
		return nil
	})

	// Release the failed record.
	require.NoError(t, ptsWithDB.Release(ctx, failedRec.ID.GetUUID()))
	require.NoError(t, ptsWithDB.Release(ctx, laterRec.ID.GetUUID()))
	state, err := ptsWithDB.GetState(ctx)
	require.NoError(t, err)
	require.Len(t, state.Records, 0)
	require.Equal(t, int(state.NumRecords), len(state.Records))
}
