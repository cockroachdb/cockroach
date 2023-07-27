// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

type subscriptionFeedSource struct {
	sub streamclient.Subscription
}

var _ replicationtestutils.FeedSource = (*subscriptionFeedSource)(nil)

// Next implements the streamingtest.FeedSource interface.
func (f *subscriptionFeedSource) Next() (streamingccl.Event, bool) {
	event, hasMore := <-f.sub.Events()
	return event, hasMore
}

// Error implements the streamingtest.FeedSource interface.
func (f *subscriptionFeedSource) Error() error {
	return f.sub.Err()
}

// Close implements the streamingtest.FeedSource interface.
func (f *subscriptionFeedSource) Close(ctx context.Context) {}

func TestPartitionedStreamReplicationClient(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h, cleanup := replicationtestutils.NewReplicationHelper(t,
		base.TestServerArgs{
			// Need to disable the test tenant until tenant-level restore is
			// supported. Tracked with #76378.
			DisableDefaultTestTenant: true,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	)

	defer cleanup()

	testTenantName := roachpb.TenantName("test-tenant")
	tenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	ctx := context.Background()
	// Makes sure source cluster producer job does not time out within test timeout
	h.SysSQL.Exec(t, `
SET CLUSTER SETTING stream_replication.job_liveness_timeout = '500s';
`)
	tenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	rng, _ := randutil.NewPseudoRand()
	maybeGenerateInlineURL := func(orig *url.URL) *url.URL {
		if rng.Float64() > 0.5 {
			return orig
		}

		t.Log("using inline certificates")
		ret := *orig
		v := ret.Query()
		for _, opt := range []string{"sslcert", "sslkey", "sslrootcert"} {
			path := v.Get(opt)
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			v.Set(opt, string(content))

		}
		v.Set("sslinline", "true")
		ret.RawQuery = v.Encode()
		return &ret
	}

	maybeInlineURL := maybeGenerateInlineURL(&h.PGUrl)
	client, err := streamclient.NewPartitionedStreamClient(ctx, maybeInlineURL)
	defer func() {
		require.NoError(t, client.Close(ctx))
	}()
	require.NoError(t, err)
	expectStreamState := func(streamID streampb.StreamID, status jobs.Status) {
		h.SysSQL.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", streamID),
			[][]string{{string(status)}})
	}

	rps, err := client.Create(ctx, testTenantName)
	require.NoError(t, err)
	streamID := rps.StreamID
	// We can create multiple replication streams for the same tenant.
	_, err = client.Create(ctx, testTenantName)
	require.NoError(t, err)

	top, err := client.Plan(ctx, streamID)
	require.NoError(t, err)
	require.Equal(t, 1, len(top.Partitions))
	// Plan for a non-existent stream
	_, err = client.Plan(ctx, 999)
	require.True(t, testutils.IsError(err, fmt.Sprintf("job with ID %d does not exist", 999)), err)

	expectStreamState(streamID, jobs.StatusRunning)
	status, err := client.Heartbeat(ctx, streamID, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	require.NoError(t, err)
	require.Equal(t, streampb.StreamReplicationStatus_STREAM_ACTIVE, status.StreamStatus)

	// Pause the underlying producer job of the replication stream
	h.SysSQL.Exec(t, `PAUSE JOB $1`, streamID)
	expectStreamState(streamID, jobs.StatusPaused)
	status, err = client.Heartbeat(ctx, streamID, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	require.NoError(t, err)
	require.Equal(t, streampb.StreamReplicationStatus_STREAM_PAUSED, status.StreamStatus)

	// Cancel the underlying producer job of the replication stream
	h.SysSQL.Exec(t, `CANCEL JOB $1`, streamID)
	expectStreamState(streamID, jobs.StatusCanceled)

	status, err = client.Heartbeat(ctx, streamID, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	require.NoError(t, err)
	require.Equal(t, streampb.StreamReplicationStatus_STREAM_INACTIVE, status.StreamStatus)

	// Non-existent stream is not active in the source cluster.
	status, err = client.Heartbeat(ctx, 999, hlc.Timestamp{WallTime: timeutil.Now().UnixNano()})
	require.NoError(t, err)
	require.Equal(t, streampb.StreamReplicationStatus_STREAM_INACTIVE, status.StreamStatus)

	initialScanTimestamp := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	// Testing client.Subscribe()
	makePartitionSpec := func(tables ...string) *streampb.StreamPartitionSpec {
		var spans []roachpb.Span
		for _, table := range tables {
			desc := desctestutils.TestingGetPublicTableDescriptor(
				h.SysServer.DB(), tenant.Codec, "d", table)
			spans = append(spans, desc.PrimaryIndexSpan(tenant.Codec))
		}

		return &streampb.StreamPartitionSpec{
			InitialScanTimestamp: initialScanTimestamp,
			Spans:                spans,
			Config: streampb.StreamPartitionSpec_ExecutionConfig{
				MinCheckpointFrequency: 10 * time.Millisecond,
			},
		}
	}

	encodeSpec := func(tables ...string) []byte {
		opaqueSpec, err := protoutil.Marshal(makePartitionSpec(tables...))
		require.NoError(t, err)
		return opaqueSpec
	}

	// Ignore table t2 and only subscribe to the changes to table t1.
	require.Equal(t, len(top.Partitions), 1)
	url, err := streamingccl.StreamAddress(top.Partitions[0].SrcAddr).URL()
	require.NoError(t, err)
	// Create a new stream client with the given partition address.
	subClient, err := streamclient.NewPartitionedStreamClient(ctx, url)
	defer func() {
		require.NoError(t, subClient.Close(ctx))
	}()
	require.NoError(t, err)
	sub, err := subClient.Subscribe(ctx, streamID, encodeSpec("t1"),
		initialScanTimestamp, hlc.Timestamp{})
	require.NoError(t, err)

	rf := replicationtestutils.MakeReplicationFeed(t, &subscriptionFeedSource{sub: sub})
	t1Descr := desctestutils.TestingGetPublicTableDescriptor(h.SysServer.DB(), tenant.Codec, "d", "t1")

	ctxWithCancel, cancelFn := context.WithCancel(ctx)
	cg := ctxgroup.WithContext(ctxWithCancel)
	cg.GoCtx(sub.Subscribe)
	// Observe the existing single row in t1.
	expected := replicationtestutils.EncodeKV(t, tenant.Codec, t1Descr, 42)
	firstObserved := rf.ObserveKey(ctx, expected.Key)
	require.Equal(t, expected.Value.RawBytes, firstObserved.Value.RawBytes)
	rf.ObserveResolved(ctx, firstObserved.Value.Timestamp)

	// Updates the existing row.
	tenant.SQL.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
	expected = replicationtestutils.EncodeKV(t, tenant.Codec, t1Descr, 42, nil, "world")

	// Observe its changes.
	secondObserved := rf.ObserveKey(ctx, expected.Key)
	require.Equal(t, expected.Value.RawBytes, secondObserved.Value.RawBytes)
	require.True(t, firstObserved.Value.Timestamp.Less(secondObserved.Value.Timestamp))

	// Test if Subscribe can react to cancellation signal.
	cancelFn()

	// When the context is cancelled, lib/pq sends a query cancellation message to
	// the server. Occasionally, we see the error from this cancellation before
	// the subscribe function sees our local context cancellation.
	err = cg.Wait()
	require.True(t, errors.Is(err, context.Canceled) || isQueryCanceledError(err))

	rf.ObserveError(ctx, func(err error) bool {
		return errors.Is(err, context.Canceled) || isQueryCanceledError(err)
	})

	// Testing client.Complete()
	err = client.Complete(ctx, streampb.StreamID(999), true)
	require.True(t, testutils.IsError(err, "job with ID 999 does not exist"), err)

	// Makes producer job exit quickly.
	h.SysSQL.Exec(t, `
SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '200ms';
`)
	rps, err = client.Create(ctx, testTenantName)
	require.NoError(t, err)
	streamID = rps.StreamID
	require.NoError(t, client.Complete(ctx, streamID, true))
	h.SysSQL.CheckQueryResultsRetry(t,
		fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", streamID), [][]string{{"succeeded"}})
}

// isQueryCanceledError returns true if the error appears to be a query cancelled error.
func isQueryCanceledError(err error) bool {
	var pqErr pq.Error
	if ok := errors.As(err, &pqErr); ok {
		return pqErr.Code == pq.ErrorCode(pgcode.QueryCanceled.String())
	}
	return strings.Contains(err.Error(), cancelchecker.QueryCanceledError.Error())
}
