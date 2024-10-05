// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvserver

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRaftReceiveQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, g,
		nil, math.MaxInt64, st,
	)
	qs := raftReceiveQueues{mon: m}

	const r1 = roachpb.RangeID(1)
	const r5 = roachpb.RangeID(5)

	qs.Load(r1)
	qs.Load(r5)
	require.Zero(t, m.AllocBytes())

	q1, loaded := qs.LoadOrCreate(r1, 10 /* maxLen */)
	require.Zero(t, m.AllocBytes())
	require.False(t, loaded)
	{
		q1x, loadedx := qs.LoadOrCreate(r1, 10 /* maxLen */)
		require.True(t, loadedx)
		require.Equal(t, q1, q1x)
	}
	require.Zero(t, m.AllocBytes())

	e1 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{Entries: []raftpb.Entry{
		{Data: []byte("foo bar baz")}}}}
	e5 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{Entries: []raftpb.Entry{
		{Data: []byte("xxxxlxlxlxlxllxlxlxlxlxxlxllxlxlxlxlxl")}}}}
	n1 := int64(e1.Size())
	n5 := int64(e5.Size())

	// Append an entry.
	{
		shouldQ, size, appended := q1.Append(e1, nil /* stream */)
		require.True(t, appended)
		require.True(t, shouldQ)
		require.Equal(t, n1, size)
		require.Equal(t, n1, q1.acc.Used())
		// NB: the monitor allocates in chunks so it will have allocated more than n1.
		// We don't check these going forward, as we've now verified that they're hooked up.
		require.GreaterOrEqual(t, m.AllocBytes(), n1)
		require.Equal(t, m.AllocBytes(), g.Value())
	}

	{
		sl, ok := q1.Drain()
		require.True(t, ok)
		require.Len(t, sl, 1)
		require.Equal(t, e1, sl[0].req)
		require.Zero(t, q1.acc.Used())
	}

	// Append a first element (again).
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.True(t, shouldQ)
		require.True(t, appended)
		require.Equal(t, n1, q1.acc.Used())
	}

	// Add a second element.
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, shouldQ) // not first entry in queue
		require.True(t, appended)
		require.Equal(t, 2*n1, q1.acc.Used())
	}

	// Now interleave creation of a second queue.
	q5, loaded := qs.LoadOrCreate(r5, 1 /* maxLen */)
	{
		require.False(t, loaded)
		require.Zero(t, q5.acc.Used())
		shouldQ, _, appended := q5.Append(e5, nil /* stream */)
		require.True(t, appended)
		require.True(t, shouldQ)

		// No accidental misattribution of bytes between the queues.
		require.Equal(t, 2*n1, q1.acc.Used())
		require.Equal(t, n5, q5.acc.Used())
	}

	// Delete the queue. Post deletion, even if someone still has a handle
	// to the deleted queue, the queue is empty and refuses appends. In other
	// words, we're not going to leak requests into abandoned queues.
	{
		qs.Delete(r1)
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, appended)
		require.False(t, shouldQ)
		require.Zero(t, q1.acc.Used())
		require.Equal(t, n5, q5.acc.Used()) // we didn't touch q5
	}
}

// TestRaftCrossLocalityMetrics verifies that
// updateCrossLocalityMetricsOn{Incoming|Outgoing}RaftMsg correctly updates
// cross-region, cross-zone byte count metrics for incoming and outgoing raft
// msg.
func TestRaftCrossLocalityMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Unix(0, 123)))
	cfg := TestStoreConfig(clock)
	var stopper *stop.Stopper
	stopper, _, _, cfg.StorePool, _ = storepool.CreateTestStorePool(ctx, cfg.Settings,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 0 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create a noop store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := storage.NewDefaultInMemForTesting()
	stopper.AddCloser(eng)
	cfg.Transport = NewDummyRaftTransport(cfg.Settings, cfg.AmbientCtx.Tracer)
	store := NewStore(ctx, cfg, eng, &node)
	store.Ident = &roachpb.StoreIdent{
		ClusterID: uuid.Nil,
		StoreID:   1,
		NodeID:    1,
	}

	const expectedInc = 10
	metricsNames := []string{
		"raft.rcvd.bytes",
		"raft.rcvd.cross_region.bytes",
		"raft.rcvd.cross_zone.bytes",
		"raft.sent.bytes",
		"raft.sent.cross_region.bytes",
		"raft.sent.cross_zone.bytes"}
	for _, tc := range []struct {
		crossLocalityType    roachpb.LocalityComparisonType
		expectedMetricChange [6]int64
		forRequest           bool
	}{
		{crossLocalityType: roachpb.LocalityComparisonType_CROSS_REGION,
			expectedMetricChange: [6]int64{expectedInc, expectedInc, 0, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			expectedMetricChange: [6]int64{expectedInc, 0, expectedInc, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			expectedMetricChange: [6]int64{expectedInc, 0, 0, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_CROSS_REGION,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, expectedInc, 0},
			forRequest:           false,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, 0, expectedInc},
			forRequest:           false,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, 0, 0},
			forRequest:           false,
		},
	} {
		t.Run(fmt.Sprintf("%-v", tc.crossLocalityType), func(t *testing.T) {
			beforeMetrics, metricsErr := store.metrics.GetStoreMetrics(metricsNames)
			if metricsErr != nil {
				t.Error(metricsErr)
			}
			if tc.forRequest {
				store.Metrics().updateCrossLocalityMetricsOnIncomingRaftMsg(tc.crossLocalityType, expectedInc)
			} else {
				store.Metrics().updateCrossLocalityMetricsOnOutgoingRaftMsg(tc.crossLocalityType, expectedInc)
			}

			afterMetrics, metricsErr := store.metrics.GetStoreMetrics(metricsNames)
			if metricsErr != nil {
				t.Error(metricsErr)
			}
			metricsDiff := getMapsDiff(beforeMetrics, afterMetrics)
			expectedDiff := make(map[string]int64, 6)
			for i, inc := range tc.expectedMetricChange {
				expectedDiff[metricsNames[i]] = inc
			}
			require.Equal(t, metricsDiff, expectedDiff)
		})
	}
}
