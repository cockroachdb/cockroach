// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package kvserver

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type noopRaftSchedulerForDequeuePacer struct{}

func (n noopRaftSchedulerForDequeuePacer) EnqueueRaftRequest(id roachpb.RangeID) {}

func TestRaftReceiveQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
		Name:     mon.MakeName("test"),
		CurCount: g,
		Settings: st,
	})
	qs := raftReceiveQueues{mon: m, dequeuePacer: newStoreRaftDequeuePacer(
		noopEngineForDequeuePacer{}, noopRaftSchedulerForDequeuePacer{}, st)}

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
		require.Equal(t, n1, q1.memoryUsedForTesting())
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
		require.Zero(t, q1.memoryUsedForTesting())
	}

	// Append a first element (again).
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.True(t, shouldQ)
		require.True(t, appended)
		require.Equal(t, n1, q1.memoryUsedForTesting())
	}

	// Add a second element.
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, shouldQ) // not first entry in queue
		require.True(t, appended)
		require.Equal(t, 2*n1, q1.memoryUsedForTesting())
	}

	// Now interleave creation of a second queue.
	q5, loaded := qs.LoadOrCreate(r5, 1 /* maxLen */)
	{
		require.False(t, loaded)
		require.Zero(t, q5.memoryUsedForTesting())
		shouldQ, _, appended := q5.Append(e5, nil /* stream */)
		require.True(t, appended)
		require.True(t, shouldQ)

		// No accidental misattribution of bytes between the queues.
		require.Equal(t, 2*n1, q1.memoryUsedForTesting())
		require.Equal(t, n5, q5.memoryUsedForTesting())
	}

	// Delete the queue. Post deletion, even if someone still has a handle
	// to the deleted queue, the queue is empty and refuses appends. In other
	// words, we're not going to leak requests into abandoned queues.
	{
		qs.Delete(r1)
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, appended)
		require.False(t, shouldQ)
		require.Zero(t, q1.memoryUsedForTesting())
		require.Equal(t, n5, q5.memoryUsedForTesting()) // we didn't touch q5
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
	stopper, _, _, cfg.StorePool, _, _ = storepool.CreateTestStorePool(ctx, cfg.Settings,
		liveness.TestTimeUntilNodeDead, false, /* deterministic */
		func() int { return 0 }, /* nodeCount */
		livenesspb.NodeLivenessStatus_DEAD)
	defer stopper.Stop(ctx)

	// Create a noop store.
	node := roachpb.NodeDescriptor{NodeID: roachpb.NodeID(1)}
	eng := kvstorage.MakeEngines(storage.NewDefaultInMemForTesting())
	stopper.AddCloser(&eng)
	cfg.Transport = NewDummyRaftTransport(cfg.AmbientCtx, cfg.Settings, cfg.Clock)
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

func TestRaftReceiveQueuesEnforceMaxLenConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderDuress(t, "slow test")
	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
		Name:     mon.MakeName("test"),
		CurCount: g,
		Settings: st,
	})
	qs := raftReceiveQueues{mon: m, dequeuePacer: newStoreRaftDequeuePacer(
		noopEngineForDequeuePacer{}, noopRaftSchedulerForDequeuePacer{}, st)}
	// checkingMu is locked in write mode when checking that the values of
	// enforceMaxLen across all the queues is the expected value.
	var checkingMu syncutil.RWMutex
	// doneCh is used to tell the goroutines to stop, and wg is used to wait
	// until they are done.
	doneCh := make(chan struct{})
	var wg sync.WaitGroup
	// Spin up goroutines to create queues.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			rng, _ := randutil.NewTestRand()
			defer wg.Done()
			// Loop until the doneCh is closed.
			for {
				checkingMu.RLock()
				// Most queues will be newly created due to lack of collision in the
				// random numbers. Newly created queues have their enforceMaxLen set
				// in LoadOrCreate.
				qs.LoadOrCreate(roachpb.RangeID(rng.Int63()), 10)
				checkingMu.RUnlock()
				select {
				case <-doneCh:
					return
				default:
				}
			}
		}()
	}
	// Iterate and set different values of enforceMaxLen.
	enforceMaxLen := false
	for i := 0; i < 25; i++ {
		// NB: SetEnforceMaxLen runs concurrently with LoadOrCreate calls.
		qs.SetEnforceMaxLen(enforceMaxLen)
		// Exclude all LoadOrCreate calls while checking is happening.
		checkingMu.Lock()
		qs.m.Range(func(_ roachpb.RangeID, q *raftReceiveQueue) bool {
			require.Equal(t, enforceMaxLen, q.getEnforceMaxLenForTesting())
			return true
		})
		checkingMu.Unlock()
		enforceMaxLen = !enforceMaxLen
		// Do a tiny sleep after releasing the write lock. This gives some
		// opportunity for a bunch of goroutines to acquire the read lock, so when
		// this code loops around to call SetEnforceMaxLen, there is a higher
		// chance of encountering concurrent LoadOrCreate.
		time.Sleep(time.Millisecond)
	}
	close(doneCh)
	wg.Wait()
}

func TestRaftReceiveQueuesEnforceMaxLen(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
		Name:     mon.MakeName("test"),
		CurCount: g,
		Settings: st,
	})
	qs := raftReceiveQueues{mon: m, dequeuePacer: newStoreRaftDequeuePacer(
		noopEngineForDequeuePacer{}, noopRaftSchedulerForDequeuePacer{}, st)}
	qs.SetEnforceMaxLen(false)
	q, _ := qs.LoadOrCreate(1, 2)
	var s RaftMessageResponseStream
	for i := 0; i < 10; i++ {
		_, _, appended := q.Append(&kvserverpb.RaftMessageRequest{}, s)
		require.True(t, appended)
	}
	qs.SetEnforceMaxLen(true)
	_, _, appended := q.Append(&kvserverpb.RaftMessageRequest{}, s)
	require.False(t, appended)
}

func decisionStr(d raftDequeueDecision) string {
	switch d {
	case raftDequeueAll:
		return "all"
	case raftDequeueSkipPaused:
		return "skip-paused"
	}
	panic("unknown decision")
}

// TestRaftReceiveDequeue verifies raftReceiveQueue's enqueueing and selective
// dequeueing behavior. It does not test storeRaftDequeuePacer itself.
func TestRaftReceiveDequeue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
		Name:     mon.MakeName("test"),
		CurCount: g,
		Settings: st,
	})
	pacer := newStoreRaftDequeuePacer(noopEngineForDequeuePacer{}, noopRaftSchedulerForDequeuePacer{}, st)
	pacer.testingKnobs.pauseCh = make(chan *raftReceiveQueue, 100)
	pacer.testingKnobs.drainingCh = make(chan *raftReceiveQueue, 100)
	pacer.testingKnobs.deregisterCh = make(chan *raftReceiveQueue, 100)
	// NB: we don't start the pacer since we are not testing the behavior of its
	// goroutine.

	qs := raftReceiveQueues{mon: m, dequeuePacer: pacer}
	qs.SetEnforceMaxLen(false)

	const r1 = roachpb.RangeID(1)
	var q *raftReceiveQueue

	// ma1 is a MsgApp.
	ma1 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{
		Type: raftpb.MsgApp,
		Entries: []raftpb.Entry{
			{Data: []byte("foo bar baz")}}}}
	sizeMA1 := int64(ma1.Size())
	// ma2 is a MsgApp with LowPriorityOverride.
	ma2 := &kvserverpb.RaftMessageRequest{
		Message: raftpb.Message{
			Type: raftpb.MsgApp,
			Entries: []raftpb.Entry{
				{Data: []byte("xxxxlxlxlxlxllxlxlxlxlxxlxllxlxlxlxlxl")}}},
		LowPriorityOverride: true,
	}
	sizeMA2 := int64(ma2.Size())
	// o1 is some other message type.
	o1 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{
		Type: raftpb.MsgHeartbeat,
	}}
	sizeO1 := int64(o1.Size())
	o2 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{
		Type: raftpb.MsgVote,
	}}
	sizeO2 := int64(o2.Size())
	type msgAndSize struct {
		msg  *kvserverpb.RaftMessageRequest
		size int64
	}
	msgsByName := map[string]msgAndSize{}
	msgsByName["ma1"] = msgAndSize{msg: ma1, size: sizeMA1}
	msgsByName["ma2"] = msgAndSize{msg: ma2, size: sizeMA2}
	msgsByName["o1"] = msgAndSize{msg: o1, size: sizeO1}
	msgsByName["o2"] = msgAndSize{msg: o2, size: sizeO2}
	msgsByPointer := map[*kvserverpb.RaftMessageRequest]string{}
	msgsByPointer[ma1] = "ma1"
	msgsByPointer[ma2] = "ma2"
	msgsByPointer[o1] = "o1"
	msgsByPointer[o2] = "o2"
	printMsgSliceAndSize := func(t *testing.T, infos []raftRequestInfo) (_ string, totalSize int64) {
		var b strings.Builder
		fmt.Fprintf(&b, "[")
		for i, info := range infos {
			if i > 0 {
				fmt.Fprintf(&b, " ")
			}
			name, ok := msgsByPointer[info.req]
			if !ok {
				t.Fatalf("unknown msg")
			}
			fmt.Fprintf(&b, "%s", name)
			require.Equal(t, infos[i].size, msgsByName[name].size)
			totalSize += msgsByName[name].size
		}
		fmt.Fprintf(&b, "]")
		return b.String(), totalSize
	}
	countAndEmptyCh := func(ch chan *raftReceiveQueue) (count int) {
		for {
			select {
			case <-ch:
				count++
			default:
				return count
			}
		}
	}
	var b strings.Builder
	builderStr := func(t *testing.T) string {
		if count := countAndEmptyCh(pacer.testingKnobs.pauseCh); count > 0 {
			fmt.Fprintf(&b, "pacer.Pause calls %d\n", count)
		}
		if count := countAndEmptyCh(pacer.testingKnobs.drainingCh); count > 0 {
			fmt.Fprintf(&b, "pacer.DrainingMsgApps calls %d\n", count)
		}
		if count := countAndEmptyCh(pacer.testingKnobs.deregisterCh); count > 0 {
			fmt.Fprintf(&b, "pacer.Deregister calls %d\n", count)
		}
		fmt.Fprintf(&b, "queue state:\n")
		q.mu.Lock()
		if q.mu.destroyed {
			fmt.Fprintf(&b, " destroyed\n")
		}
		msgsStr, size := printMsgSliceAndSize(t, q.mu.normalMsgStream)
		require.Equal(t, size, q.mu.normalMsgStreamAcc.Used())
		fmt.Fprintf(&b, " normal-stream: %s\n", msgsStr)
		msgsStr, size = printMsgSliceAndSize(t, q.mu.pausedMsgStream)
		require.Equal(t, size, q.mu.pausedMsgStreamAcc.Used())
		fmt.Fprintf(&b, " paused-stream: %s\n", msgsStr)
		fmt.Fprintf(&b, " queuedAtRaftScheduler: %t\n", q.mu.queuedAtRaftScheduler)
		fmt.Fprintf(&b, " pacer: decision: %s, accountingBytes: %d\n",
			decisionStr(q.mu.pacerDequeueDecision), q.mu.pacerDequeueAccountingBytes)
		q.mu.Unlock()
		str := b.String()
		b.Reset()
		return str
	}
	var lastDrain []raftRequestInfo
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "raft_receive_dequeue"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				q, _ = qs.LoadOrCreate(r1, 10 /* maxLen */)
				return builderStr(t)

			case "append":
				var msgStr string
				d.ScanArgs(t, "msg", &msgStr)
				msg, ok := msgsByName[msgStr]
				if !ok {
					t.Fatalf("unknown msg %s", msgStr)
				}
				shouldQ, size, appended := q.Append(msg.msg, nil /* stream */)
				fmt.Fprintf(&b, "shouldQ=%t size=%d appended=%t\n", shouldQ, size, appended)
				return builderStr(t)

			case "drain":
				var ok bool
				lastDrain, ok = q.Drain()
				require.Equal(t, ok, len(lastDrain) > 0)
				msgsStr, _ := printMsgSliceAndSize(t, lastDrain)
				fmt.Fprintf(&b, "drained msgs: %s\n", msgsStr)
				return builderStr(t)

			case "recycle":
				q.Recycle(lastDrain)
				lastDrain = nil
				return builderStr(t)

			case "destroy":
				qs.Delete(r1)
				return builderStr(t)

			case "next-should-pause-says-yes":
				pacer.mu.Lock()
				// Fake the pacer into thinking that it already has queues waiting.
				pacer.mu.waiting[&raftReceiveQueue{}] = struct{}{}
				pacer.mu.Unlock()
				return ""

			case "transition-to-dequeue-all":
				q.mu.Lock()
				q.mu.pacerDequeueDecision = raftDequeueAll
				// Fix up the pacer state so invariant checking doesn't panic.
				pacer.mu.Lock()
				delete(pacer.mu.waiting, q)
				pacer.mu.Unlock()
				q.mu.Unlock()
				return builderStr(t)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type testRaftScheduler struct {
	b *strings.Builder
}

func (t *testRaftScheduler) EnqueueRaftRequest(rangeID roachpb.RangeID) {
	fmt.Fprintf(t.b, "raftScheduler.EnqueueRaftRequest(r%d)\n", rangeID)
}

type testEngineForDequeuePacer struct {
	b                     *strings.Builder
	returnValueWhenNoWait bool
	allowedBurst          int64
	startWaitCh           chan struct{}
	endWaitCh             chan struct{}
	endedWaitCh           chan struct{}
}

func (t *testEngineForDequeuePacer) TryWaitForMemTableStallHeadroom(
	doWait bool,
) (ok bool, allowedBurst int64) {
	if !doWait {
		if t.returnValueWhenNoWait {
			allowedBurst = t.allowedBurst
		}
		fmt.Fprintf(t.b,
			"engine.TryWaitForMemTableStallHeadroom(doWait=false) => ok=%t, allowedBurst=%d\n",
			t.returnValueWhenNoWait, allowedBurst)
		return t.returnValueWhenNoWait, allowedBurst
	}
	fmt.Fprintf(t.b, "start engine.TryWaitForMemTableStallHeadroom(doWait=true)\n")
	t.startWaitCh <- struct{}{}
	<-t.endWaitCh
	fmt.Fprintf(t.b,
		"end engine.TryWaitForMemTableStallHeadroom(doWait=true) => ok=true, allowedBurst=%d\n",
		t.allowedBurst)
	t.endedWaitCh <- struct{}{}
	return true, t.allowedBurst
}

func TestStoreRaftDequeuePacer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var now time.Duration
	crtimeNow := func() crtime.Mono {
		return crtime.Mono(now)
	}
	var scheduler *testRaftScheduler
	var eng *testEngineForDequeuePacer
	var p *storeRaftDequeuePacer
	var qs *raftReceiveQueues
	var b strings.Builder
	var st *cluster.Settings
	init := func() {
		b.Reset()
		scheduler = &testRaftScheduler{b: &b}
		eng = &testEngineForDequeuePacer{
			b:                     &b,
			returnValueWhenNoWait: true,
			allowedBurst:          100,
			startWaitCh:           make(chan struct{}),
			endWaitCh:             make(chan struct{}),
			endedWaitCh:           make(chan struct{}),
		}
		st = cluster.MakeTestingClusterSettings()
		p = newStoreRaftDequeuePacer(eng, scheduler, st)
		now = engineHeadroomPollingInterval
		p.nowMono = crtimeNow
		p.testingKnobs.dequeueBurstCh = make(chan struct{})
		g := metric.NewGauge(metric.Metadata{})
		m := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
			Name:     mon.MakeName("test"),
			CurCount: g,
			Settings: st,
		})
		qs = &raftReceiveQueues{mon: m, dequeuePacer: p}
		qs.SetEnforceMaxLen(false)
		p.Start()
	}
	ma1 := &kvserverpb.RaftMessageRequest{
		Message: raftpb.Message{
			Type: raftpb.MsgApp,
			Entries: []raftpb.Entry{
				{Data: []byte("foo bar baz")}}},
		LowPriorityOverride: true,
	}
	sizeMA1 := int64(ma1.Size())
	require.Greater(t, int64(100), sizeMA1)
	ma2 := &kvserverpb.RaftMessageRequest{
		Message: raftpb.Message{
			Type: raftpb.MsgApp,
			Entries: []raftpb.Entry{
				{Data: []byte("xxxxlxlxlxlxllxlxlxlxlxxlxllxlxlxlxlxlxlxlxlxlxlxl")}}},
		LowPriorityOverride: true,
	}
	sizeMA2 := int64(ma2.Size())
	require.Less(t, int64(100), sizeMA2)
	msgsByName := map[string]*kvserverpb.RaftMessageRequest{}
	msgsByName["ma1"] = ma1
	msgsByName["ma2"] = ma2
	printQueueState := func(q *raftReceiveQueue) string {
		q.mu.Lock()
		defer q.mu.Unlock()
		return fmt.Sprintf("r%s %s %d", q.rangeID, decisionStr(q.mu.pacerDequeueDecision),
			q.mu.pacerDequeueAccountingBytes)
	}
	builderStr := func() string {
		var queues []*raftReceiveQueue
		p.mu.Lock()
		queues = slices.AppendSeq(queues[:0], maps.Keys(p.mu.waiting))
		fmt.Fprintf(&b, "pacer state:\n")
		fmt.Fprintf(&b, " lastHeadroomSampleTime: %v\n",
			time.Duration(p.mu.lastHeadroomAvailableSampleTime))
		if p.mu.burstBytesToDrain > 0 {
			fmt.Fprintf(&b, " burstBytesToDrain: %d\n", p.mu.burstBytesToDrain)
		}
		p.mu.Unlock()
		if len(queues) > 0 {
			slices.SortFunc(queues, func(a, b *raftReceiveQueue) int {
				return cmp.Compare(a.rangeID, b.rangeID)
			})
			fmt.Fprintf(&b, " waiting queues (rangeID, decision, accounting-bytes):")
			for _, q := range queues {
				fmt.Fprintf(&b, " (%s)", printQueueState(q))
			}
			fmt.Fprintf(&b, "\n")
		}
		str := b.String()
		b.Reset()
		return str
	}
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "raft_dequeue_pacer"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				init()
				return builderStr()

			case "enqueue":
				var rangeID int
				d.ScanArgs(t, "range", &rangeID)
				q, _ := qs.LoadOrCreate(roachpb.RangeID(rangeID), 10 /* maxLen */)
				var msgStr string
				d.ScanArgs(t, "msg", &msgStr)
				msg, ok := msgsByName[msgStr]
				if !ok {
					t.Fatalf("unknown msg %s", msgStr)
				}
				shouldQ, size, appended := q.Append(msg, nil /* stream */)
				if d.HasArg("wait-for-start-wait") {
					<-eng.startWaitCh
				}
				fmt.Fprintf(&b, "raftReceiveQueue.Append() => shouldQ=%t size=%d appended=%t\n",
					shouldQ, size, appended)
				fmt.Fprintf(&b, "queue state: %s\n", printQueueState(q))
				return builderStr()

			case "drain":
				var rangeID int
				d.ScanArgs(t, "range", &rangeID)
				q, _ := qs.LoadOrCreate(roachpb.RangeID(rangeID), 10 /* maxLen */)
				msgs, _ := q.Drain()
				if d.HasArg("wait-for-start-wait") {
					<-eng.startWaitCh
				}
				fmt.Fprintf(&b, "raftReceiveQueue.Drain() => msgs=%d\n", len(msgs))
				fmt.Fprintf(&b, "queue state: %s\n", printQueueState(q))
				return builderStr()

			case "delete":
				var rangeID int
				d.ScanArgs(t, "range", &rangeID)
				q, _ := qs.LoadOrCreate(roachpb.RangeID(rangeID), 10 /* maxLen */)
				qs.Delete(roachpb.RangeID(rangeID))
				q.mu.Lock()
				require.Equal(t, raftDequeueAll, q.mu.pacerDequeueDecision)
				require.Zero(t, q.mu.pacerDequeueAccountingBytes)
				q.mu.Unlock()
				if d.HasArg("wait-for-start-wait") {
					<-eng.startWaitCh
				}
				fmt.Fprintf(&b, "raftReceiveQueues.Delete()\n")
				fmt.Fprintf(&b, "queue state: %s\n", printQueueState(q))
				return builderStr()

			case "advance-time":
				now += engineHeadroomPollingInterval
				return ""

			case "close":
				p.Close()
				return builderStr()

			case "set-engine-overload":
				var overload bool
				d.ScanArgs(t, "overload", &overload)
				eng.returnValueWhenNoWait = !overload
				return ""

			case "end-wait":
				eng.endWaitCh <- struct{}{}
				<-eng.endedWaitCh
				if !d.HasArg("do-not-wait-for-dequeue-burst") {
					<-p.testingKnobs.dequeueBurstCh
				}
				if d.HasArg("wait-for-start-wait") {
					<-eng.startWaitCh
				}
				return builderStr()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
