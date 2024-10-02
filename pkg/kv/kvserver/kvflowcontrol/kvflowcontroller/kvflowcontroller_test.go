// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowcontroller

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestFlowTokenAdjustment(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var (
		ctx         = context.Background()
		controller  *Controller
		adjustments []adjustment
		stream      = kvflowcontrol.Stream{
			TenantID: roachpb.SystemTenantID,
			StoreID:  roachpb.StoreID(1),
		}
	)

	datadriven.RunTest(t, "testdata/flow_token_adjustment",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				controller = New(
					metric.NewRegistry(),
					cluster.MakeTestingClusterSettings(),
					hlc.NewClockForTesting(nil),
				)
				adjustments = nil
				return ""

			case "adjust":
				require.NotNilf(t, controller, "uninitialized flow controller (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 2, "expected form 'class={regular,elastic} delta={+,-}<size>")

					var delta kvflowcontrol.Tokens
					var pri admissionpb.WorkPriority

					// Parse class={regular,elastic}.
					parts[0] = strings.TrimSpace(parts[0])
					require.True(t, strings.HasPrefix(parts[0], "class="))
					parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "class=")
					switch parts[0] {
					case "regular":
						pri = admissionpb.NormalPri
					case "elastic":
						pri = admissionpb.BulkNormalPri
					}

					// Parse delta={+,-}<size>
					parts[1] = strings.TrimSpace(parts[1])
					require.True(t, strings.HasPrefix(parts[1], "delta="))
					parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "delta=")
					require.True(t, strings.HasPrefix(parts[1], "+") || strings.HasPrefix(parts[1], "-"))
					isPositive := strings.Contains(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "+")
					parts[1] = strings.TrimPrefix(parts[1], "-")
					bytes, err := humanize.ParseBytes(parts[1])
					require.NoError(t, err)
					delta = kvflowcontrol.Tokens(int64(bytes))
					if !isPositive {
						delta = -delta
					}

					controller.adjustTokens(ctx, pri, delta, stream)
					adjustments = append(adjustments, adjustment{
						pri:   pri,
						delta: delta,
						post:  controller.getTokensForStream(stream),
					})
				}
				return ""

			case "history":
				limit := controller.testingGetLimit()

				var buf strings.Builder
				buf.WriteString("                   regular |  elastic\n")
				buf.WriteString(fmt.Sprintf("                  %8s | %8s\n",
					printTrimmedTokens(limit.regular),
					printTrimmedTokens(limit.elastic),
				))
				buf.WriteString("======================================\n")
				for _, h := range adjustments {
					buf.WriteString(fmt.Sprintf("%s\n", h))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

type adjustment struct {
	pri   admissionpb.WorkPriority
	delta kvflowcontrol.Tokens
	post  tokensPerWorkClass
}

func printTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.ReplaceAll(t.String(), " ", "")
}

func (h adjustment) String() string {
	class := admissionpb.WorkClassFromPri(h.pri)

	comment := ""
	if h.post.regular <= 0 {
		comment = "regular"
	}
	if h.post.elastic <= 0 {
		if len(comment) == 0 {
			comment = "elastic"
		} else {
			comment = "regular and elastic"
		}
	}
	if len(comment) != 0 {
		comment = fmt.Sprintf(" (%s blocked)", comment)
	}
	return fmt.Sprintf("%8s %7s  %8s | %8s%s",
		printTrimmedTokens(h.delta),
		class,
		printTrimmedTokens(h.post.regular),
		printTrimmedTokens(h.post.elastic),
		comment,
	)
}

func TestBucket(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := timeutil.NewManualTime(timeutil.Unix(10, 0))
	limit := tokensPerWorkClass{
		regular: 10,
		elastic: 5,
	}
	b := newBucket(limit, clock.Now())
	stopWaitCh := make(chan struct{}, 1)
	// No waiting for regular work.
	state, waited := b.wait(ctx, admissionpb.RegularWorkClass, stopWaitCh)
	require.Equal(t, waitSuccess, state)
	require.False(t, waited)
	// No waiting for elastic work.
	state, waited = b.wait(ctx, admissionpb.ElasticWorkClass, stopWaitCh)
	require.Equal(t, waitSuccess, state)
	require.False(t, waited)

	clock.Advance(10 * time.Second)
	// regular: 4 tokens, elastic -1 tokens.
	adj, unaccounted := b.adjust(
		ctx, admissionpb.RegularWorkClass, -6, limit, false, clock.Now())
	require.Equal(t, tokensPerWorkClass{
		regular: -6,
		elastic: -6,
	}, adj)
	require.Equal(t, tokensPerWorkClass{}, unaccounted)
	// stopWaitCh has entry.
	stopWaitCh <- struct{}{}
	// No waiting for regular work.
	state, waited = b.wait(ctx, admissionpb.RegularWorkClass, stopWaitCh)
	require.Equal(t, waitSuccess, state)
	require.False(t, waited)
	// Elastic work returns since stopWaitCh has entry.
	state, waited = b.wait(ctx, admissionpb.ElasticWorkClass, stopWaitCh)
	require.Equal(t, stopWaitSignaled, state)
	require.True(t, waited)

	canceledCtx, cancelFunc := context.WithCancel(ctx)
	cancelFunc()
	// No waiting for regular work.
	state, waited = b.wait(canceledCtx, admissionpb.RegularWorkClass, stopWaitCh)
	require.Equal(t, waitSuccess, state)
	require.False(t, waited)
	// Elastic work returns since context is canceled.
	state, waited = b.wait(canceledCtx, admissionpb.ElasticWorkClass, stopWaitCh)
	require.Equal(t, contextCanceled, state)
	require.True(t, waited)

	clock.Advance(10 * time.Second)
	// regular: 0 tokens, elastic -5 tokens.
	adj, unaccounted = b.adjust(
		ctx, admissionpb.RegularWorkClass, -4, limit, false, clock.Now())
	require.Equal(t, tokensPerWorkClass{
		regular: -4,
		elastic: -4,
	}, adj)
	require.Equal(t, tokensPerWorkClass{}, unaccounted)
	require.Equal(t, kvflowcontrol.Tokens(0), b.tokens(admissionpb.RegularWorkClass))
	require.Equal(t, kvflowcontrol.Tokens(-5), b.tokens(admissionpb.ElasticWorkClass))

	// Regular work waits. Have two waiting work requests to test the signal
	// chaining.
	workAdmitted := make(chan struct{}, 2)
	go func() {
		state, waited := b.wait(ctx, admissionpb.RegularWorkClass, stopWaitCh)
		require.Equal(t, waitSuccess, state)
		require.True(t, waited)
		workAdmitted <- struct{}{}
	}()
	go func() {
		state, waited := b.wait(ctx, admissionpb.RegularWorkClass, stopWaitCh)
		require.Equal(t, waitSuccess, state)
		require.True(t, waited)
		workAdmitted <- struct{}{}
	}()
	// Sleep until they have started waiting.
	time.Sleep(time.Second)
	select {
	case <-workAdmitted:
		log.Fatalf(ctx, "should not be admitted")
	default:
	}
	clock.Advance(10 * time.Second)
	adj, unaccounted = b.adjust(
		ctx, admissionpb.RegularWorkClass, +10, limit, false, clock.Now())
	require.Equal(t, tokensPerWorkClass{
		regular: 10,
		elastic: 10,
	}, adj)
	require.Equal(t, tokensPerWorkClass{}, unaccounted)
	// Sleep until they have stopped waiting.
	time.Sleep(time.Second)
	select {
	case <-workAdmitted:
	default:
		log.Fatalf(ctx, "should be admitted")
	}
	select {
	case <-workAdmitted:
	default:
		log.Fatalf(ctx, "should be admitted")
	}

	// Deduct 2 from elastic tokens
	adj, unaccounted = b.adjust(
		ctx, admissionpb.ElasticWorkClass, -2, limit, false, clock.Now())
	require.Equal(t, tokensPerWorkClass{
		regular: 0,
		elastic: -2,
	}, adj)
	require.Equal(t, tokensPerWorkClass{}, unaccounted)

	regularStats, elasticStats := b.getAndResetStats(clock.Now())
	require.Equal(t, deltaStats{
		noTokenDuration: 10 * time.Second,
		tokensDeducted:  10,
	}, regularStats)
	require.Equal(t, deltaStats{
		noTokenDuration: 20 * time.Second,
		tokensDeducted:  12,
	}, elasticStats)
}

// TestBucketSignalingBug triggers a bug in the code prior to
// https://github.com/cockroachdb/cockroach/pull/111088. The bug was due to
// using a shared `signalCh chan struct{}` inside the bucket struct for both
// the work classes. This could result in a situation where an elastic request
// consumed the entry in the channel that was added when the regular tokens
// became greater than zero. This would prevent a waiting regular request from
// getting unblocked.
func TestBucketSignalingBug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, 10)
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, 5)
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)
	controller := New(
		metric.NewRegistry(),
		st,
		hlc.NewClockForTesting(nil),
	)
	stream := kvflowcontrol.Stream{
		TenantID: roachpb.MustMakeTenantID(1),
		StoreID:  1,
	}
	controller.DeductTokens(ctx, admissionpb.NormalPri, 10, stream)
	controller.DeductTokens(ctx, admissionpb.BulkNormalPri, 10, stream)
	streamState := controller.InspectStream(ctx, stream)
	require.Equal(t, int64(0), streamState.AvailableEvalRegularTokens)
	require.Equal(t, int64(-15), streamState.AvailableEvalElasticTokens)

	connectedStream := &mockConnectedStream{
		stream: stream,
	}

	// Elastic work waits first.
	lowPriAdmitted := make(chan struct{})
	go func() {
		admitted, err := controller.Admit(ctx, admissionpb.BulkNormalPri, time.Time{}, connectedStream)
		require.Equal(t, true, admitted)
		require.NoError(t, err)
		lowPriAdmitted <- struct{}{}
	}()
	// Sleep until it has started waiting.
	time.Sleep(time.Second)
	select {
	case <-lowPriAdmitted:
		log.Fatalf(ctx, "low-pri should not be admitted")
	default:
	}
	// Regular work waits second.
	normalPriAdmitted := make(chan struct{})
	go func() {
		admitted, err := controller.Admit(ctx, admissionpb.NormalPri, time.Time{}, connectedStream)
		require.Equal(t, true, admitted)
		require.NoError(t, err)
		normalPriAdmitted <- struct{}{}
	}()
	// Sleep until it has started waiting.
	time.Sleep(time.Second)
	select {
	case <-normalPriAdmitted:
		log.Fatalf(ctx, "normal-pri should not be admitted")
	default:
	}

	controller.ReturnTokens(ctx, admissionpb.NormalPri, 1, stream)
	streamState = controller.InspectStream(ctx, stream)
	require.Equal(t, int64(1), streamState.AvailableEvalRegularTokens)
	require.Equal(t, int64(-14), streamState.AvailableEvalElasticTokens)

	// Sleep to give enough time for regular work to get admitted.
	time.Sleep(2 * time.Second)

	select {
	case <-normalPriAdmitted:
	default:
		// Bug: fails here since the entry in bucket.signalCh has been taken by
		// the elastic work.
		log.Fatalf(ctx, "normal-pri should be admitted")
	}
	// Return enough tokens that the elastic work gets admitted.
	controller.ReturnTokens(ctx, admissionpb.NormalPri, 9, stream)
	streamState = controller.InspectStream(ctx, stream)
	require.Equal(t, int64(10), streamState.AvailableEvalRegularTokens)
	require.Equal(t, int64(-5), streamState.AvailableEvalElasticTokens)

	controller.ReturnTokens(ctx, admissionpb.BulkNormalPri, 7, stream)
	streamState = controller.InspectStream(ctx, stream)
	require.Equal(t, int64(10), streamState.AvailableEvalRegularTokens)
	require.Equal(t, int64(2), streamState.AvailableEvalElasticTokens)
	<-lowPriAdmitted
}

// TestInspectController tests the Inspect() API.
func TestInspectController(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	makeStream := func(id uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(id),
			StoreID:  roachpb.StoreID(id),
		}
	}
	makeInspectStream := func(id uint64, availableElastic, availableRegular int64) kvflowinspectpb.Stream {
		return kvflowinspectpb.Stream{
			TenantID:                   roachpb.MustMakeTenantID(id),
			StoreID:                    roachpb.StoreID(id),
			AvailableEvalRegularTokens: availableRegular,
			AvailableEvalElasticTokens: availableElastic,
		}
	}
	makeConnectedStream := func(id uint64) kvflowcontrol.ConnectedStream {
		return &mockConnectedStream{
			stream: makeStream(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, 8<<20 /* 8 MiB */)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, 16<<20 /* 16 MiB */)
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)
	controller := New(metric.NewRegistry(), st, hlc.NewClockForTesting(nil))

	// No streams connected -- inspect state should be empty.
	require.Len(t, controller.Inspect(ctx), 0)

	// Set up a single connected stream, s1/t1, and ensure it shows up in the
	// Inspect() state.
	admitted, err := controller.Admit(ctx, admissionpb.NormalPri, time.Time{}, makeConnectedStream(1))
	require.NoError(t, err)
	require.True(t, admitted)
	require.Len(t, controller.Inspect(ctx), 1)
	require.Equal(t, controller.Inspect(ctx)[0],
		makeInspectStream(1, 8<<20 /* 8MiB */, 16<<20 /* 16 MiB */))

	// Deduct some {regular,elastic} tokens from s1/t1 and verify that Inspect()
	// renders the state correctly.
	controller.DeductTokens(ctx, admissionpb.NormalPri, kvflowcontrol.Tokens(1<<20 /* 1 MiB */), makeStream(1))
	controller.DeductTokens(ctx, admissionpb.BulkNormalPri, kvflowcontrol.Tokens(2<<20 /* 2 MiB */), makeStream(1))
	require.Len(t, controller.Inspect(ctx), 1)
	require.Equal(t, controller.Inspect(ctx)[0],
		makeInspectStream(1, 5<<20 /* 8 MiB - 2 MiB - 1 MiB = 5 MiB */, 15<<20 /* 16 MiB - 1 MiB = 15 MiB */))

	// Connect another stream, s1/s2, and ensure it shows up in the Inspect()
	// state.
	admitted, err = controller.Admit(ctx, admissionpb.BulkNormalPri, time.Time{}, makeConnectedStream(2))
	require.NoError(t, err)
	require.True(t, admitted)
	require.Len(t, controller.Inspect(ctx), 2)
	require.Equal(t, controller.Inspect(ctx)[1],
		makeInspectStream(2, 8<<20 /* 8MiB */, 16<<20 /* 16 MiB */))
}

func TestControllerLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	// Causes every call to retrieve the elastic metric to log.
	prevVModule := log.GetVModule()
	_ = log.SetVModule("kvflowcontroller_metrics=2")
	defer func() { _ = log.SetVModule(prevVModule) }()
	defer s.Close(t)

	ctx := context.Background()
	testStartTs := timeutil.Now()

	makeStream := func(id uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(id),
			StoreID:  roachpb.StoreID(id),
		}
	}
	makeConnectedStream := func(id uint64) kvflowcontrol.ConnectedStream {
		return &mockConnectedStream{
			stream: makeStream(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	const numTokens = 1 << 20 /* 1 MiB */
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, numTokens)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, numTokens)
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)
	controller := New(metric.NewRegistry(), st, hlc.NewClockForTesting(nil))

	numBlocked := 0
	createStreamAndExhaustTokens := func(id uint64, checkMetric bool) {
		admitted, err := controller.Admit(
			ctx, admissionpb.NormalPri, time.Time{}, makeConnectedStream(id))
		require.NoError(t, err)
		require.True(t, admitted)
		controller.DeductTokens(
			ctx, admissionpb.BulkNormalPri, kvflowcontrol.Tokens(numTokens), makeStream(id))
		controller.DeductTokens(
			ctx, admissionpb.NormalPri, kvflowcontrol.Tokens(numTokens), makeStream(id))
		if checkMetric {
			// This first call will also log.
			require.Equal(t, int64(numBlocked+1), controller.metrics.BlockedStreamCount[elastic].Value())
			require.Equal(t, int64(numBlocked+1), controller.metrics.BlockedStreamCount[regular].Value())
		}
		numBlocked++
	}
	// 1 stream that is blocked.
	id := uint64(1)
	createStreamAndExhaustTokens(id, true)
	// Total 24 streams are blocked.
	for id++; id < 25; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 25th stream will also be blocked. The detailed stats will only cover an
	// arbitrary subset of 20 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	// Total 104 streams are blocked.
	for id++; id < 105; id++ {
		createStreamAndExhaustTokens(id, false)
	}
	// 105th stream will also be blocked. The blocked stream names will only
	// list 100 streams.
	log.Infof(ctx, "creating stream id %d", id)
	createStreamAndExhaustTokens(id, true)

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 2000,
		regexp.MustCompile(`kvflowcontroller_metrics\.go|kvflowcontroller_test\.go`),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)
	/*
		Log output is:

		stream t1/s1 was blocked: durations: regular 16.083µs elastic 17.875µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		1 blocked elastic replication stream(s): t1/s1
		1 blocked regular replication stream(s): t1/s1
		creating stream id 25
		stream t18/s18 was blocked: durations: regular 39.708µs elastic 40.208µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t2/s2 was blocked: durations: regular 113.75µs elastic 219.792µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t14/s14 was blocked: durations: regular 77.625µs elastic 78.167µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t24/s24 was blocked: durations: regular 56.166µs elastic 56.708µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t12/s12 was blocked: durations: regular 107.792µs elastic 108.334µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t3/s3 was blocked: durations: regular 150.916µs elastic 151.5µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t10/s10 was blocked: durations: regular 134.167µs elastic 134.667µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t17/s17 was blocked: durations: regular 121.583µs elastic 122.125µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t4/s4 was blocked: durations: regular 178.417µs elastic 179µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t16/s16 was blocked: durations: regular 146.667µs elastic 147.208µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t1/s1 was blocked: durations: regular 809.833µs elastic 809.833µs tokens deducted: regular 0 B elastic 0 B
		stream t20/s20 was blocked: durations: regular 154.083µs elastic 154.625µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t7/s7 was blocked: durations: regular 209.208µs elastic 209.708µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t13/s13 was blocked: durations: regular 199µs elastic 199.583µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t21/s21 was blocked: durations: regular 182.667µs elastic 183.25µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t23/s23 was blocked: durations: regular 188.042µs elastic 188.583µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t5/s5 was blocked: durations: regular 263.75µs elastic 264.375µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t8/s8 was blocked: durations: regular 262µs elastic 262.541µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t19/s19 was blocked: durations: regular 233.334µs elastic 233.875µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t22/s22 was blocked: durations: regular 235µs elastic 235.542µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		skipped logging some streams that were blocked
		25 blocked elastic replication stream(s): t18/s18, t2/s2, t14/s14, t24/s24, t12/s12, t3/s3, t10/s10, t17/s17, t4/s4, t16/s16, t1/s1, t20/s20, t7/s7, t13/s13, t21/s21, t23/s23, t5/s5, t8/s8, t19/s19, t22/s22, t25/s25, t11/s11, t6/s6, t9/s9, t15/s15
		25 blocked regular replication stream(s): t14/s14, t24/s24, t12/s12, t3/s3, t18/s18, t2/s2, t4/s4, t16/s16, t1/s1, t20/s20, t7/s7, t13/s13, t10/s10, t17/s17, t21/s21, t23/s23, t19/s19, t22/s22, t25/s25, t11/s11, t6/s6, t5/s5, t8/s8, t9/s9, t15/s15
		creating stream id 105
		stream t104/s104 was blocked: durations: regular 14.416µs elastic 14.916µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t3/s3 was blocked: durations: regular 535.75µs elastic 535.75µs tokens deducted: regular 0 B elastic 0 B
		stream t90/s90 was blocked: durations: regular 76.25µs elastic 76.834µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t1/s1 was blocked: durations: regular 500.667µs elastic 500.667µs tokens deducted: regular 0 B elastic 0 B
		stream t105/s105 was blocked: durations: regular 43.708µs elastic 44.416µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t54/s54 was blocked: durations: regular 239.542µs elastic 240.083µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t96/s96 was blocked: durations: regular 100.708µs elastic 101.25µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t97/s97 was blocked: durations: regular 109.416µs elastic 110µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t68/s68 was blocked: durations: regular 222.417µs elastic 223µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t45/s45 was blocked: durations: regular 320.25µs elastic 320.792µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t94/s94 was blocked: durations: regular 153.167µs elastic 153.75µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t65/s65 was blocked: durations: regular 266.916µs elastic 267.458µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t14/s14 was blocked: durations: regular 690.875µs elastic 690.875µs tokens deducted: regular 0 B elastic 0 B
		stream t8/s8 was blocked: durations: regular 547.167µs elastic 547.167µs tokens deducted: regular 0 B elastic 0 B
		stream t6/s6 was blocked: durations: regular 517.875µs elastic 517.875µs tokens deducted: regular 0 B elastic 0 B
		stream t55/s55 was blocked: durations: regular 360.583µs elastic 361.166µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		stream t5/s5 was blocked: durations: regular 600.042µs elastic 600.042µs tokens deducted: regular 0 B elastic 0 B
		stream t15/s15 was blocked: durations: regular 566.875µs elastic 566.875µs tokens deducted: regular 0 B elastic 0 B
		stream t13/s13 was blocked: durations: regular 674.459µs elastic 674.459µs tokens deducted: regular 0 B elastic 0 B
		stream t32/s32 was blocked: durations: regular 517µs elastic 517.542µs tokens deducted: regular 1.0 MiB elastic 2.0 MiB
		skipped logging some streams that were blocked
		105 blocked elastic replication stream(s): t104/s104, t3/s3, t90/s90, t1/s1, t105/s105, t54/s54, t96/s96, t97/s97, t68/s68, t45/s45, t94/s94, t65/s65, t14/s14, t8/s8, t6/s6, t55/s55, t5/s5, t15/s15, t13/s13, t32/s32, t79/s79, t20/s20, t11/s11, t24/s24, t48/s48, t84/s84, t99/s99, t46/s46, t37/s37, t12/s12, t26/s26, t44/s44, t75/s75, t21/s21, t49/s49, t82/s82, t91/s91, t81/s81, t51/s51, t74/s74, t22/s22, t19/s19, t25/s25, t83/s83, t47/s47, t58/s58, t76/s76, t9/s9, t103/s103, t33/s33, t62/s62, t38/s38, t95/s95, t52/s52, t85/s85, t53/s53, t72/s72, t102/s102, t10/s10, t59/s59, t73/s73, t42/s42, t66/s66, t29/s29, t92/s92, t100/s100, t27/s27, t86/s86, t2/s2, t63/s63, t43/s43, t101/s101, t69/s69, t39/s39, t28/s28, t36/s36, t35/s35, t93/s93, t87/s87, t57/s57, t56/s56, t98/s98, t41/s41, t50/s50, t78/s78, t88/s88, t89/s89, t4/s4, t60/s60, t70/s70, t67/s67, t23/s23, t71/s71, t34/s34, t16/s16, t64/s64, t17/s17, t40/s40, t7/s7, t61/s61 omitted some due to overflow
		105 blocked regular replication stream(s): t11/s11, t84/s84, t24/s24, t48/s48, t99/s99, t37/s37, t12/s12, t46/s46, t21/s21, t49/s49, t82/s82, t26/s26, t44/s44, t75/s75, t91/s91, t51/s51, t81/s81, t74/s74, t25/s25, t83/s83, t47/s47, t22/s22, t19/s19, t58/s58, t76/s76, t9/s9, t103/s103, t62/s62, t33/s33, t52/s52, t85/s85, t38/s38, t95/s95, t10/s10, t59/s59, t53/s53, t72/s72, t102/s102, t66/s66, t29/s29, t73/s73, t42/s42, t100/s100, t27/s27, t92/s92, t2/s2, t86/s86, t43/s43, t101/s101, t69/s69, t63/s63, t28/s28, t36/s36, t35/s35, t39/s39, t87/s87, t93/s93, t57/s57, t56/s56, t41/s41, t98/s98, t78/s78, t88/s88, t89/s89, t4/s4, t50/s50, t60/s60, t70/s70, t67/s67, t34/s34, t16/s16, t64/s64, t17/s17, t23/s23, t71/s71, t7/s7, t61/s61, t40/s40, t18/s18, t30/s30, t31/s31, t77/s77, t80/s80, t104/s104, t3/s3, t90/s90, t1/s1, t105/s105, t97/s97, t68/s68, t54/s54, t96/s96, t65/s65, t45/s45, t94/s94, t6/s6, t14/s14, t8/s8, t15/s15, t13/s13 omitted some due to overflow
	*/

	blockedStreamRegexp, err := regexp.Compile(
		"stream .* was blocked: durations: regular .* elastic .* tokens deducted: regular .* elastic .*")
	require.NoError(t, err)
	blockedStreamSkippedRegexp, err := regexp.Compile(
		"skipped logging some streams that were blocked")
	require.NoError(t, err)
	const blockedCountElasticRegexp = "%d blocked elastic replication stream"
	const blockedCountRegularRegexp = "%d blocked regular replication stream"
	blocked1ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 1))
	require.NoError(t, err)
	blocked1RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 1))
	require.NoError(t, err)
	blocked25ElasticRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountElasticRegexp, 25))
	require.NoError(t, err)
	blocked25RegularRegexp, err := regexp.Compile(fmt.Sprintf(blockedCountRegularRegexp, 25))
	require.NoError(t, err)
	blocked105ElasticRegexp, err := regexp.Compile(
		"105 blocked elastic replication stream.* omitted some due to overflow")
	require.NoError(t, err)
	blocked105RegularRegexp, err := regexp.Compile(
		"105 blocked regular replication stream.* omitted some due to overflow")
	require.NoError(t, err)

	const creatingRegexp = "creating stream id %d"
	creating25Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 25))
	require.NoError(t, err)
	creating105Regexp, err := regexp.Compile(fmt.Sprintf(creatingRegexp, 105))
	require.NoError(t, err)

	blockedStreamCount := 0
	foundBlockedElastic := false
	foundBlockedRegular := false
	foundBlockedStreamSkipped := false
	// First section of the log where 1 stream blocked. Entries are in reverse
	// chronological order.
	index := len(entries) - 1
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating25Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked1ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked1RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 1, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.False(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Second section of the log where 25 streams blocked.
	for ; index >= 0; index-- {
		entry := entries[index]
		if creating105Regexp.MatchString(entry.Message) {
			break
		}
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked25ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked25RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.True(t, foundBlockedStreamSkipped)

	blockedStreamCount = 0
	foundBlockedElastic = false
	foundBlockedRegular = false
	// Third section of the log where 105 streams blocked.
	for ; index >= 0; index-- {
		entry := entries[index]
		if blockedStreamRegexp.MatchString(entry.Message) {
			blockedStreamCount++
		}
		if blocked105ElasticRegexp.MatchString(entry.Message) {
			foundBlockedElastic = true
		}
		if blocked105RegularRegexp.MatchString(entry.Message) {
			foundBlockedRegular = true
		}
		if blockedStreamSkippedRegexp.MatchString(entry.Message) {
			foundBlockedStreamSkipped = true
		}
	}
	require.Equal(t, 20, blockedStreamCount)
	require.True(t, foundBlockedElastic)
	require.True(t, foundBlockedRegular)
	require.True(t, foundBlockedStreamSkipped)
}

type mockConnectedStream struct {
	stream kvflowcontrol.Stream
}

var _ kvflowcontrol.ConnectedStream = &mockConnectedStream{}

func (m *mockConnectedStream) Stream() kvflowcontrol.Stream {
	return m.stream
}

func (m *mockConnectedStream) Disconnected() <-chan struct{} {
	return nil
}

func BenchmarkController(b *testing.B) {
	ctx := context.Background()
	makeStream := func(id uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(id),
			StoreID:  roachpb.StoreID(id),
		}
	}
	makeConnectedStream := func(id uint64) kvflowcontrol.ConnectedStream {
		return &mockConnectedStream{
			stream: makeStream(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.ElasticTokensPerStream.Override(ctx, &st.SV, 8<<20 /* 8 MiB */)
	kvflowcontrol.RegularTokensPerStream.Override(ctx, &st.SV, 16<<20 /* 16 MiB */)
	controller := New(metric.NewRegistry(), st, hlc.NewClockForTesting(nil))

	// Deduct some {regular,elastic} tokens from s1/t1 and verify that Inspect()
	// renders the state correctly.
	t1s1 := makeStream(1)
	ct1s1 := makeConnectedStream(1)

	for i := 0; i < b.N; i++ {
		_, _ = controller.Admit(ctx, admissionpb.NormalPri, time.Time{}, ct1s1)
		controller.DeductTokens(ctx, admissionpb.NormalPri, kvflowcontrol.Tokens(1 /* 1b */), t1s1)
		controller.ReturnTokens(ctx, admissionpb.NormalPri, kvflowcontrol.Tokens(1 /* 1b */), t1s1)
	}
}
