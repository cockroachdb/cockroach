// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontroller

import (
	"context"
	"fmt"
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
	regularTokensPerStream.Override(ctx, &st.SV, 10)
	elasticTokensPerStream.Override(ctx, &st.SV, 5)
	kvflowcontrol.Mode.Override(ctx, &st.SV, int64(kvflowcontrol.ApplyToAll))
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
	require.Equal(t, int64(0), streamState.AvailableRegularTokens)
	require.Equal(t, int64(-15), streamState.AvailableElasticTokens)

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
	require.Equal(t, int64(1), streamState.AvailableRegularTokens)
	require.Equal(t, int64(-14), streamState.AvailableElasticTokens)

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
	require.Equal(t, int64(10), streamState.AvailableRegularTokens)
	require.Equal(t, int64(-5), streamState.AvailableElasticTokens)

	controller.ReturnTokens(ctx, admissionpb.BulkNormalPri, 7, stream)
	streamState = controller.InspectStream(ctx, stream)
	require.Equal(t, int64(10), streamState.AvailableRegularTokens)
	require.Equal(t, int64(2), streamState.AvailableElasticTokens)
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
			TenantID:               roachpb.MustMakeTenantID(id),
			StoreID:                roachpb.StoreID(id),
			AvailableElasticTokens: availableElastic,
			AvailableRegularTokens: availableRegular,
		}
	}
	makeConnectedStream := func(id uint64) kvflowcontrol.ConnectedStream {
		return &mockConnectedStream{
			stream: makeStream(id),
		}
	}

	st := cluster.MakeTestingClusterSettings()
	elasticTokensPerStream.Override(ctx, &st.SV, 8<<20 /* 8 MiB */)
	regularTokensPerStream.Override(ctx, &st.SV, 16<<20 /* 16 MiB */)
	kvflowcontrol.Mode.Override(ctx, &st.SV, int64(kvflowcontrol.ApplyToAll))
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
	elasticTokensPerStream.Override(ctx, &st.SV, 8<<20 /* 8 MiB */)
	regularTokensPerStream.Override(ctx, &st.SV, 16<<20 /* 16 MiB */)
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
