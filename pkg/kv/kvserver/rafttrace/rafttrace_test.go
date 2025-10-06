// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttrace

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

func createTracer(count int64) *RaftTracer {
	ctx := context.Background()
	tracer := tracing.NewTracer()
	st := cluster.MakeTestingClusterSettings()
	MaxConcurrentRaftTraces.Override(ctx, &st.SV, count)
	numRegisteredStore := atomic.Int64{}
	return NewRaftTracer(ctx, tracer, st, &numRegisteredStore)
}

func TestRegisterRemote(t *testing.T) {
	rt := createTracer(10)

	te := kvserverpb.TracedEntry{Index: 1, TraceID: 123, SpanID: 456}
	rt.RegisterRemote(te)
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt.numRegisteredReplica.Load())
}

func TestMaybeRegisterNoSpan(t *testing.T) {
	rt := createTracer(10)

	// Test without a span in context
	ctx := context.Background()
	require.False(t, rt.MaybeRegister(ctx, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(0), rt.numRegisteredStore.Load())
	require.Equal(t, int64(0), rt.numRegisteredReplica.Load())
}

func TestMaybeRegisterWithSpan(t *testing.T) {
	rt := createTracer(10)

	ctx := context.Background()
	// Test with a span in the context.
	ctx, span := rt.tracer.StartSpanCtx(ctx, "test-span", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span.Finish()

	require.True(t, rt.MaybeRegister(ctx, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt.numRegisteredReplica.Load())
}

func TestMaybeTraceNoSpan(t *testing.T) {
	rt := createTracer(10)
	ctx := context.Background()

	ent := raftpb.Entry{Index: 1}
	require.False(t, rt.MaybeRegister(ctx, ent))
	require.Empty(t, rt.MaybeTrace(raftpb.Message{Type: raftpb.MsgApp, Entries: []raftpb.Entry{ent}}))
}

func TestMaybeTraceWithSpan(t *testing.T) {
	rt := createTracer(10)
	ctx, span := rt.tracer.StartSpanCtx(context.Background(), "test-span", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span.Finish()

	ent := raftpb.Entry{Index: 1}
	require.True(t, rt.MaybeRegister(ctx, ent))
	tracedEntries := rt.MaybeTrace(raftpb.Message{
		Type:    raftpb.MsgApp,
		Entries: []raftpb.Entry{ent},
	})
	require.Len(t, tracedEntries, 1)
	require.Equal(t, kvpb.RaftIndex(1), tracedEntries[0].Index)
}

func TestClose(t *testing.T) {
	rt := createTracer(10)
	ctx, span := rt.tracer.StartSpanCtx(context.Background(), "test-span", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span.Finish()

	require.True(t, rt.MaybeRegister(ctx, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt.numRegisteredReplica.Load())

	rt.Close()
	require.Equal(t, int64(0), rt.numRegisteredStore.Load())
	require.Greater(t, rt.numRegisteredReplica.Load(), int64(1000))
}

func TestTwoTracersSharingNumRegisteredStore(t *testing.T) {
	numRegisteredStore := atomic.Int64{}
	ctx := context.Background()
	tracer := tracing.NewTracer()
	st := cluster.MakeTestingClusterSettings()
	MaxConcurrentRaftTraces.Override(ctx, &st.SV, 3)

	rt1 := NewRaftTracer(ctx, tracer, st, &numRegisteredStore)
	rt2 := NewRaftTracer(ctx, tracer, st, &numRegisteredStore)

	// Register a trace in the first tracer.
	ctx1, span1 := rt1.tracer.StartSpanCtx(ctx, "test-span-1", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span1.Finish()
	require.True(t, rt1.MaybeRegister(ctx1, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(1), rt1.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt1.numRegisteredReplica.Load())

	// Register a trace in the second tracer.
	ctx2, span2 := rt2.tracer.StartSpanCtx(ctx, "test-span-2", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span2.Finish()
	require.True(t, rt2.MaybeRegister(ctx2, raftpb.Entry{Index: 2}))
	require.Equal(t, int64(2), rt2.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt2.numRegisteredReplica.Load())

	// Ensure both tracers share the same numRegisteredStore.
	require.Equal(t, rt1.numRegisteredStore, rt2.numRegisteredStore)

	// Close the first tracer and check the counts.
	rt1.Close()
	require.Equal(t, int64(1), rt2.numRegisteredStore.Load())
	require.Greater(t, rt1.numRegisteredReplica.Load(), int64(1000))
	require.Equal(t, int64(1), rt2.numRegisteredReplica.Load())

	// Close the second tracer and check the counts.
	rt2.Close()
	require.Equal(t, int64(0), rt2.numRegisteredStore.Load())
	require.Greater(t, rt2.numRegisteredReplica.Load(), int64(1000))
}

func TestLimit(t *testing.T) {
	rt := createTracer(2)
	ctx1, span1 := rt.tracer.StartSpanCtx(context.Background(), "test-span", tracing.WithRecording(tracingpb.RecordingVerbose))
	defer span1.Finish()
	// Only 2 traces are allowed but we attempt to register 3.
	require.True(t, rt.MaybeRegister(ctx1, raftpb.Entry{Index: 1}))
	require.True(t, rt.MaybeRegister(ctx1, raftpb.Entry{Index: 2}))
	require.False(t, rt.MaybeRegister(ctx1, raftpb.Entry{Index: 3}))
	rt.Close()
	require.Equal(t, int64(0), rt.numRegisteredStore.Load())
	require.Greater(t, rt.numRegisteredReplica.Load(), int64(1000))
}

func TestMaybeTraceMsgAppResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	require.True(t, rt.MaybeRegister(ctx, raftpb.Entry{Index: 1}))
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		Term:  1,
		From:  1,
		To:    2,
		Type:  raftpb.MsgAppResp,
		Index: uint64(5),
	}), 0)
	output := finish().String()
	require.NoError(t, testutils.MatchInOrder(output, []string{"1->2 MsgAppResp Term:1 Index:5"}...))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
}

func TestDupeMsgAppResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	ent := raftpb.Entry{Index: 1}
	require.True(t, rt.MaybeRegister(ctx, ent))
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		Term:  1,
		From:  1,
		To:    2,
		Type:  raftpb.MsgAppResp,
		Index: uint64(5),
	}))
	// The second message should not trace.
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		Term:  1,
		From:  1,
		To:    2,
		Type:  raftpb.MsgAppResp,
		Index: uint64(6),
	}))

	output := finish().String()
	require.NoError(t, testutils.MatchInOrder(output, []string{"1->2 MsgAppResp Term:1 Index:5"}...))
	require.Error(t, testutils.MatchInOrder(output, []string{"1->2 MsgAppResp Term:1 Index:6"}...))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
}

func TestTraceMsgStorageAppendResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	ent := raftpb.Entry{Index: 1}
	require.True(t, rt.MaybeRegister(ctx, ent))
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		From:    1,
		To:      2,
		Term:    3,
		Type:    raftpb.MsgStorageAppendResp,
		Index:   uint64(5),
		LogTerm: uint64(4),
	}))

	output := finish().String()
	require.NoError(t, testutils.MatchInOrder(output, []string{"1->2 MsgStorageAppendResp Log:4/5"}...))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
}

func TestDupeMsgStorageAppendResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	ent := raftpb.Entry{Index: 1}
	require.True(t, rt.MaybeRegister(ctx, ent))
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		From:    1,
		To:      2,
		Term:    3,
		Type:    raftpb.MsgStorageAppendResp,
		Index:   uint64(5),
		LogTerm: uint64(4),
	}))
	// The second messsage should not trace.
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		From:    5,
		To:      6,
		Term:    7,
		Type:    raftpb.MsgStorageAppendResp,
		Index:   uint64(8),
		LogTerm: uint64(9),
	}))

	output := finish().String()
	require.NoError(t, testutils.MatchInOrder(output, []string{"1->2 MsgStorageAppendResp Log:4/5"}...))
	require.Error(t, testutils.MatchInOrder(output, []string{"5->6 MsgStorageAppendResp"}...))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
}

func TestNoTraceMsgStorageAppendResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	ent := raftpb.Entry{Index: 10}
	require.True(t, rt.MaybeRegister(ctx, ent))

	// This doesn't trace since the index is behind the entry index.
	require.Empty(t, rt.MaybeTrace(raftpb.Message{
		From:    1,
		To:      2,
		Term:    3,
		Type:    raftpb.MsgStorageAppendResp,
		Index:   uint64(5),
		LogTerm: uint64(4),
	}))

	output := finish().String()
	require.Error(t, testutils.MatchInOrder(output, []string{"MsgStorageAppendResp"}...))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
}

func TestTraceMsgStorageApplyResp(t *testing.T) {
	rt := createTracer(10)
	ctx, finish := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "test")

	require.True(t, rt.MaybeRegister(ctx, raftpb.Entry{Index: 1}))
	require.Empty(t, rt.MaybeTrace(
		raftpb.Message{
			From: 1,
			To:   2,
			Type: raftpb.MsgStorageApplyResp,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 4},
			},
		}))

	output := finish().String()
	require.NoError(t, testutils.MatchInOrder(output,
		[]string{
			`1->2 MsgStorageApplyResp LastEntry:2/4`,
			`unregistered log index`,
		}...))
	require.Equal(t, int64(0), rt.numRegisteredStore.Load())
}

func TestDuplicateIndex(t *testing.T) {
	rt := createTracer(10)
	ctx1, trace1 := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "trace1")
	require.True(t, rt.MaybeRegister(ctx1, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt.numRegisteredReplica.Load())
	// This returns true indicating that the index is registered, but it doesn't
	// add a new registration.
	ctx2, trace2 := tracing.ContextWithRecordingSpan(context.Background(), rt.tracer, "trace2")
	require.True(t, rt.MaybeRegister(ctx2, raftpb.Entry{Index: 1}))
	require.Equal(t, int64(1), rt.numRegisteredStore.Load())
	require.Equal(t, int64(1), rt.numRegisteredReplica.Load())

	// Unregister the entry with a MsgStorageApplyResp.
	require.Empty(t, rt.MaybeTrace(
		raftpb.Message{
			From: 1,
			To:   2,
			Type: raftpb.MsgStorageApplyResp,
			Entries: []raftpb.Entry{
				{Term: 1, Index: 1},
				{Term: 2, Index: 4},
			},
		}))
	// We expect the logs to go to the first trace.
	output1 := trace1().String()
	output2 := trace2().String()
	require.NoError(t, testutils.MatchInOrder(output1,
		[]string{
			`1->2 MsgStorageApplyResp LastEntry:2/4`,
			`unregistered log index`,
		}...))
	require.NoError(t, testutils.MatchInOrder(output1,
		[]string{
			`additional registration for same index`,
		}...))
	require.Error(t, testutils.MatchInOrder(output2,
		[]string{
			`1->2 MsgStorageApplyResp LastEntry:2/4`,
			`unregistered log index`,
		}...))
	require.NoError(t, testutils.MatchInOrder(output2,
		[]string{
			`duplicate registration ignored`,
		}...))

	require.Equal(t, int64(0), rt.numRegisteredStore.Load())
	require.Equal(t, int64(0), rt.numRegisteredReplica.Load())
}
