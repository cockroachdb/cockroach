// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowhandle_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestHandleAdmit tests the blocking behavior of Handle.Admit():
// - we block until there are flow tokens available;
// - we unblock when streams without flow tokens are disconnected;
// - we unblock when the handle is closed.
func TestHandleAdmit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stream := kvflowcontrol.Stream{TenantID: roachpb.MustMakeTenantID(42), StoreID: roachpb.StoreID(42)}
	pos := func(d uint64) kvflowcontrolpb.RaftLogPosition {
		return kvflowcontrolpb.RaftLogPosition{Term: 1, Index: d}
	}

	for _, tc := range []struct {
		name      string
		unblockFn func(context.Context, kvflowcontrol.Handle)
	}{
		{
			name: "blocks-for-tokens",
			unblockFn: func(ctx context.Context, handle kvflowcontrol.Handle) {
				// Return tokens tied to pos=1 (16MiB worth); the call to
				// .Admit() should unblock.
				handle.ReturnTokensUpto(ctx, admissionpb.NormalPri, pos(1), stream)
			},
		},
		{
			name: "unblocked-when-stream-disconnects",
			unblockFn: func(ctx context.Context, handle kvflowcontrol.Handle) {
				// Disconnect the stream; the call to .Admit() should unblock.
				handle.DisconnectStream(ctx, stream)
			},
		},
		{
			name: "unblocked-when-closed",
			unblockFn: func(ctx context.Context, handle kvflowcontrol.Handle) {
				// Close the handle; the call to .Admit() should unblock.
				handle.Close(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := metric.NewRegistry()
			clock := hlc.NewClockForTesting(nil)
			controller := kvflowcontroller.New(registry, cluster.MakeTestingClusterSettings(), clock)
			handle := kvflowhandle.New(controller, kvflowhandle.NewMetrics(registry), clock)

			// Connect a single stream at pos=0 and deplete all 16MiB of regular
			// tokens at pos=1.
			handle.ConnectStream(ctx, pos(0), stream)
			handle.DeductTokensFor(ctx, admissionpb.NormalPri, time.Time{}, pos(1), kvflowcontrol.Tokens(16<<20 /* 16MiB */))

			// Invoke .Admit() in a separate goroutine, and test below whether
			// the goroutine is blocked.
			admitCh := make(chan struct{})
			go func() {
				require.NoError(t, handle.Admit(ctx, admissionpb.NormalPri, time.Time{}))
				close(admitCh)
			}()

			select {
			case <-admitCh:
				t.Fatalf("unexpectedly admitted")
			case <-time.After(10 * time.Millisecond):
			}

			tc.unblockFn(ctx, handle)

			select {
			case <-admitCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("didn't get admitted")
			}
		})
	}
}

// TestHandleSequencing tests the sequencing behavior of
// Handle.DeductTokensFor(), namely that we:
// - advance sequencing timestamps when the create-time advances;
// - advance sequencing timestamps when the log position advances.
func TestHandleSequencing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// tzero represents the t=0, the earliest possible time. All other
	// create-time=<duration> is relative to this time.
	var tzero = timeutil.Unix(0, 0)

	ctx := context.Background()
	stream := kvflowcontrol.Stream{
		TenantID: roachpb.MustMakeTenantID(42),
		StoreID:  roachpb.StoreID(42),
	}
	pos := func(t, i uint64) kvflowcontrolpb.RaftLogPosition {
		return kvflowcontrolpb.RaftLogPosition{Term: t, Index: i}
	}
	ct := func(d int64) time.Time {
		return tzero.Add(time.Nanosecond * time.Duration(d))
	}

	const tokens = kvflowcontrol.Tokens(1 << 20 /* MiB */)
	const normal = admissionpb.NormalPri

	registry := metric.NewRegistry()
	clock := hlc.NewClockForTesting(nil)
	controller := kvflowcontroller.New(registry, cluster.MakeTestingClusterSettings(), clock)
	handle := kvflowhandle.New(controller, kvflowhandle.NewMetrics(registry), clock)

	// Test setup: handle is connected to a single stream at pos=1/0 and has
	// deducted 1MiB of regular tokens at pos=1 ct=1.
	handle.ConnectStream(ctx, pos(1, 0), stream)
	seq0 := handle.DeductTokensFor(ctx, normal, ct(1), pos(1, 1), tokens)

	// If create-time advances, so does the sequencing timestamp.
	seq1 := handle.DeductTokensFor(ctx, normal, ct(2), pos(1, 1), tokens)
	require.Greater(t, seq1, seq0)

	// If <create-time,log-position> stays static, the sequencing timestamp
	// still advances.
	seq2 := handle.DeductTokensFor(ctx, normal, ct(2), pos(1, 1), tokens)
	require.Greater(t, seq2, seq1)

	// If the log index advances, so does the sequencing timestamp.
	seq3 := handle.DeductTokensFor(ctx, normal, ct(3), pos(1, 2), tokens)
	require.Greater(t, seq3, seq2)

	// If the log term advances, so does the sequencing timestamp.
	seq4 := handle.DeductTokensFor(ctx, normal, ct(3), pos(2, 2), tokens)
	require.Greater(t, seq4, seq3)

	// If both the create-time and log-position advance, so does the sequencing
	// timestamp.
	seq5 := handle.DeductTokensFor(ctx, normal, ct(1000), pos(4, 20), tokens)
	require.Greater(t, seq5, seq4)

	// Verify that the sequencing timestamp is kept close to the maximum
	// observed create-time.
	require.LessOrEqual(t, seq5.Sub(ct(1000)), time.Nanosecond)
}
