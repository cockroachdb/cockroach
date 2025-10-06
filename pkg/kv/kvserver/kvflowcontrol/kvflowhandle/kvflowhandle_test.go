// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowhandle_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
)

// TestHandleAdmit tests the blocking behavior of Handle.Admit():
// - we block until there are flow tokens available;
// - we unblock when streams without flow tokens are disconnected;
// - we unblock when the handle is closed;
// - we unblock when the handle is reset.
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
		{
			name: "unblocked-when-reset",
			unblockFn: func(ctx context.Context, handle kvflowcontrol.Handle) {
				// Reset all streams on the handle; the call to .Admit() should
				// unblock.
				handle.ResetStreams(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := metric.NewRegistry()
			clock := hlc.NewClockForTesting(nil)
			st := cluster.MakeTestingClusterSettings()
			kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
			kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)

			controller := kvflowcontroller.New(registry, st, clock)
			handle := kvflowhandle.New(
				controller,
				kvflowhandle.NewMetrics(registry),
				clock,
				roachpb.RangeID(1),
				roachpb.SystemTenantID,
				nil, /* knobs */
			)

			// Connect a single stream at pos=0 and deplete all 16MiB of regular
			// tokens at pos=1.
			handle.ConnectStream(ctx, pos(0), stream)
			handle.DeductTokensFor(ctx, admissionpb.NormalPri, pos(1), kvflowcontrol.Tokens(16<<20 /* 16MiB */))

			// Invoke .Admit() in a separate goroutine, and test below whether
			// the goroutine is blocked.
			admitCh := make(chan struct{})
			go func() {
				admitted, err := handle.Admit(ctx, admissionpb.NormalPri, time.Time{})
				require.NoError(t, err)
				require.True(t, admitted)
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

// TestFlowControlMode tests the behavior of kvadmission.flow_control.mode.
func TestFlowControlMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stream := kvflowcontrol.Stream{
		TenantID: roachpb.MustMakeTenantID(42),
		StoreID:  roachpb.StoreID(42),
	}
	pos := func(d uint64) kvflowcontrolpb.RaftLogPosition {
		return kvflowcontrolpb.RaftLogPosition{Term: 1, Index: d}
	}

	for _, tc := range []struct {
		mode            kvflowcontrol.ModeT
		blocks, ignores []admissionpb.WorkClass
	}{
		{
			mode: kvflowcontrol.ApplyToElastic,
			blocks: []admissionpb.WorkClass{
				admissionpb.ElasticWorkClass,
			},
			ignores: []admissionpb.WorkClass{
				admissionpb.RegularWorkClass,
			},
		},
		{
			mode: kvflowcontrol.ApplyToAll,
			blocks: []admissionpb.WorkClass{
				admissionpb.ElasticWorkClass, admissionpb.RegularWorkClass,
			},
			ignores: []admissionpb.WorkClass{},
		},
	} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			registry := metric.NewRegistry()
			clock := hlc.NewClockForTesting(nil)
			st := cluster.MakeTestingClusterSettings()
			kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
			kvflowcontrol.Mode.Override(ctx, &st.SV, tc.mode)

			controller := kvflowcontroller.New(registry, st, clock)
			handle := kvflowhandle.New(
				controller,
				kvflowhandle.NewMetrics(registry),
				clock,
				roachpb.RangeID(1),
				roachpb.SystemTenantID,
				nil, /* knobs */
			)
			defer handle.Close(ctx)

			// Connect a single stream at pos=0 and deplete all 16MiB of regular
			// tokens at pos=1. It also puts elastic tokens in the -ve.
			handle.ConnectStream(ctx, pos(0), stream)
			handle.DeductTokensFor(ctx, admissionpb.NormalPri, pos(1), kvflowcontrol.Tokens(16<<20 /* 16MiB */))

			mode := tc.mode // copy to avoid nogo error

			// Invoke .Admit() for {regular,elastic} work in a separate
			// goroutines, and test below whether the goroutines are blocked.
			regularAdmitCh := make(chan struct{})
			elasticAdmitCh := make(chan struct{})
			go func() {
				admitted, err := handle.Admit(ctx, admissionpb.NormalPri, time.Time{})
				require.NoError(t, err)
				if mode == kvflowcontrol.ApplyToElastic {
					require.False(t, admitted)
				} else {
					require.True(t, admitted)
				}
				close(regularAdmitCh)
			}()
			go func() {
				admitted, err := handle.Admit(ctx, admissionpb.BulkNormalPri, time.Time{})
				require.NoError(t, err)
				require.True(t, admitted)
				close(elasticAdmitCh)
			}()

			for _, ignoredClass := range tc.ignores { // work should not block
				classAdmitCh := regularAdmitCh
				if ignoredClass == admissionpb.ElasticWorkClass {
					classAdmitCh = elasticAdmitCh
				}

				select {
				case <-classAdmitCh:
				case <-time.After(5 * time.Second):
					t.Fatalf("%s work didn't get admitted", ignoredClass)
				}
			}

			for _, blockedClass := range tc.blocks { // work should get blocked
				classAdmitCh := regularAdmitCh
				if blockedClass == admissionpb.ElasticWorkClass {
					classAdmitCh = elasticAdmitCh
				}

				select {
				case <-classAdmitCh:
					t.Fatalf("unexpectedly admitted %s work", blockedClass)
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	}
}

// TestInspectHandle tests the Inspect() API.
func TestInspectHandle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	registry := metric.NewRegistry()
	clock := hlc.NewClockForTesting(nil)
	st := cluster.MakeTestingClusterSettings()
	kvflowcontrol.Enabled.Override(ctx, &st.SV, true)
	kvflowcontrol.Mode.Override(ctx, &st.SV, kvflowcontrol.ApplyToAll)

	pos := func(d uint64) kvflowcontrolpb.RaftLogPosition {
		return kvflowcontrolpb.RaftLogPosition{Term: 1, Index: d}
	}
	stream := func(i uint64) kvflowcontrol.Stream {
		return kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(1),
			StoreID:  roachpb.StoreID(i),
		}
	}

	controller := kvflowcontroller.New(registry, st, clock)
	handle := kvflowhandle.New(
		controller,
		kvflowhandle.NewMetrics(registry),
		clock,
		roachpb.RangeID(1),
		roachpb.SystemTenantID,
		nil, /* knobs */
	)
	marshaller := jsonpb.Marshaler{
		Indent:       "  ",
		EmitDefaults: true,
		OrigName:     true,
	}

	var buf strings.Builder
	defer func() { echotest.Require(t, buf.String(), datapathutils.TestDataPath(t, "handle_inspect")) }()

	record := func(header string) {
		if buf.Len() > 0 {
			buf.WriteString("\n\n")
		}
		state := handle.Inspect(ctx)
		marshaled, err := marshaller.MarshalToString(&state)
		require.NoError(t, err)
		buf.WriteString(fmt.Sprintf("# %s\n", header))
		buf.WriteString(marshaled)
	}
	record("No connected streams.")

	handle.ConnectStream(ctx, pos(42), stream(1))
	record("Single connected stream with no tracked deductions.")

	handle.DeductTokensFor(ctx, admissionpb.NormalPri, pos(43), kvflowcontrol.Tokens(1<<20 /* 1 MiB */))
	handle.DeductTokensFor(ctx, admissionpb.BulkNormalPri, pos(44), kvflowcontrol.Tokens(1<<20 /* 1 MiB */))
	record("Single connected stream with one 1MiB tracked deduction per work class.")

	handle.ReturnTokensUpto(ctx, admissionpb.NormalPri, pos(43), stream(1))
	handle.ConnectStream(ctx, pos(45), stream(2))
	handle.ConnectStream(ctx, pos(46), stream(3))
	handle.DeductTokensFor(ctx, admissionpb.BulkNormalPri, pos(47), kvflowcontrol.Tokens(1<<20 /* 1 MiB */))
	record("Triply connected stream with 2MiB, 1MiB, and 1MiB tracked elastic deductions respectively.")

	handle.DisconnectStream(ctx, stream(2))
	record("Doubly connected stream with 2MiB and 1MiB tracked elastic deductions respectively.")

	handle.ResetStreams(ctx)
	record("Doubly connected stream with no tracked deductions.")
}
