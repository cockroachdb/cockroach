// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvprober

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestReadProbe(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled by default", func(t *testing.T) {
		m := &mock{
			t:      t,
			noPlan: true,
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Zero(t, p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		m := &mock{t: t, read: true}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("happy path with bypass cluster setting overridden", func(t *testing.T) {
		m := &mock{t: t, bypass: true, read: true}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("planning fails", func(t *testing.T) {
		m := &mock{
			t:       t,
			read:    true,
			planErr: fmt.Errorf("inject plan failure"),
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	// Once a node is fully decommissioned, neither kvclient nor kvprober work from
	// the node. This does not indicate a service health issue; it is expected behavior.
	//
	// This is not tested with an integration test, since the kvclient of a decommissioned
	// node will occasionally return other errors. We choose not to filter those out for
	// reasons given at errorIsExpectedDuringNormalOperation. As a result, an integration test
	// would be flaky. We believe a unit test is sufficient, largely because the main risk
	// in only having a unit test is false positive pages on SRE, due to changes in what errors
	// are returned from the kvclient of a decommissioned node. Though false positive pages add
	// ops load, they do not directly affect the customer experience.
	t.Run("planning fails due to decommissioning but not counted as error", func(t *testing.T) {
		m := &mock{
			t:       t,
			read:    true,
			planErr: kvpb.NewDecommissionedStatusErrorf(codes.PermissionDenied, "foobar"),
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("txn fails", func(t *testing.T) {
		m := &mock{
			t:      t,
			read:   true,
			txnErr: fmt.Errorf("inject txn failure"),
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("read fails", func(t *testing.T) {
		m := &mock{
			t:       t,
			read:    true,
			readErr: fmt.Errorf("inject read failure"),
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeFailures.Count())
	})

	// See comment above matching case in TestReadProbe regarding planning.
	t.Run("read fails due to decommissioning but not counted as error", func(t *testing.T) {
		m := &mock{
			t:       t,
			read:    true,
			readErr: kvpb.NewDecommissionedStatusErrorf(codes.PermissionDenied, "foobar"),
		}
		p := initTestProber(ctx, m)
		p.readProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})
}

func TestWriteProbe(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled by default", func(t *testing.T) {
		m := &mock{
			t:      t,
			noPlan: true,
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Zero(t, p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		m := &mock{t: t, write: true}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("happy path with bypass cluster setting overridden", func(t *testing.T) {
		m := &mock{t: t, bypass: true, write: true}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("planning fails", func(t *testing.T) {
		m := &mock{
			t:       t,
			write:   true,
			planErr: fmt.Errorf("inject plan failure"),
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	// See comment above matching case in TestReadProbe regarding planning.
	t.Run("planning fails due to decommissioning but not counted as error", func(t *testing.T) {
		m := &mock{
			t:       t,
			write:   true,
			planErr: kvpb.NewDecommissionedStatusErrorf(codes.PermissionDenied, "foobar"),
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("open txn fails", func(t *testing.T) {
		m := &mock{
			t:      t,
			write:  true,
			txnErr: fmt.Errorf("inject txn failure"),
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("write fails", func(t *testing.T) {
		m := &mock{
			t:        t,
			write:    true,
			writeErr: fmt.Errorf("inject write failure"),
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeFailures.Count())
	})

	// See comment above matching case in TestReadProbe regarding planning.
	t.Run("write fails due to decommissioning but not counted as error", func(t *testing.T) {
		m := &mock{
			t:        t,
			write:    true,
			writeErr: kvpb.NewDecommissionedStatusErrorf(codes.PermissionDenied, "foobar"),
		}
		p := initTestProber(ctx, m)
		p.writeProbeImpl(ctx, m, m, m)

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})
}

func TestReturnLeaseholderInfo(t *testing.T) {

	ctx := context.Background()

	t.Run("leaseholder information is returned correctly", func(t *testing.T) {

		// Mock recording with 2 spans. Line "[n1] node received request" contains the leaseholder information.
		var mockRecording tracingpb.Recording = []tracingpb.RecordedSpan{
			{
				TraceID:      001,
				SpanID:       002,
				ParentSpanID: 003,
				Operation:    "test probe 1",
				TagGroups:    nil,
				StartTime:    time.Now(),
				Duration:     1,
				Logs: []tracingpb.LogRecord{{Time: time.Now(), Message: "querying next range at /Table/9"},
					{Time: time.Now(), Message: "key: /Table/9, desc: r13:‹/Table/{9-11}› [(n1,s1):1, next=2, gen=0]"},
					{Time: time.Now(), Message: "r13: sending batch ‹1 Get› to (n1,s1):1"},
					{Time: time.Now(), Message: "sending request to ‹localhost:26257›"}},
			},
			{
				TraceID:      004,
				SpanID:       005,
				ParentSpanID: 006,
				Operation:    "test probe 2",
				TagGroups:    nil,
				StartTime:    time.Now(),
				Duration:     2,
				Logs: []tracingpb.LogRecord{{Time: time.Now(), Message: "[n1] node received request: ‹1 Get›"},
					{Time: time.Now(), Message: "[n1,s1,r13/1:‹/Table/{9-11}›] read-only path"},
					{Time: time.Now(), Message: "[n1] node sending response"}},
			},
		}

		m := &mock{t: t, read: true}
		p := initTestProber(ctx, m)
		// Expected leaseholder information is node 1.
		require.Equal(t, "1", string(p.returnLeaseholderInfo(mockRecording)))
	})

	t.Run("traces do not contain leaseholder information", func(t *testing.T) {
		// Mock recording without any leaseholder information.
		var mockRecording tracingpb.Recording = []tracingpb.RecordedSpan{
			{
				TraceID:      001,
				SpanID:       002,
				ParentSpanID: 003,
				Operation:    "test probe 1",
				TagGroups:    nil,
				StartTime:    time.Now(),
				Duration:     1,
				Logs: []tracingpb.LogRecord{{Time: time.Now(), Message: "querying next range at /Table/9"},
					{Time: time.Now(), Message: "key: /Table/9, desc: r13:‹/Table/{9-11}› [(n1,s1):1, next=2, gen=0]"},
					{Time: time.Now(), Message: "r13: sending batch ‹1 Get› to (n1,s1):1"},
					{Time: time.Now(), Message: "sending request to ‹localhost:26257›"}},
				RecordingMode:              0,
				StructuredRecords:          nil,
				GoroutineID:                0,
				Finished:                   false,
				StructuredRecordsSizeBytes: 0,
				ChildrenMetadata:           nil,
			},
		}

		m := &mock{t: t, read: true}
		p := initTestProber(ctx, m)
		// Since no leaseholder information is present, the function is expected to return an empty string.
		require.Equal(t, "", string(p.returnLeaseholderInfo(mockRecording)))
	})
}

func initTestProber(ctx context.Context, m *mock) *Prober {
	p := NewProber(Opts{
		Tracer:                  tracing.NewTracer(),
		HistogramWindowInterval: time.Minute, // actual value not important to test
		Settings:                cluster.MakeTestingClusterSettings(),
	})
	readEnabled.Override(ctx, &p.settings.SV, m.read)
	writeEnabled.Override(ctx, &p.settings.SV, m.write)
	quarantineWriteEnabled.Override(ctx, &p.settings.SV, m.qWrite)
	bypassAdmissionControl.Override(ctx, &p.settings.SV, m.bypass)
	p.readPlanner = m
	return p
}

type mock struct {
	t *testing.T

	bypass bool

	noPlan     bool
	emptyQPool bool
	planErr    error

	read     bool
	write    bool
	qWrite   bool
	readErr  error
	writeErr error
	txnErr   error
}

func (m *mock) next(ctx context.Context) (Step, error) {
	step := Step{}
	if m.noPlan {
		m.t.Error("plan call made but not expected")
	}
	if !m.emptyQPool {
		step = Step{
			RangeID: 1,
			Key:     keys.LocalMax,
		}
	}
	return step, m.planErr
}

func (m *mock) Read(key roachpb.Key) func(context.Context, *kv.Txn) error {
	return func(context.Context, *kv.Txn) error {
		if !m.read {
			m.t.Error("read call made but not expected")
		}
		return m.readErr
	}
}

func (m *mock) Write(key roachpb.Key) func(context.Context, *kv.Txn) error {
	return func(context.Context, *kv.Txn) error {
		if !m.write {
			m.t.Error("write call made but not expected")
		}
		return m.writeErr
	}
}

func (m *mock) Txn(ctx context.Context, f func(ctx context.Context, txn *kv.Txn) error) error {
	if !m.bypass {
		m.t.Error("normal txn used but bypass is not set")
	}
	if m.txnErr != nil {
		return m.txnErr
	}
	return f(ctx, &kv.Txn{})
}

func (m *mock) TxnRootKV(
	ctx context.Context, f func(ctx context.Context, txn *kv.Txn) error,
) error {
	if m.bypass {
		m.t.Error("root kv txn used but bypass is set")
	}
	if m.txnErr != nil {
		return m.txnErr
	}
	return f(ctx, &kv.Txn{})
}
