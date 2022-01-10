// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestReadProbe(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled by default", func(t *testing.T) {
		m := &mock{
			t:      t,
			noPlan: true,
		}
		p := initTestProber(ctx, m)
		require.False(t, p.readProbeImpl(ctx, m, m, m))

		require.Zero(t, p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		m := &mock{t: t, read: true}
		p := initTestProber(ctx, m)
		require.True(t, p.readProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("happy path with bypass cluster setting overridden", func(t *testing.T) {
		m := &mock{t: t, bypass: true, read: true}
		p := initTestProber(ctx, m)
		require.True(t, p.readProbeImpl(ctx, m, m, m))

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
		require.True(t, p.readProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().ReadProbeAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().ReadProbeFailures.Count())
	})

	t.Run("txn fails", func(t *testing.T) {
		m := &mock{
			t:      t,
			read:   true,
			txnErr: fmt.Errorf("inject txn failure"),
		}
		p := initTestProber(ctx, m)
		require.True(t, p.readProbeImpl(ctx, m, m, m))

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
		require.True(t, p.readProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().ReadProbeFailures.Count())
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
		require.False(t, p.writeProbeImpl(ctx, m, m, m))

		require.Zero(t, p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		m := &mock{t: t, write: true}
		p := initTestProber(ctx, m)
		require.True(t, p.writeProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("happy path with bypass cluster setting overridden", func(t *testing.T) {
		m := &mock{t: t, bypass: true, write: true}
		p := initTestProber(ctx, m)
		require.True(t, p.writeProbeImpl(ctx, m, m, m))

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
		require.True(t, p.writeProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics().WriteProbeAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().ProbePlanFailures.Count())
		require.Zero(t, p.Metrics().WriteProbeFailures.Count())
	})

	t.Run("open txn fails", func(t *testing.T) {
		m := &mock{
			t:      t,
			write:  true,
			txnErr: fmt.Errorf("inject txn failure"),
		}
		p := initTestProber(ctx, m)
		require.True(t, p.writeProbeImpl(ctx, m, m, m))

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
		require.True(t, p.writeProbeImpl(ctx, m, m, m))

		require.Equal(t, int64(1), p.Metrics().ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeAttempts.Count())
		require.Zero(t, p.Metrics().ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics().WriteProbeFailures.Count())
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
	bypassAdmissionControl.Override(ctx, &p.settings.SV, m.bypass)
	p.readPlanner = m
	return p
}

type mock struct {
	t *testing.T

	bypass bool

	noPlan  bool
	planErr error

	read     bool
	write    bool
	readErr  error
	writeErr error
	txnErr   error
}

func (m *mock) next(ctx context.Context) (Step, error) {
	if m.noPlan {
		m.t.Error("plan call made but not expected")
	}
	return Step{}, m.planErr
}

func (m *mock) Read(key interface{}) func(context.Context, *kv.Txn) error {
	return func(context.Context, *kv.Txn) error {
		if !m.read {
			m.t.Error("read call made but not expected")
		}
		return m.readErr
	}
}

func (m *mock) Write(key interface{}) func(context.Context, *kv.Txn) error {
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
