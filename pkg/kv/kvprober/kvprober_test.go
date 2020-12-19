package kvprober

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

// TODO(josh): If I have some extra time, would folks want me to look into
// setting up gomock? We use it a lot in CC; it is quite nice; I think it makes
// for more readable tests than ones that use a custom mock. I see this issue:
// https://github.com/cockroachdb/cockroach/issues/6933
func TestProbe(t *testing.T) {
	ctx := context.Background()

	p := NewProber(ProberOpts{
		AmbientCtx: log.AmbientContext{
			Tracer:  tracing.NewTracer(),
		},
		HistogramWindowInterval: time.Minute, // actual value not important to test
		DB: nil,
		Settings: cluster.MakeTestingClusterSettings(),
	})
	p.Planner = nil

	t.Run("want default disabled", func(t *testing.T) {
		m := &mock{
			t: t,
			noPlan: true,
			noGet: true,
		}
		p.Planner = m
		clearMetrics(p)

		p.probe(ctx, m)

		require.Zero(t, p.Metrics.ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics.ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
	})

	t.Run("happy path", func(t *testing.T) {
		ReadEnabled.Override(&p.settings.SV, true)
		m := &mock{t: t}
		p.Planner = m
		clearMetrics(p)

		p.probe(ctx, m)

		require.Equal(t, int64(1), p.Metrics.ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics.ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
	})

	t.Run("planning fails", func(t *testing.T) {
		ReadEnabled.Override(&p.settings.SV, true)
		m := &mock{
			t: t,
			planErr: fmt.Errorf("inject plan failure"),
			noGet: true,
		}
		p.Planner = m
		clearMetrics(p)

		p.probe(ctx, m)

		require.Equal(t, int64(1), p.Metrics.ProbePlanAttempts.Count())
		require.Zero(t, p.Metrics.ReadProbeAttempts.Count())
		require.Equal(t, int64(1), p.Metrics.ProbePlanFailures.Count())
		require.Zero(t, p.Metrics.ReadProbeFailures.Count())
	})

	t.Run("get fails", func(t *testing.T) {
		ReadEnabled.Override(&p.settings.SV, true)
		m := &mock{
			t: t,
			getErr: fmt.Errorf("inject get failure"),
		}
		p.Planner = m
		clearMetrics(p)

		p.probe(ctx, m)

		require.Equal(t, int64(1), p.Metrics.ProbePlanAttempts.Count())
		require.Equal(t, int64(1), p.Metrics.ReadProbeAttempts.Count())
		require.Zero(t, p.Metrics.ProbePlanFailures.Count())
		require.Equal(t, int64(1), p.Metrics.ReadProbeFailures.Count())
	})
}

type mock struct {
	t *testing.T

	noPlan bool
	planErr error

	noGet bool
	getErr error
}

func (m *mock) Plan(ctx context.Context) (Plan, error) {
	if m.noPlan {
		m.t.Errorf("plan call made but not expected")
	}
	return Plan{}, m.planErr
}

func (m *mock) Get(ctx context.Context, key interface{}) (kv.KeyValue, error) {
	if m.noGet {
		m.t.Errorf("get call made but not expected")
	}
	return kv.KeyValue{}, m.getErr
}

func clearMetrics(p *Prober) {
	p.Metrics.ReadProbeAttempts.Clear()
	p.Metrics.ReadProbeFailures.Clear()
	p.Metrics.ProbePlanAttempts.Clear()
	p.Metrics.ProbePlanFailures.Clear()
}

func TestWithJitter(t *testing.T) {
	cases := []struct{
		desc string
		in time.Duration
		intn func(n int) int
		want time.Duration
	}{
		{
			"no jitter added",
			time.Minute,
			func(n int) int {
				return 0
			},
			time.Minute,
		},
		{
			"max jitter added",
			time.Minute,
			func(n int) int {
				return n-1
			},
			72 * time.Second,
		},
		{
			"max jitter subtracted",
			time.Minute,
			func(n int) int {
				if n == 2 {
					return 0
				}
				return n-1
			},
			48 * time.Second,
		},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			got := withJitter(tc.in, tc.intn)
			require.InEpsilon(t, tc.want, got, 0.01)
		})
	}
}
