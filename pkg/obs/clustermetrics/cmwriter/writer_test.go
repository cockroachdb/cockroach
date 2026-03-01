// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// storedMetric represents a stored metric value with metadata.
type storedMetric struct {
	Metric      cmmetrics.WritableMetric
	StoredValue int64
	Timestamp   time.Time
}

// mapStore is a MetricStore implementation backed by an in-memory map.
type mapStore struct {
	mu struct {
		syncutil.Mutex
		metrics map[string]storedMetric
	}
}

// storeKey returns a unique key for a metric based on its name and labels.
func storeKey(m cmmetrics.WritableMetric) string {
	name := m.GetName(false /* useStaticLabels */)
	labels := m.GetLabels()
	if len(labels) == 0 {
		return name
	}
	b, _ := json.Marshal(labels)
	return name + string(b)
}

func newMapStore() *mapStore {
	s := &mapStore{}
	s.mu.metrics = make(map[string]storedMetric)
	return s
}

func (s *mapStore) Write(_ context.Context, metrics []cmmetrics.WritableMetric) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := timeutil.Now()
	for _, m := range metrics {
		s.mu.metrics[storeKey(m)] = storedMetric{
			Metric:      m,
			StoredValue: m.Value(),
			Timestamp:   now,
		}
	}
	return nil
}

func (s *mapStore) get(name string) (storedMetric, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.mu.metrics[name]
	return m, ok
}

// getByLabels returns the stored metric matching the given name and labels.
func (s *mapStore) getByLabels(name string, labels map[string]string) (storedMetric, bool) {
	key := name
	if len(labels) > 0 {
		b, _ := json.Marshal(labels)
		key = name + string(b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.mu.metrics[key]
	return m, ok
}

// count returns the total number of stored metrics.
func (s *mapStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mu.metrics)
}

// testEnv provides common test infrastructure for writer tests.
type testEnv struct {
	ctx           context.Context
	store         *mapStore
	writerMetrics *WriterMetrics
	writer        *Writer
}

func newTestEnv() *testEnv {
	ctx := context.Background()
	store := newMapStore()
	writerMetrics := NewWriterMetrics()
	writer := NewWriterWithStore(store, writerMetrics)
	return &testEnv{
		ctx:           ctx,
		store:         store,
		writerMetrics: writerMetrics,
		writer:        writer,
	}
}

func TestWriter_Flush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cmmetrics.TestingAllowNonInitConstruction()()

	t.Run("single flush", func(t *testing.T) {
		tests := []struct {
			name          string
			setup         func(env *testEnv)
			wantStored    map[string]int64
			wantNotStored []string
			postCheck     func(t *testing.T, env *testEnv)
		}{{
			name: "counter incremented is stored",
			setup: func(env *testEnv) {
				c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
				env.writer.AddMetric(c)
				c.Inc(5)
			},
			wantStored: map[string]int64{"c": 5},
		}, {
			name: "counter accumulates multiple increments",
			setup: func(env *testEnv) {
				c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
				env.writer.AddMetric(c)
				c.Inc(5)
				c.Inc(3)
			},
			wantStored: map[string]int64{"c": 8},
		}, {
			name: "counter not incremented is not stored",
			setup: func(env *testEnv) {
				c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
				env.writer.AddMetric(c)
			},
			wantNotStored: []string{"c"},
		}, {
			name: "gauge updated is stored",
			setup: func(env *testEnv) {
				g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
				env.writer.AddMetric(g)
				g.Update(100)
			},
			wantStored: map[string]int64{"g": 100},
		}, {
			name: "gauge not updated is not stored",
			setup: func(env *testEnv) {
				g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
				env.writer.AddMetric(g)
			},
			wantNotStored: []string{"g"},
		}, {
			name: "gauge Inc marks dirty",
			setup: func(env *testEnv) {
				g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
				env.writer.AddMetric(g)
				g.Inc(10)
			},
			wantStored: map[string]int64{"g": 10},
		}, {
			name: "gauge Dec marks dirty",
			setup: func(env *testEnv) {
				g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
				env.writer.AddMetric(g)
				g.Dec(-5) // Dec of negative = increase
			},
			wantStored: map[string]int64{"g": 5},
		}, {
			name: "multiple counters all stored",
			setup: func(env *testEnv) {
				c1 := cmmetrics.NewCounter(metric.Metadata{Name: "c1"})
				c2 := cmmetrics.NewCounter(metric.Metadata{Name: "c2"})
				env.writer.AddMetric(c1)
				env.writer.AddMetric(c2)
				c1.Inc(10)
				c2.Inc(20)
			},
			wantStored: map[string]int64{"c1": 10, "c2": 20},
		}, {
			name: "multiple gauges all stored",
			setup: func(env *testEnv) {
				g1 := cmmetrics.NewGauge(metric.Metadata{Name: "g1"})
				g2 := cmmetrics.NewGauge(metric.Metadata{Name: "g2"})
				env.writer.AddMetric(g1)
				env.writer.AddMetric(g2)
				g1.Update(100)
				g2.Update(200)
			},
			wantStored: map[string]int64{"g1": 100, "g2": 200},
		}, {
			name: "mixed counters and gauges all stored",
			setup: func(env *testEnv) {
				c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
				g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
				env.writer.AddMetric(c)
				env.writer.AddMetric(g)
				c.Inc(10)
				g.Update(20)
			},
			wantStored: map[string]int64{"c": 10, "g": 20},
		}, {
			name: "only dirty metrics stored",
			setup: func(env *testEnv) {
				c1 := cmmetrics.NewCounter(metric.Metadata{Name: "c1"})
				c2 := cmmetrics.NewCounter(metric.Metadata{Name: "c2"})
				g1 := cmmetrics.NewGauge(metric.Metadata{Name: "g1"})
				g2 := cmmetrics.NewGauge(metric.Metadata{Name: "g2"})
				env.writer.AddMetric(c1)
				env.writer.AddMetric(c2)
				env.writer.AddMetric(g1)
				env.writer.AddMetric(g2)
				c1.Inc(10)
				// c2 not incremented
				g1.Update(20)
				// g2 not updated
			},
			wantStored:    map[string]int64{"c1": 10, "g1": 20},
			wantNotStored: []string{"c2", "g2"},
		}, {
			name: "stopwatch started is stored",
			setup: func(env *testEnv) {
				sw := cmmetrics.NewWriteStopwatch(metric.Metadata{Name: "sw"}, timeutil.DefaultTimeSource{})
				env.writer.AddMetric(sw)
				sw.SetStartTime()
			},
			postCheck: func(t *testing.T, env *testEnv) {
				stored, ok := env.store.get("sw")
				require.True(t, ok, "stopwatch should be stored")
				require.NotZero(t, stored.StoredValue, "stopwatch should store a nonzero unix timestamp")
			},
		}, {
			name: "stopwatch not started is not stored",
			setup: func(env *testEnv) {
				sw := cmmetrics.NewWriteStopwatch(metric.Metadata{Name: "sw"}, timeutil.DefaultTimeSource{})
				env.writer.AddMetric(sw)
			},
			wantNotStored: []string{"sw"},
		}, {
			name: "struct registration",
			setup: func(env *testEnv) {
				type TestMetrics struct {
					Count  *cmmetrics.Counter
					Status *cmmetrics.Gauge
				}
				m := TestMetrics{
					Count:  cmmetrics.NewCounter(metric.Metadata{Name: "count"}),
					Status: cmmetrics.NewGauge(metric.Metadata{Name: "status"}),
				}
				env.writer.AddMetricStruct(&m)
				m.Count.Inc(15)
				m.Status.Update(200)
			},
			wantStored: map[string]int64{"count": 15, "status": 200},
		}}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newTestEnv()
				tt.setup(env)
				env.writer.Flush(env.ctx)

				for name, wantValue := range tt.wantStored {
					stored, ok := env.store.get(name)
					require.True(t, ok, "metric %q should be stored", name)
					require.Equal(t, wantValue, stored.StoredValue, "metric %q value mismatch", name)
				}

				for _, name := range tt.wantNotStored {
					_, ok := env.store.get(name)
					require.False(t, ok, "metric %q should not be stored", name)
				}

				if tt.postCheck != nil {
					tt.postCheck(t, env)
				}
			})
		}
	})

	t.Run("gauge unchanged on second flush not written", func(t *testing.T) {
		env := newTestEnv()
		g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
		env.writer.AddMetric(g)

		g.Update(50)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Second flush without change should not write.
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Update and verify it writes again.
		g.Update(75)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())
	})

	t.Run("gauge retains value after flush", func(t *testing.T) {
		env := newTestEnv()
		g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
		env.writer.AddMetric(g)

		g.Update(42)
		env.writer.Flush(env.ctx)

		require.Equal(t, int64(42), g.Value(), "gauge should retain value after flush")
	})

	t.Run("counter accumulates across increments then resets", func(t *testing.T) {
		env := newTestEnv()
		c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
		env.writer.AddMetric(c)

		c.Inc(5)
		c.Inc(3)
		env.writer.Flush(env.ctx)

		stored, _ := env.store.get("c")
		require.Equal(t, int64(8), stored.StoredValue)
		require.Equal(t, int64(0), c.Count())

		// Second round of increments.
		c.Inc(2)
		env.writer.Flush(env.ctx)

		stored, _ = env.store.get("c")
		require.Equal(t, int64(2), stored.StoredValue)
	})

	t.Run("mixed dirty and clean on second flush", func(t *testing.T) {
		env := newTestEnv()
		c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
		g := cmmetrics.NewGauge(metric.Metadata{Name: "g"})
		env.writer.AddMetric(c)
		env.writer.AddMetric(g)

		c.Inc(10)
		g.Update(100)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())

		// Only update counter on second flush.
		c.Inc(5)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(3), env.writerMetrics.MetricsWritten.Count())

		stored, _ := env.store.get("c")
		require.Equal(t, int64(5), stored.StoredValue)

		// Gauge still has old value in store.
		stored, _ = env.store.get("g")
		require.Equal(t, int64(100), stored.StoredValue)
	})
}

func TestWriter_OperationalMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cmmetrics.TestingAllowNonInitConstruction()()

	t.Run("records flush metrics", func(t *testing.T) {
		env := newTestEnv()
		c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
		env.writer.AddMetric(c)
		c.Inc(1)

		env.writer.Flush(env.ctx)

		require.Equal(t, int64(1), env.writerMetrics.FlushCount.Count())
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())
		require.Equal(t, int64(0), env.writerMetrics.FlushErrors.Count())
	})

	t.Run("empty flush records nothing", func(t *testing.T) {
		env := newTestEnv()

		env.writer.Flush(env.ctx)

		require.Equal(t, int64(0), env.writerMetrics.FlushCount.Count())
		require.Equal(t, int64(0), env.writerMetrics.MetricsWritten.Count())
	})
}

func TestWriter_PeriodicFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cmmetrics.TestingAllowNonInitConstruction()()

	t.Run("enabled", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		store := newMapStore()
		st := cluster.MakeTestingClusterSettings()
		FlushInterval.Override(ctx, &st.SV, 50*time.Millisecond)

		writerMetrics := NewWriterMetrics()
		w := NewWriterWithStore(store, writerMetrics)

		c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
		w.AddMetric(c)
		c.Inc(5)

		require.NoError(t, w.Start(ctx, stopper, &st.SV))

		testutils.SucceedsSoon(t, func() error {
			if writerMetrics.FlushCount.Count() < 1 {
				return errors.New("waiting for at least one flush")
			}
			return nil
		})
	})

	t.Run("disabled", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		store := newMapStore()
		st := cluster.MakeTestingClusterSettings()
		FlushInterval.Override(ctx, &st.SV, 50*time.Millisecond)
		FlushEnabled.Override(ctx, &st.SV, false)

		writerMetrics := NewWriterMetrics()
		w := NewWriterWithStore(store, writerMetrics)

		c := cmmetrics.NewCounter(metric.Metadata{Name: "c"})
		w.AddMetric(c)
		c.Inc(5)

		require.NoError(t, w.Start(ctx, stopper, &st.SV))

		// Wait for enough time that flushes would have occurred if enabled,
		// then verify no flushes happened.
		start := timeutil.Now()
		testutils.SucceedsSoon(t, func() error {
			if timeutil.Since(start) < 200*time.Millisecond {
				return errors.New("waiting for sufficient time to pass")
			}
			return nil
		})

		require.Equal(t, int64(0), writerMetrics.FlushCount.Count())
	})
}

func TestWriter_VecMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer cmmetrics.TestingAllowNonInitConstruction()()

	t.Run("gauge vec children persisted with labels", func(t *testing.T) {
		env := newTestEnv()
		gv := cmmetrics.NewGaugeVec(metric.Metadata{Name: "gv"}, "region", "status")
		env.writer.AddMetric(gv)

		gv.Update(map[string]string{"region": "us-east", "status": "ok"}, 10)
		gv.Update(map[string]string{"region": "us-west", "status": "err"}, 20)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.getByLabels("gv", map[string]string{"region": "us-east", "status": "ok"})
		require.True(t, ok, "expected labeled metric to be stored")
		require.Equal(t, int64(10), stored.StoredValue)

		stored, ok = env.store.getByLabels("gv", map[string]string{"region": "us-west", "status": "err"})
		require.True(t, ok, "expected labeled metric to be stored")
		require.Equal(t, int64(20), stored.StoredValue)
	})

	t.Run("counter vec children persisted with labels", func(t *testing.T) {
		env := newTestEnv()
		cv := cmmetrics.NewCounterVec(metric.Metadata{Name: "cv"}, "method")
		env.writer.AddMetric(cv)

		cv.Inc(map[string]string{"method": "GET"}, 5)
		cv.Inc(map[string]string{"method": "POST"}, 3)
		env.writer.Flush(env.ctx)

		stored, ok := env.store.getByLabels("cv", map[string]string{"method": "GET"})
		require.True(t, ok, "expected labeled metric to be stored")
		require.Equal(t, int64(5), stored.StoredValue)

		stored, ok = env.store.getByLabels("cv", map[string]string{"method": "POST"})
		require.True(t, ok, "expected labeled metric to be stored")
		require.Equal(t, int64(3), stored.StoredValue)
	})

	t.Run("vec with no children is not flushed", func(t *testing.T) {
		env := newTestEnv()
		gv := cmmetrics.NewGaugeVec(metric.Metadata{Name: "gv"}, "region")
		env.writer.AddMetric(gv)

		env.writer.Flush(env.ctx)
		require.Equal(t, int64(0), env.writerMetrics.MetricsWritten.Count())
	})

	t.Run("vec children cleared after flush", func(t *testing.T) {
		env := newTestEnv()
		gv := cmmetrics.NewGaugeVec(metric.Metadata{Name: "gv"}, "region")
		env.writer.AddMetric(gv)

		gv.Update(map[string]string{"region": "us-east"}, 10)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Second flush without new updates should not write anything.
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Setting a new value should trigger a write.
		gv.Update(map[string]string{"region": "us-east"}, 20)
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())
	})

	t.Run("only dirty vec children are written", func(t *testing.T) {
		env := newTestEnv()
		gv := cmmetrics.NewGaugeVec(metric.Metadata{Name: "gv"}, "region")
		cv := cmmetrics.NewCounterVec(metric.Metadata{Name: "cv"}, "method")
		env.writer.AddMetric(gv)
		env.writer.AddMetric(cv)

		// Only update gauge vec, counter vec has no children.
		gv.Update(map[string]string{"region": "us-east"}, 10)
		env.writer.Flush(env.ctx)

		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())
		require.Equal(t, 1, env.store.count())
	})

	t.Run("stopwatch vec children persisted with labels", func(t *testing.T) {
		env := newTestEnv()
		sv := cmmetrics.NewWriteStopwatchVec(metric.Metadata{Name: "sv"}, timeutil.DefaultTimeSource{}, "job")
		env.writer.AddMetric(sv)

		sv.SetStartTime(map[string]string{"job": "backup"})
		sv.SetStartTime(map[string]string{"job": "restore"})
		env.writer.Flush(env.ctx)

		stored, ok := env.store.getByLabels("sv", map[string]string{"job": "backup"})
		require.True(t, ok, "expected labeled stopwatch to be stored")
		require.NotZero(t, stored.StoredValue, "stopwatch should store a nonzero unix timestamp")

		stored, ok = env.store.getByLabels("sv", map[string]string{"job": "restore"})
		require.True(t, ok, "expected labeled stopwatch to be stored")
		require.NotZero(t, stored.StoredValue, "stopwatch should store a nonzero unix timestamp")
	})

	t.Run("stopwatch vec with no children is not flushed", func(t *testing.T) {
		env := newTestEnv()
		sv := cmmetrics.NewWriteStopwatchVec(metric.Metadata{Name: "sv"}, timeutil.DefaultTimeSource{}, "job")
		env.writer.AddMetric(sv)

		env.writer.Flush(env.ctx)
		require.Equal(t, int64(0), env.writerMetrics.MetricsWritten.Count())
	})

	t.Run("stopwatch vec children cleared after flush", func(t *testing.T) {
		env := newTestEnv()
		sv := cmmetrics.NewWriteStopwatchVec(metric.Metadata{Name: "sv"}, timeutil.DefaultTimeSource{}, "job")
		env.writer.AddMetric(sv)

		sv.SetStartTime(map[string]string{"job": "backup"})
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Second flush without new updates should not write anything.
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(1), env.writerMetrics.MetricsWritten.Count())

		// Setting a new start time should trigger a write.
		sv.SetStartTime(map[string]string{"job": "backup"})
		env.writer.Flush(env.ctx)
		require.Equal(t, int64(2), env.writerMetrics.MetricsWritten.Count())
	})
}
