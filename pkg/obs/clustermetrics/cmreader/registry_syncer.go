// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmmetrics"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type registrySyncer struct {
	tableWatcher *cmwatcher.Watcher
	registry     *cmmetrics.Registry
	stopper      *stop.Stopper
	mu           struct {
		syncutil.Mutex
		trackedMetrics map[string]metric.Iterable           // name → metric
		trackedRows    map[int64]cmwatcher.ClusterMetricRow // id → last known row
	}
}

func newRegistrySyncer(
	reg *cmmetrics.Registry,
	codec keys.SQLCodec,
	clock *hlc.Clock,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
) *registrySyncer {
	s := &registrySyncer{
		registry: reg,
		stopper:  stopper,
	}
	s.mu.trackedMetrics = make(map[string]metric.Iterable)
	s.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)

	s.tableWatcher = cmwatcher.NewWatcher(codec, clock, f, stopper, cmwatcher.Handler{
		OnUpsert:  s.updateMetric,
		OnDelete:  s.deregisterMetric,
		OnRefresh: s.reloadAllMetrics,
	})

	return s
}

// updateMetricLocked updates an already-tracked metric's value, or creates a
// new metric if it hasn't been seen before. Requires u.mu to be held.
func (s *registrySyncer) updateMetricLocked(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	if _, ok := s.mu.trackedRows[row.ID]; !ok {
		// First time seeing this row ID — ensure metric is registered.
		if m, exists := s.mu.trackedMetrics[row.Name]; exists {
			// Multiple row IDs for the same name is only valid for vector metrics.
			if _, isVec := m.(metric.PrometheusVector); !isVec {
				log.Dev.Warningf(ctx, "metric %q already exists as non-vector type %T; cannot add labels for id=%d", row.Name, m, row.ID)
				return
			}
		} else {
			m, err := row.ToMetric()
			if err != nil {
				log.Dev.Errorf(ctx, "failed to convert metric row to metric: %s", err)
				return
			}
			s.mu.trackedMetrics[row.Name] = m
			s.registry.AddMetric(m)
		}
	}

	// Update the metric value.
	m := s.mu.trackedMetrics[row.Name]
	switch t := m.(type) {
	case *metric.Gauge:
		t.Update(row.Value)
	case *metric.Counter:
		t.Update(row.Value)
	case *metric.GaugeVec:
		t.Update(row.Labels, row.Value)
	case *metric.CounterVec:
		t.Update(row.Labels, row.Value)
	default:
		if buildutil.CrdbTestBuild {
			panic(errors.Newf("unsupported metric type %T for metric.id=%d, metric.name=%s", m, row.ID, row.Name))
		}
		log.Dev.Errorf(ctx, "unsupported metric type %T for metric.id=%d, metric.name=%s", m, row.ID, row.Name)
		return
	}
	s.mu.trackedRows[row.ID] = row
}

// deregisterMetricLocked removes a metric from the registry and stops tracking it.
// It looks up the last-known row by ID, which is necessary because tombstone
// events only carry the row ID (name and labels are empty). Requires u.mu to
// be held.
func (s *registrySyncer) deregisterMetricLocked(
	ctx context.Context, row cmwatcher.ClusterMetricRow,
) {
	storedRow, ok := s.mu.trackedRows[row.ID]
	if !ok {
		return
	}
	m, ok := s.mu.trackedMetrics[storedRow.Name]
	if !ok {
		delete(s.mu.trackedRows, row.ID)
		return
	}
	if tm, ok := m.(metric.PrometheusVector); ok {
		tm.Delete(storedRow.Labels)
	} else {
		s.registry.RemoveMetric(m)
		delete(s.mu.trackedMetrics, storedRow.Name)
	}
	delete(s.mu.trackedRows, row.ID)
}

func (s *registrySyncer) start(
	ctx context.Context, tableResolver catalog.SystemTableIDResolver,
) error {
	err := s.tableWatcher.Start(ctx, tableResolver)
	if err != nil {
		log.Dev.Errorf(ctx, "failed to start cluster metrics rangefeed: %s", err)
		return err
	}

	s.stopper.AddCloser(stop.CloserFn(func() {
		s.stop()
	}))
	return nil
}

func (s *registrySyncer) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.mu.trackedMetrics {
		s.registry.RemoveMetric(m)
	}
	s.mu.trackedMetrics = make(map[string]metric.Iterable)
	s.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)
}

func (s *registrySyncer) updateMetric(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateMetricLocked(ctx, row)
}

// deregisterMetric removes a metric from the registry and stops tracking it.
func (s *registrySyncer) deregisterMetric(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deregisterMetricLocked(ctx, row)
}

// reloadAllMetrics removes all metrics from registrySyncer.registry and
// registers all metrics provided in the metrics maps.
func (s *registrySyncer) reloadAllMetrics(
	ctx context.Context, metrics map[int64]cmwatcher.ClusterMetricRow,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.mu.trackedMetrics {
		s.registry.RemoveMetric(m)
	}
	s.mu.trackedMetrics = make(map[string]metric.Iterable)
	s.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)
	for _, row := range metrics {
		s.updateMetricLocked(ctx, row)
	}
}

// Start initializes the cluster metrics registrySyncer, which watches the
// system.cluster_metrics table via a rangefeed and syncs rows into
// the server's cluster metric registry.
func Start(ctx context.Context, config *sql.ExecutorConfig) error {
	rr := config.MetricsRecorder.ClusterMetricRegistry(config.Codec.TenantID)
	if reg, ok := rr.(*cmmetrics.Registry); ok {
		return newRegistrySyncer(
			reg,
			config.Codec,
			config.Clock,
			config.RangeFeedFactory,
			config.Stopper).start(ctx, config.SystemTableIDResolver)
	}

	if buildutil.CrdbTestBuild {
		panic("expected cmmetrics.Registry type")
	} else {
		return errors.New("expected cmmetrics.Registry type")
	}
}
