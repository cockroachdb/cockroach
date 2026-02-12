// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
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

type updater struct {
	tableWatcher *cmwatcher.Watcher
	registry     *registry
	stopper      *stop.Stopper

	mu struct {
		syncutil.Mutex
		trackedMetrics map[string]metric.Iterable           // name → metric
		trackedRows    map[int64]cmwatcher.ClusterMetricRow // id → last known row
	}
}

func newUpdater(
	reg *registry, clock *hlc.Clock, f *rangefeed.Factory, stopper *stop.Stopper,
) *updater {
	u := &updater{
		registry: reg,
		stopper:  stopper,
	}
	u.mu.trackedMetrics = make(map[string]metric.Iterable)
	u.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)

	u.tableWatcher = cmwatcher.NewWatcher(clock, f, stopper, cmwatcher.Handler{
		OnUpsert:  u.onUpsert,
		OnDelete:  u.onDelete,
		OnRefresh: u.onRefresh,
	})

	return u
}

// updateMetricLocked updates an already-tracked metric's value, or creates a
// new metric if it hasn't been seen before. Requires u.mu to be held.
func (u *updater) updateMetricLocked(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	if _, ok := u.mu.trackedRows[row.ID]; !ok {
		// First time seeing this row ID.
		var m metric.Iterable
		var err error
		if m, ok = u.mu.trackedMetrics[row.Name]; ok {
			// Metric already exists by name; assume it's a vector and add labels.
			switch t := m.(type) {
			case *metric.GaugeVec:
				t.Update(row.Labels, row.Value)
			case *metric.CounterVec:
				t.Update(row.Labels, row.Value)
			default:
				log.Dev.Warningf(ctx, "metric %q already exists as non-vector type %T; cannot add labels for id=%d", row.Name, m, row.ID)
				return
			}
		} else {
			if m, err = row.ToMetric(); err != nil {
				log.Dev.Errorf(ctx, "failed to convert metric row to metric: %s", err)
				return
			}
			u.mu.trackedMetrics[row.Name] = m
			u.registry.AddMetric(m)
		}
		u.mu.trackedRows[row.ID] = row
	} else {
		// Already tracking this row ID; update the value.
		m := u.mu.trackedMetrics[row.Name]
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
		}
		u.mu.trackedRows[row.ID] = row
	}
}

// deleteMetricLocked removes a metric from the registry and stops tracking it.
// It looks up the last-known row by ID, which is necessary because tombstone
// events only carry the row ID (name and labels are empty). Requires u.mu to
// be held.
func (u *updater) deleteMetricLocked(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	storedRow, ok := u.mu.trackedRows[row.ID]
	if !ok {
		return
	}
	m, ok := u.mu.trackedMetrics[storedRow.Name]
	if !ok {
		delete(u.mu.trackedRows, row.ID)
		return
	}
	if tm, ok := m.(metric.PrometheusVector); ok {
		tm.Delete(storedRow.Labels)
	} else {
		u.registry.RemoveMetric(m)
		delete(u.mu.trackedMetrics, storedRow.Name)
	}
	delete(u.mu.trackedRows, row.ID)
}

func (u *updater) start(ctx context.Context, tableResolver catalog.SystemTableIDResolver) error {
	err := u.tableWatcher.Start(ctx, tableResolver)
	if err != nil {
		log.Dev.Errorf(ctx, "failed to start cluster metrics rangefeed: %s", err)
		return err
	}

	u.stopper.AddCloser(stop.CloserFn(func() {
		u.stop()
	}))
	return nil
}

func (u *updater) stop() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, m := range u.mu.trackedMetrics {
		u.registry.RemoveMetric(m)
	}
	u.mu.trackedMetrics = make(map[string]metric.Iterable)
	u.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)
}

func (u *updater) onUpsert(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.updateMetricLocked(ctx, row)
}

// onDelete removes a metric from the registry and stops tracking it.
func (u *updater) onDelete(ctx context.Context, row cmwatcher.ClusterMetricRow) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.deleteMetricLocked(ctx, row)
}

func (u *updater) onRefresh(ctx context.Context, rows map[int64]cmwatcher.ClusterMetricRow) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, m := range u.mu.trackedMetrics {
		u.registry.RemoveMetric(m)
	}
	u.mu.trackedMetrics = make(map[string]metric.Iterable)
	u.mu.trackedRows = make(map[int64]cmwatcher.ClusterMetricRow)
	for _, row := range rows {
		u.updateMetricLocked(ctx, row)
	}
}

// Start initializes the cluster metrics updater, which watches the
// system.cluster_metrics table via a rangefeed and syncs rows into
// the server's cluster metric registry.
func Start(ctx context.Context, config *sql.ExecutorConfig) error {
	rr := config.MetricsRecorder.ClusterMetricRegistry(config.Codec.TenantID)
	if reg, ok := rr.(*registry); ok {
		return newUpdater(reg, config.Clock, config.RangeFeedFactory, config.Stopper).start(ctx, config.SystemTableIDResolver)
	}

	if buildutil.CrdbTestBuild {
		panic("expected cmreader.registry type")
	} else {
		return errors.New("expected cmreader.registry type")
	}
}
