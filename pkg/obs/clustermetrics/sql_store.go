// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// SQLStore implements MetricStore by persisting metrics to the
// system.cluster_metrics table. It is used by the Writer to flush
// metrics on each node.
type SQLStore struct {
	db             isql.DB
	sqlIDContainer *base.SQLIDContainer
	st             *cluster.Settings
}

// Verify interface compliance.
var _ MetricStore = (*SQLStore)(nil)

// NewSQLStore creates a new SQLStore.
func NewSQLStore(db isql.DB, sqlIDContainer *base.SQLIDContainer, st *cluster.Settings) *SQLStore {
	return &SQLStore{
		db:             db,
		sqlIDContainer: sqlIDContainer,
		st:             st,
	}
}

// Write writes the given metrics to the system.cluster_metrics table.
// Each metric is upserted based on its (name, labels) unique index.
func (s *SQLStore) Write(ctx context.Context, metrics []Metric) error {
	// Check if the cluster version supports the system.cluster_metrics table.
	if !s.st.Version.IsActive(ctx, clusterversion.V26_2_AddSystemClusterMetricsTable) {
		return nil
	}

	nodeID := int64(s.sqlIDContainer.SQLInstanceID())

	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, m := range metrics {
			metricType := metricTypeString(m)
			name := m.GetName(false /* useStaticLabels */)
			value := m.Get()

			// Use INSERT ON CONFLICT to upsert based on (name, labels) unique index.
			// Labels default to empty object {}.
			_, err := txn.ExecEx(ctx, "upsert-cluster-metric", txn.KV(),
				sessiondata.NodeUserSessionDataOverride,
				`INSERT INTO system.cluster_metrics (name, labels, type, value, node_id, last_updated)
				 VALUES ($1, '{}'::JSONB, $2, $3, $4, now())
				 ON CONFLICT (name, labels)
				 DO UPDATE SET value = excluded.value, node_id = excluded.node_id, last_updated = now()`,
				name, metricType, value, nodeID,
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Get retrieves all stored metrics from the system.cluster_metrics table.
// This is a stub implementation that will be completed when implementing
// the reader functionality.
func (s *SQLStore) Get(ctx context.Context) ([]Metric, error) {
	// Stub: return empty slice for now.
	// Will be implemented as part of the reader functionality.
	return nil, nil
}

// metricTypeString returns the type string for a metric.
func metricTypeString(m Metric) string {
	switch m.(type) {
	case *Counter:
		return "counter"
	case *Gauge:
		return "gauge"
	default:
		return "unknown"
	}
}
