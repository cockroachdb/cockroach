// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwriter

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// SQLStore implements MetricStore by persisting metrics to the
// system.cluster_metrics table. It is used by the Writer to flush
// metrics on each node.
type SQLStore struct {
	db             isql.DB
	sqlIDContainer *base.SQLIDContainer
	st             *cluster.Settings
}

var _ MetricStore = (*SQLStore)(nil)

func newSQLStore(db isql.DB, sqlIDContainer *base.SQLIDContainer, st *cluster.Settings) *SQLStore {
	return &SQLStore{
		db:             db,
		sqlIDContainer: sqlIDContainer,
		st:             st,
	}
}

// Write writes the given metrics to the system.cluster_metrics table
// in a single batched upsert. Each metric is upserted based on its
// (name, labels) unique index. Counters accumulate: the new value is
// added to the existing stored value. Gauges replace: the stored
// value is overwritten.
func (s *SQLStore) Write(ctx context.Context, metrics []clustermetrics.Metric) error {
	if !s.st.Version.IsActive(ctx, clusterversion.V26_2_AddSystemClusterMetricsTable) {
		return nil
	}

	nodeID := int64(s.sqlIDContainer.SQLInstanceID())

	// Build a single INSERT ... VALUES (...), (...) statement with all
	// metrics. Each metric contributes 4 parameters: name, type, value,
	// node_id. Labels default to '{}' and last_updated to now().
	//
	// The ON CONFLICT clause uses a CASE on the type to decide whether
	// to accumulate (counter) or replace (gauge) the stored value.
	//
	// TODO(angles-n-daemons): label support will be added in a
	// follow-up PR; for now labels default to empty object {}.
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var buf strings.Builder
		args := make([]interface{}, 0, len(metrics)*4)
		for i, m := range metrics {
			if i > 0 {
				buf.WriteString(", ")
			}
			p := i*4 + 1 // 1-based placeholder index
			fmt.Fprintf(&buf, "($%d, '{}'::JSONB, $%d, $%d, $%d, now())", p, p+1, p+2, p+3)
			args = append(args,
				m.GetName(false /* useStaticLabels */),
				m.Type(),
				m.Get(),
				nodeID,
			)
		}

		stmt := `INSERT INTO system.cluster_metrics (name, labels, type, value, node_id, last_updated)
		 VALUES ` + buf.String() + `
		 ON CONFLICT (name, labels)
		 DO UPDATE SET value = CASE
		     WHEN excluded.type = 'counter'
		       THEN system.cluster_metrics.value + excluded.value
		     ELSE excluded.value
		   END,
		   node_id = excluded.node_id, last_updated = now()`

		_, err := txn.ExecEx(ctx, "upsert-cluster-metrics", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			stmt, args...,
		)
		return err
	})
}

// Get retrieves all stored metrics from the system.cluster_metrics table.
// This method is not part of the MetricStore interface; it exists for
// testing purposes only.
func (s *SQLStore) Get(ctx context.Context) ([]clustermetrics.Metric, error) {
	if !s.st.Version.IsActive(ctx, clusterversion.V26_2_AddSystemClusterMetricsTable) {
		return nil, nil
	}

	var result []clustermetrics.Metric
	err := s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rows, err := txn.QueryBufferedEx(ctx, "get-cluster-metrics", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`SELECT name, type, value FROM system.cluster_metrics`,
		)
		if err != nil {
			return err
		}
		for _, row := range rows {
			name := string(tree.MustBeDString(row[0]))
			typ := string(tree.MustBeDString(row[1]))
			value := int64(tree.MustBeDInt(row[2]))
			md := metric.Metadata{Name: name}
			switch typ {
			case "counter":
				c := clustermetrics.NewCounter(md)
				c.Inc(value)
				result = append(result, c)
			case "gauge":
				g := clustermetrics.NewGauge(md)
				g.Update(value)
				result = append(result, g)
			}
		}
		return nil
	})
	return result, err
}
