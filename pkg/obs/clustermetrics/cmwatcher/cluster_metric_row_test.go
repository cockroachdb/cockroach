// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwatcher_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// TestRowDecoder verifies that the row decoder can decode rows stored in the
// system.cluster_metrics table of a real cluster.
func TestRowDecoder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `INSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (1, 'test.metric', '{"store": "1"}', 'gauge', 42, 1)`)
	r.Exec(t, `INSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (2, 'test.second', '{}', 'counter', 0, 2)`)

	tableID, err := ts.SystemTableIDResolver().(catalog.SystemTableIDResolver).LookupSystemTableID(ctx, "cluster_metrics")
	require.NoError(t, err)
	// Scan only the primary index (index ID 1) to avoid secondary index entries.
	k := ts.Codec().IndexPrefix(uint32(tableID), 1)
	rows, err := kvDB.Scan(ctx, k, k.PrefixEnd(), 0 /* maxRows */)
	require.NoError(t, err)
	require.NotEmpty(t, rows)

	dec := cmwatcher.MakeRowDecoder(ts.Codec())
	var decoded []cmwatcher.ClusterMetricRow
	for _, row := range rows {
		kv := roachpb.KeyValue{
			Key:   row.Key,
			Value: *row.Value,
		}
		cmRow, tombstone, err := dec.DecodeRow(kv)
		require.NoError(t, err)
		require.False(t, tombstone)
		decoded = append(decoded, cmRow)

		// Test tombstone handling.
		kv.Value.Reset()
		tombstoneRow, isTombstone, err := dec.DecodeRow(kv)
		require.NoError(t, err)
		require.True(t, isTombstone)
		require.Equal(t, cmRow.ID, tombstoneRow.ID)
	}

	// Find the two rows we inserted and verify their fields.
	var foundMetric, foundSecond bool
	for _, row := range decoded {
		switch row.ID {
		case 1:
			foundMetric = true
			require.Equal(t, "test.metric", row.Name)
			require.Equal(t, map[string]string{"store": "1"}, row.Labels)
			require.Equal(t, "gauge", row.Type)
			require.Equal(t, int64(42), row.Value)
			require.Equal(t, int64(1), row.NodeID)
			require.False(t, row.LastUpdated.IsZero())
		case 2:
			foundSecond = true
			require.Equal(t, "test.second", row.Name)
			require.Equal(t, int64(0), row.Value)
			require.Equal(t, int64(2), row.NodeID)
		}
	}
	require.True(t, foundMetric, "did not find row with id=1")
	require.True(t, foundSecond, "did not find row with id=2")
}

func TestToMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	gaugeMeta := metric.Metadata{Name: "test.gauge", Help: "A test gauge"}
	counterMeta := metric.Metadata{Name: "test.counter", Help: "A test counter"}
	labeledGaugeMeta := metric.Metadata{Name: "test.labeled.gauge", Help: "A labeled gauge"}
	labeledCounterMeta := metric.Metadata{Name: "test.labeled.counter", Help: "A labeled counter"}

	cleanupGauge := clustermetrics.TestingRegisterClusterMetric("test.gauge", gaugeMeta)
	defer cleanupGauge()
	cleanupCounter := clustermetrics.TestingRegisterClusterMetric("test.counter", counterMeta)
	defer cleanupCounter()
	cleanupLabeledGauge := clustermetrics.TestingRegisterLabeledClusterMetric(
		"test.labeled.gauge", labeledGaugeMeta, []string{"store"},
	)
	defer cleanupLabeledGauge()
	cleanupLabeledCounter := clustermetrics.TestingRegisterLabeledClusterMetric(
		"test.labeled.counter", labeledCounterMeta, []string{"node"},
	)
	defer cleanupLabeledCounter()

	tests := []struct {
		name     string
		row      cmwatcher.ClusterMetricRow
		wantType metric.Iterable
		wantErr  string
	}{{
		name: "gauge without labels",
		row: cmwatcher.ClusterMetricRow{
			Name:  "test.gauge",
			Type:  "gauge",
			Value: 42,
		},
		wantType: &metric.Gauge{},
	}, {
		name: "counter without labels",
		row: cmwatcher.ClusterMetricRow{
			Name:  "test.counter",
			Type:  "counter",
			Value: 100,
		},
		wantType: &metric.Counter{},
	}, {
		name: "labeled gauge",
		row: cmwatcher.ClusterMetricRow{
			Name:   "test.labeled.gauge",
			Labels: map[string]string{"store": "1"},
			Type:   "gauge",
			Value:  7,
		},
		wantType: &metric.GaugeVec{},
	}, {
		name: "labeled counter",
		row: cmwatcher.ClusterMetricRow{
			Name:   "test.labeled.counter",
			Labels: map[string]string{"node": "3"},
			Type:   "counter",
			Value:  55,
		},
		wantType: &metric.CounterVec{},
	}, {
		name: "unknown type without labels",
		row: cmwatcher.ClusterMetricRow{
			Name:  "test.gauge",
			Type:  "histogram",
			Value: 1,
		},
		wantErr: "unknown metric type histogram",
	}, {
		name: "unknown type with labels",
		row: cmwatcher.ClusterMetricRow{
			Name:   "test.labeled.gauge",
			Labels: map[string]string{"store": "1"},
			Type:   "histogram",
			Value:  1,
		},
		wantErr: "unknown metric type histogram",
	}, {
		name: "unregistered metric without labels",
		row: cmwatcher.ClusterMetricRow{
			Name:  "nonexistent.metric",
			Type:  "gauge",
			Value: 1,
		},
		wantErr: "no metadata found for metric nonexistent.metric",
	}, {
		name: "unregistered labeled metric",
		row: cmwatcher.ClusterMetricRow{
			Name:   "nonexistent.labeled",
			Labels: map[string]string{"k": "v"},
			Type:   "gauge",
			Value:  1,
		},
		wantErr: "no metadata found for metric nonexistent.labeled",
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := tt.row.ToMetric()
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, m)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, m)
			require.Equal(t, tt.row.Name, m.GetName(false))
			require.Equal(t,
				reflect.TypeOf(tt.wantType), reflect.TypeOf(m),
				"expected %T, got %T", tt.wantType, m,
			)
		})
	}
}
