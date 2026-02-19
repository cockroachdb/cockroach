// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwatcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/obs/clustermetrics/cmwatcher"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	r := sqlutils.MakeSQLRunner(db)

	// Insert initial rows with flat labels JSON.
	r.Exec(t, `INSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (100, 'cpu.percent', '{"node": "1"}', 'gauge', 55, 1)`)
	r.Exec(t, `INSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (200, 'mem.bytes', '{}', 'gauge', 1024, 1)`)

	// Channels to receive callback events.
	refreshCh := make(chan map[int64]cmwatcher.ClusterMetricRow, 1)
	upsertCh := make(chan cmwatcher.ClusterMetricRow, 10)
	deleteCh := make(chan cmwatcher.ClusterMetricRow, 10)

	w := cmwatcher.NewWatcher(
		keys.SystemSQLCodec,
		ts.Clock(),
		ts.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		ts.AppStopper(),
		cmwatcher.Handler{
			OnRefresh: func(ctx context.Context, rows map[int64]cmwatcher.ClusterMetricRow) {
				refreshCh <- rows
			},
			OnUpsert: func(ctx context.Context, row cmwatcher.ClusterMetricRow) {
				upsertCh <- row
			},
			OnDelete: func(ctx context.Context, row cmwatcher.ClusterMetricRow) {
				deleteCh <- row
			},
		},
	)
	err := w.Start(ctx, ts.SystemTableIDResolver().(catalog.SystemTableIDResolver))
	require.NoError(t, err)

	// Verify initial snapshot via OnRefresh.
	rows := receiveRows(t, refreshCh)
	require.Len(t, rows, 2)
	require.Equal(t, "cpu.percent", rows[100].Name)
	require.Equal(t, int64(55), rows[100].Value)
	require.Equal(t, "mem.bytes", rows[200].Name)

	// Verify labels were decoded correctly.
	require.Equal(t, map[string]string{"node": "1"}, rows[100].Labels)

	// Row with empty labels should have no entries.
	require.Empty(t, rows[200].Labels)

	// INSERT a new row and verify OnUpsert fires.
	r.Exec(t, `INSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (300, 'disk.iops', '{}', 'counter', 999, 2)`)

	row := receiveRow(t, upsertCh)
	require.Equal(t, int64(300), row.ID)
	require.Equal(t, "disk.iops", row.Name)
	require.Equal(t, int64(999), row.Value)

	// UPSERT an existing row and verify OnUpsert fires.
	r.Exec(t, `UPSERT INTO system.cluster_metrics (id, name, labels, type, value, node_id)
		VALUES (100, 'cpu.percent', '{"node": "1"}', 'gauge', 75, 1)`)

	row = receiveRow(t, upsertCh)
	require.Equal(t, int64(100), row.ID)
	require.Equal(t, int64(75), row.Value)

	// DELETE a row and verify OnDelete fires.
	r.Exec(t, `DELETE FROM system.cluster_metrics WHERE id = 200`)

	row = receiveRow(t, deleteCh)
	require.Equal(t, int64(200), row.ID)
}

func receiveRows(
	t *testing.T, ch <-chan map[int64]cmwatcher.ClusterMetricRow,
) map[int64]cmwatcher.ClusterMetricRow {
	t.Helper()
	select {
	case rows := <-ch:
		return rows
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for OnRefresh callback")
		return nil
	}
}

func receiveRow(t *testing.T, ch <-chan cmwatcher.ClusterMetricRow) cmwatcher.ClusterMetricRow {
	t.Helper()
	select {
	case row := <-ch:
		return row
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for callback")
		return cmwatcher.ClusterMetricRow{}
	}
}
