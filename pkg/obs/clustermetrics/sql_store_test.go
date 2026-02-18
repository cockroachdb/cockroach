// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

// sqlStoreTestEnv provides common test infrastructure for SQLStore tests.
type sqlStoreTestEnv struct {
	ctx    context.Context
	store  *SQLStore
	runner *sqlutils.SQLRunner
}

func newSQLStoreTestEnv(t *testing.T) *sqlStoreTestEnv {
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	s := srv.ApplicationLayer()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	db := s.InternalDB().(isql.DB)
	store := NewSQLStore(db, execCfg.NodeInfo.NodeID, s.ClusterSettings())
	runner := sqlutils.MakeSQLRunner(s.SQLConn(t))

	return &sqlStoreTestEnv{
		ctx:    ctx,
		store:  store,
		runner: runner,
	}
}

func TestSQLStore_Write(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("counter", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := NewCounter(metric.Metadata{Name: "test.counter"})
		c.Inc(42)

		err := env.store.Write(env.ctx, []Metric{c})
		require.NoError(t, err)

		row := env.runner.QueryRow(t,
			`SELECT name, type, value FROM system.cluster_metrics WHERE name = $1`,
			"test.counter",
		)
		var name, typ string
		var value int64
		row.Scan(&name, &typ, &value)
		require.Equal(t, "test.counter", name)
		require.Equal(t, "counter", typ)
		require.Equal(t, int64(42), value)
	})

	t.Run("gauge", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		g := NewGauge(metric.Metadata{Name: "test.gauge"})
		g.Update(100)

		err := env.store.Write(env.ctx, []Metric{g})
		require.NoError(t, err)

		row := env.runner.QueryRow(t,
			`SELECT name, type, value FROM system.cluster_metrics WHERE name = $1`,
			"test.gauge",
		)
		var name, typ string
		var value int64
		row.Scan(&name, &typ, &value)
		require.Equal(t, "test.gauge", name)
		require.Equal(t, "gauge", typ)
		require.Equal(t, int64(100), value)
	})

	t.Run("upsert semantics", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := NewCounter(metric.Metadata{Name: "test.upsert"})
		c.Inc(10)

		require.NoError(t, env.store.Write(env.ctx, []Metric{c}))

		// Update the counter and write again.
		c.Inc(5)
		require.NoError(t, env.store.Write(env.ctx, []Metric{c}))

		// Should have exactly one row with the updated value.
		var count int
		env.runner.QueryRow(t,
			`SELECT count(*) FROM system.cluster_metrics WHERE name = $1`,
			"test.upsert",
		).Scan(&count)
		require.Equal(t, 1, count)

		var value int64
		env.runner.QueryRow(t,
			`SELECT value FROM system.cluster_metrics WHERE name = $1`,
			"test.upsert",
		).Scan(&value)
		require.Equal(t, int64(15), value)
	})

	t.Run("multiple metrics", func(t *testing.T) {
		env := newSQLStoreTestEnv(t)
		c := NewCounter(metric.Metadata{Name: "test.multi.counter"})
		g := NewGauge(metric.Metadata{Name: "test.multi.gauge"})
		c.Inc(7)
		g.Update(200)

		err := env.store.Write(env.ctx, []Metric{c, g})
		require.NoError(t, err)

		var count int
		env.runner.QueryRow(t,
			`SELECT count(*) FROM system.cluster_metrics WHERE name LIKE 'test.multi.%'`,
		).Scan(&count)
		require.Equal(t, 2, count)
	})
}
