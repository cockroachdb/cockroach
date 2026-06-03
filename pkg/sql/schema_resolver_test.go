// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// This benchmark tests the performance of resolving an enum with many (10,000)
// values. This is a regression test for #109228.
func BenchmarkResolveTypeByOID(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	query := strings.Builder{}
	query.WriteString("CREATE TYPE typ AS ENUM ('v0'")
	for i := 1; i < 10_000; i++ {
		query.WriteString(", 'v")
		query.WriteString(strconv.Itoa(i))
		query.WriteString("'")
	}
	query.WriteString(")")
	_, err := sqlDB.Exec(query.String())
	require.NoError(b, err)

	var typOID uint32
	err = sqlDB.QueryRow("SELECT 'typ'::regtype::oid").Scan(&typOID)
	require.NoError(b, err)

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "defaultdb"
	planner, cleanup := newInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.NodeUserName(), &MemoryMetrics{}, &execCfg, sd,
	)
	defer cleanup()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		typ, err := planner.schemaResolver.ResolveTypeByOID(ctx, oid.Oid(typOID))
		require.NoError(b, err)
		require.Equal(b, "typ", typ.Name())
	}
	b.StopTimer()
}

// TestResolvedTypeCacheClearedAcrossStatements verifies that planner.resetPlanner
// clears the resolved-type cache between statements. Without it, an enum hydrated
// before ALTER TYPE ... ADD VALUE would persist and reject the new value.
func TestResolvedTypeCacheClearedAcrossStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Pin a single connection so all statements share one planner; a missing
	// cache reset only manifests when the cache survives across statements.
	conn, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	exec := func(stmt string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, stmt)
		require.NoError(t, err)
	}

	exec("CREATE TYPE typ AS ENUM ('a', 'b')")
	exec("CREATE TABLE t (k INT PRIMARY KEY, v typ)")

	// Resolve and cache typ by OID: planning the insert type-checks the
	// synthesized `v IN ('a', 'b')` enum check constraint.
	exec("INSERT INTO t VALUES (1, 'a')")

	// Use the new value on the same session; a stale cached typ would reject it.
	exec("ALTER TYPE typ ADD VALUE 'c'")
	exec("INSERT INTO t VALUES (2, 'c')")

	var n int
	require.NoError(t,
		conn.QueryRowContext(ctx, "SELECT count(*) FROM t WHERE v = 'c'").Scan(&n))
	require.Equal(t, 1, n)
}

// TestResolveTypeByOIDCaching exercises the canCache guard in
// schemaResolver.ResolveTypeByOID directly: the default resolution path serves
// repeat lookups of the same OID from the cache, while the skipDescriptorCache
// and database-restricted paths bypass it (a cache shared across the restricted
// path would be unsound, since it applies WithoutOtherParent).
func TestResolveTypeByOIDCaching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec("CREATE TYPE typ AS ENUM ('a', 'b')")
	require.NoError(t, err)
	_, err = sqlDB.Exec("CREATE TYPE typ2 AS ENUM ('x', 'y')")
	require.NoError(t, err)
	var typOID, typ2OID uint32
	require.NoError(t, sqlDB.QueryRow("SELECT 'typ'::regtype::oid").Scan(&typOID))
	require.NoError(t, sqlDB.QueryRow("SELECT 'typ2'::regtype::oid").Scan(&typ2OID))
	var dbID uint32
	require.NoError(t, sqlDB.QueryRow(
		`SELECT id FROM system.namespace WHERE name = 'defaultdb' AND "parentID" = 0`).Scan(&dbID))

	execCfg := s.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "defaultdb"
	p, cleanup := newInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.NodeUserName(), &MemoryMetrics{}, &execCfg, sd,
	)
	defer cleanup()
	sr := &p.schemaResolver

	t.Run("caches repeat lookups on the default path", func(t *testing.T) {
		sr.resolvedTypesByOID = nil
		// A cache hit returns the very same instance; without caching each call
		// hydrates a fresh *types.T.
		typ1, err := sr.ResolveTypeByOID(ctx, oid.Oid(typOID))
		require.NoError(t, err)
		typ1Again, err := sr.ResolveTypeByOID(ctx, oid.Oid(typOID))
		require.NoError(t, err)
		require.Same(t, typ1, typ1Again)

		// A distinct OID is cached as its own instance, not aliased to the first.
		typ2, err := sr.ResolveTypeByOID(ctx, oid.Oid(typ2OID))
		require.NoError(t, err)
		require.NotSame(t, typ1, typ2)
		require.Equal(t, "typ2", typ2.Name())
	})

	t.Run("bypasses cache when skipDescriptorCache is set", func(t *testing.T) {
		sr.resolvedTypesByOID = nil
		sr.runWithOptions(resolveFlags{skipCache: true}, func() {
			_, err := sr.ResolveTypeByOID(ctx, oid.Oid(typOID))
			require.NoError(t, err)
		})
		require.Empty(t, sr.resolvedTypesByOID)
	})

	t.Run("bypasses cache when restricted to a database", func(t *testing.T) {
		sr.resolvedTypesByOID = nil
		sr.runWithOptions(resolveFlags{contextDatabaseID: descpb.ID(dbID)}, func() {
			_, err := sr.ResolveTypeByOID(ctx, oid.Oid(typOID))
			require.NoError(t, err)
		})
		require.Empty(t, sr.resolvedTypesByOID)
	})
}
