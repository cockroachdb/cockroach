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
