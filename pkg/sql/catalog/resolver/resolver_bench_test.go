// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resolver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkResolveExistingObject exercises resolver code to ensure that
// we do not regress in this important hot path.
func BenchmarkResolveExistingObject(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	for _, tc := range []struct {
		testName   string
		setup      []string
		name       tree.UnresolvedName
		flags      tree.ObjectLookupFlags
		searchPath string
	}{
		{
			testName: "basic",
			setup: []string{
				"CREATE TABLE foo ()",
			},
			name:  tree.MakeUnresolvedName("foo"),
			flags: tree.ObjectLookupFlags{Required: true},
		},
		{
			testName: "in schema, explicit",
			setup: []string{
				"CREATE SCHEMA sc",
				"CREATE TABLE sc.foo ()",
			},
			name:  tree.MakeUnresolvedName("sc", "foo"),
			flags: tree.ObjectLookupFlags{Required: true},
		},
		{
			testName: "in schema, implicit",
			setup: []string{
				"CREATE SCHEMA sc",
				"CREATE TABLE sc.foo ()",
			},
			name:       tree.MakeUnresolvedName("foo"),
			flags:      tree.ObjectLookupFlags{Required: true},
			searchPath: "public,$user,sc",
		},
	} {
		b.Run(tc.testName, func(b *testing.B) {
			tc.flags.DesiredObjectKind = tree.TableObject
			ctx := context.Background()
			s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tDB := sqlutils.MakeSQLRunner(sqlDB)
			for _, stmt := range tc.setup {
				tDB.Exec(b, stmt)
			}

			var m sessiondatapb.MigratableSession
			var sessionSerialized []byte
			tDB.QueryRow(b, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
			require.NoError(b, protoutil.Unmarshal(sessionSerialized, &m))
			sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
			require.NoError(b, err)
			sd.SessionData = m.SessionData
			sd.LocalOnlySessionData = m.LocalOnlySessionData

			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			txn := kvDB.NewTxn(ctx, "test")
			p, cleanup := sql.NewInternalPlanner("asdf", txn, username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg, sd)
			defer cleanup()

			// The internal planner overrides the database to "system", here we
			// change it back.
			{
				ec := p.(interface{ EvalContext() *eval.Context }).EvalContext()
				require.NoError(b, ec.SessionAccessor.SetSessionVar(
					ctx, "database", "defaultdb", false,
				))

				if tc.searchPath != "" {
					require.NoError(b, ec.SessionAccessor.SetSessionVar(
						ctx, "search_path", tc.searchPath, false,
					))
				}
			}

			rs := p.(resolver.SchemaResolver)
			uon, err := tc.name.ToUnresolvedObjectName(0)
			require.NoError(b, err)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desc, _, err := resolver.ResolveExistingObject(ctx, rs, &uon, tc.flags)
				require.NoError(b, err)
				require.NotNil(b, desc)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkResolveFunction exercises resolver code to ensure that
// we do not regress in this important hot path.
func BenchmarkResolveFunction(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	for _, tc := range []struct {
		testName   string
		setup      []string
		name       tree.UnresolvedName
		flags      tree.ObjectLookupFlags
		searchPath string
	}{
		{
			testName: "basic",
			setup: []string{
				"CREATE FUNCTION foo() RETURNS int IMMUTABLE LANGUAGE SQL AS $$ SELECT 1 $$",
			},
			name:  tree.MakeUnresolvedName("foo"),
			flags: tree.ObjectLookupFlags{Required: true},
		},
		{
			testName: "in schema, explicit",
			setup: []string{
				"CREATE SCHEMA sc",
				"CREATE FUNCTION sc.foo() RETURNS int IMMUTABLE LANGUAGE SQL AS $$ SELECT 1 $$",
			},
			name:  tree.MakeUnresolvedName("sc", "foo"),
			flags: tree.ObjectLookupFlags{Required: true},
		},
		{
			testName: "in schema, implicit",
			setup: []string{
				"CREATE SCHEMA sc",
				"CREATE FUNCTION sc.foo() RETURNS int IMMUTABLE LANGUAGE SQL AS $$ SELECT 1 $$",
			},
			name:       tree.MakeUnresolvedName("foo"),
			flags:      tree.ObjectLookupFlags{Required: true},
			searchPath: "public,$user,sc",
		},
	} {
		b.Run(tc.testName, func(b *testing.B) {
			ctx := context.Background()
			s, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			tDB := sqlutils.MakeSQLRunner(sqlDB)
			for _, stmt := range tc.setup {
				tDB.Exec(b, stmt)
			}

			var m sessiondatapb.MigratableSession
			var sessionSerialized []byte
			tDB.QueryRow(b, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
			require.NoError(b, protoutil.Unmarshal(sessionSerialized, &m))
			sd, err := sessiondata.UnmarshalNonLocal(m.SessionData)
			require.NoError(b, err)
			sd.SessionData = m.SessionData
			sd.LocalOnlySessionData = m.LocalOnlySessionData

			execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
			txn := kvDB.NewTxn(ctx, "test")
			p, cleanup := sql.NewInternalPlanner("asdf", txn, username.NodeUserName(), &sql.MemoryMetrics{}, &execCfg, sd)
			defer cleanup()

			// The internal planner overrides the database to "system", here we
			// change it back.
			var sp tree.SearchPath
			{
				ec := p.(interface{ EvalContext() *eval.Context }).EvalContext()
				require.NoError(b, ec.SessionAccessor.SetSessionVar(
					ctx, "database", "defaultdb", false,
				))

				if tc.searchPath != "" {
					require.NoError(b, ec.SessionAccessor.SetSessionVar(
						ctx, "search_path", tc.searchPath, false,
					))
				}
				sp = &ec.SessionData().SearchPath
			}

			rs := p.(resolver.SchemaResolver)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fd, err := rs.ResolveFunction(ctx, tree.MakeUnresolvedFunctionName(&tc.name), sp)
				require.NoError(b, err)
				require.NotNil(b, fd)
			}
			b.StopTimer()
		})
	}
}
