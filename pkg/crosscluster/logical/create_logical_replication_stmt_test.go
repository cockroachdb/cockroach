// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestResolveDestinationObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDeadlock(t)
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE db1")
	sqlDB.Exec(t, "CREATE SCHEMA db1.sc1")
	sqlDB.Exec(t, "CREATE SCHEMA db1.sc2")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.t1 (a INT PRIMARY KEY)")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.t2 (a INT PRIMARY KEY)")
	sqlDB.Exec(t, "CREATE TABLE t3 (a INT PRIMARY KEY)")

	sqlUser, err := username.MakeSQLUsernameFromUserInput("root", username.PurposeValidation)
	require.NoError(t, err)

	resolveObjects := func(destResources tree.LogicalReplicationResources, createTables bool) (resolved ResolvedDestObjects, err error) {
		err = sqltestutils.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, _ *descs.Collection) error {
			opName := redact.SafeString("resolve")
			sessionData := sql.NewInternalSessionData(ctx, execCfg.Settings, opName)
			sessionData.Database = "defaultdb"
			planner, close := sql.NewInternalPlanner(
				opName,
				txn.KV(),
				sqlUser,
				&sql.MemoryMetrics{},
				&execCfg,
				sessionData,
			)
			defer close()
			resolved, err = resolveDestinationObjects(ctx, planner.(resolver.SchemaResolver), sessionData, destResources, createTables)
			return err
		})
		return resolved, err
	}

	res := func(db string, tables ...string) tree.LogicalReplicationResources {
		resources := tree.LogicalReplicationResources{
			Database: tree.Name(db),
		}
		for _, table := range tables {
			t := strings.Split(table, ".")
			resources.Tables = append(resources.Tables, tree.NewUnresolvedName(t...))
		}
		return resources
	}

	type testCase struct {
		name         string
		resources    tree.LogicalReplicationResources
		create       bool
		expectedDesc string
		expectedErr  string
	}

	for _, tc := range []testCase{
		{
			name:         "single",
			resources:    res("", "db1.sc1.t1"),
			expectedDesc: "db1.sc1.t1",
		},
		{
			name:         "single/create",
			resources:    res("", "db1.sc1.t1_c"),
			create:       true,
			expectedDesc: "db1.sc1.t1_c",
		},
		{
			name:         "implicit_schema",
			resources:    res("", "defaultdb.t3"),
			expectedDesc: "defaultdb.public.t3",
		},
		{
			name:         "implicit_schema/create",
			resources:    res("", "defaultdb.t3_c"),
			create:       true,
			expectedDesc: "defaultdb.public.t3_c",
		},
		{
			name:         "implicit_db",
			resources:    res("", "t3"),
			expectedDesc: "defaultdb.public.t3",
		},
		{
			name:         "implicit_db/create",
			resources:    res("", "t3_c"),
			create:       true,
			expectedDesc: "defaultdb.public.t3_c",
		},
		{
			name:         "multi",
			resources:    res("", "db1.sc1.t1", "db1.sc1.t2"),
			expectedDesc: "db1.sc1.t1, db1.sc1.t2",
		},
		{
			name:         "multi/create",
			resources:    res("", "db1.sc1.t1_c", "db1.sc1.t2_c"),
			create:       true,
			expectedDesc: "db1.sc1.t1_c, db1.sc1.t2_c",
		},
		{
			name:        "missing_schema",
			resources:   res("", "db1.s2.t1"),
			expectedErr: "failed to find existing destination table db1.s2.t1",
		},
		{
			name:        "missing_db",
			resources:   res("", "db2.t1"),
			expectedErr: "failed to find existing destination table db2.t1",
		},
		{
			name:        "missing_schema/create",
			resources:   res("", "db1.s2.t1"),
			create:      true,
			expectedErr: "database or schema not found for destination table db1.s2.t1",
		},
		{
			name:        "missing_db/create",
			resources:   res("", "db2.t1"),
			create:      true,
			expectedErr: "database or schema not found for destination table db2.t1",
		},
		{
			name:         "multiple_db_target",
			resources:    res("", "t3", "db1.sc1.t1"),
			expectedDesc: "defaultdb.public.t3, db1.sc1.t1",
		},
		{
			name:        "multiple_db_target/create",
			resources:   res("", "t3_c", "db1.sc1.t1_c"),
			create:      true,
			expectedErr: "destination tables must all be in the same database",
		},
		{
			name:        "multiple_schema_target/create",
			resources:   res("", "db1.sc2.t2_c", "db1.sc1.t1_c"),
			create:      true,
			expectedErr: "destination tables must all be in the same schema",
		},
		{
			name:        "existing_table/create",
			resources:   res("", "db1.sc1.t1"),
			create:      true,
			expectedErr: "destination table db1.sc1.t1 already exists",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolved, err := resolveObjects(tc.resources, tc.create)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedDesc, resolved.TargetDescription())
			if tc.create {
				require.NotZero(t, resolved.ParentDatabaseID)
				require.NotZero(t, resolved.ParentSchemaID)
			} else {
				require.Zero(t, resolved.ParentDatabaseID)
				require.Zero(t, resolved.ParentSchemaID)
			}
		})

	}

}
