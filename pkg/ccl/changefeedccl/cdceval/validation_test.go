// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNormalizeAndValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t,
		`CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`,
		`CREATE TYPE unused AS ENUM ('do not use')`,
		`CREATE SCHEMA alt`,
		`CREATE TYPE alt.status AS ENUM ('alt_open', 'alt_closed', 'alt_inactive')`,
		`CREATE TYPE alt.unused AS ENUM ('really', 'do', 'not', 'use')`,
		`CREATE TABLE foo (a INT PRIMARY KEY, status status, alt alt.status)`,
		`CREATE DATABASE other`,
		`CREATE TABLE other.foo (a INT)`,
	)

	fooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	otherFooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "other", "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	p, cleanup := sql.NewInternalPlanner("test",
		kvDB.NewTxn(ctx, "test-planner"),
		username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
		sessiondatapb.SessionData{
			Database:   "defaultdb",
			SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
		})
	defer cleanup()
	execCtx := p.(sql.JobExecContext)

	for _, tc := range []struct {
		name       string
		desc       catalog.TableDescriptor
		stmt       string
		expectErr  string
		expectStmt string
	}{
		{
			name:      "reject multiple tables",
			desc:      fooDesc,
			stmt:      "SELECT * FROM foo, other.foo",
			expectErr: "invalid CDC expression: only 1 table supported",
		},
		{
			name:      "reject contradiction",
			desc:      fooDesc,
			stmt:      "SELECT * FROM foo WHERE a IS NULL",
			expectErr: `filter "a IS NULL" is a contradiction`,
		},
		{
			name:      "enum must be referenced",
			desc:      fooDesc,
			stmt:      "SELECT 'open'::status, 'do not use':::unused FROM foo",
			expectErr: `use of user defined types not references by target table is not supported`,
		},
		{
			name:       "replaces table name with ref",
			desc:       fooDesc,
			stmt:       "SELECT * FROM foo",
			expectStmt: fmt.Sprintf("SELECT * FROM [%d AS foo]", fooDesc.GetID()),
		},
		{
			name:       "replaces table name with other.ref",
			desc:       otherFooDesc,
			stmt:       "SELECT * FROM other.foo",
			expectStmt: fmt.Sprintf("SELECT * FROM [%d AS foo]", otherFooDesc.GetID()),
		},
		{
			name:       "replaces table name with ref aliased",
			desc:       fooDesc,
			stmt:       "SELECT * FROM foo AS bar",
			expectStmt: fmt.Sprintf("SELECT * FROM [%d AS bar]", fooDesc.GetID()),
		},
		{
			name: "UDTs fully qualified",
			desc: fooDesc,
			stmt: "SELECT *, 'inactive':::status FROM foo AS bar WHERE status = 'open':::status",
			expectStmt: fmt.Sprintf(
				"SELECT *, 'inactive':::defaultdb.public.status "+
					"FROM [%d AS bar] WHERE status = 'open':::defaultdb.public.status",
				fooDesc.GetID()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			require.NoError(t, err)
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
			}

			err = NormalizeAndValidateSelectForTarget(ctx, execCtx, tc.desc, target, sc, false)
			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
				return
			}

			require.NoError(t, err)
			serialized := AsStringUnredacted(sc)
			log.Infof(context.Background(), "DEBUG: %s", tree.StmtDebugString(sc))
			log.Infof(context.Background(), "Serialized: %s", serialized)
			require.Equal(t, tc.expectStmt, serialized)

			// Make sure we can deserialize back.
			_, err = ParseChangefeedExpression(serialized)
			require.NoError(t, err)
		})
	}
}
