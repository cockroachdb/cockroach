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
		`CREATE TABLE baz (a INT PRIMARY KEY, b INT, c STRING, FAMILY most (a, b), FAMILY only_c (c))`,
		`CREATE TABLE bop (a INT, b INT, c STRING, FAMILY most (a, b), FAMILY only_c (c), primary key (a, b))`,
	)

	fooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "foo")
	otherFooDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "other", "foo")
	bazDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "baz")
	bopDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "bop")

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
		name         string
		desc         catalog.TableDescriptor
		stmt         string
		expectErr    string
		expectStmt   string
		splitColFams bool
	}{
		{
			name:         "reject multiple tables",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo, other.foo",
			expectErr:    "invalid CDC expression: only 1 table supported",
			splitColFams: false,
		},
		{
			name:         "reject contradiction",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo WHERE a IS NULL",
			expectErr:    `filter "a IS NULL" is a contradiction`,
			splitColFams: false,
		},
		{
			name:         "enum must be referenced",
			desc:         fooDesc,
			stmt:         "SELECT 'open'::status, 'do not use':::unused FROM foo",
			expectErr:    `use of user defined types not referenced by target table is not supported`,
			splitColFams: false,
		},
		{
			name:         "reject multiple column families",
			desc:         bazDesc,
			stmt:         "SELECT a, b, c FROM baz",
			expectErr:    `expressions can't reference columns from more than one column family`,
			splitColFams: false,
		},
		{
			name:         "reject multiple column families with star",
			desc:         bazDesc,
			stmt:         "SELECT * FROM baz",
			expectErr:    `targeting a table with multiple column families requires WITH split_column_families and will emit multiple events per row.`,
			splitColFams: false,
		},
		{
			name:         "replaces table name with ref",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo",
			expectStmt:   fmt.Sprintf("SELECT * FROM [%d AS foo]", fooDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "replaces table name with other.ref",
			desc:         otherFooDesc,
			stmt:         "SELECT * FROM other.foo",
			expectStmt:   fmt.Sprintf("SELECT * FROM [%d AS foo]", otherFooDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "replaces table name with ref aliased",
			desc:         fooDesc,
			stmt:         "SELECT * FROM foo AS bar",
			expectStmt:   fmt.Sprintf("SELECT * FROM [%d AS bar]", fooDesc.GetID()),
			splitColFams: false,
		},
		{
			name: "UDTs fully qualified",
			desc: fooDesc,
			stmt: "SELECT *, 'inactive':::status FROM foo AS bar WHERE status = 'open':::status",
			expectStmt: fmt.Sprintf(
				"SELECT *, 'inactive':::defaultdb.public.status "+
					"FROM [%d AS bar] WHERE status = 'open':::defaultdb.public.status",
				fooDesc.GetID()),
			splitColFams: false,
		},
		{
			name: "can cast to standard type",
			desc: fooDesc,
			stmt: "SELECT 'cast'::string, 'type_annotation':::string FROM foo AS bar",
			expectStmt: fmt.Sprintf(
				"SELECT 'cast'::STRING, 'type_annotation':::STRING FROM [%d AS bar]",
				fooDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "can target one column family",
			desc:         bazDesc,
			stmt:         "SELECT a, b FROM baz",
			expectStmt:   fmt.Sprintf("SELECT a, b FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT a, b FROM bop",
			desc:         bopDesc,
			stmt:         "SELECT a, b FROM bop",
			expectStmt:   fmt.Sprintf("SELECT a, b FROM [%d AS bop]", bopDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT a, c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT a, c FROM baz",
			expectStmt:   fmt.Sprintf("SELECT a, c FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT b, b+1 AS c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT b, b+1 AS c FROM baz",
			expectStmt:   fmt.Sprintf("SELECT b, b + 1 AS c FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT b, c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT b, c FROM baz",
			expectErr:    `expressions can't reference columns from more than one column family`,
			splitColFams: false,
		},
		{
			name:         "SELECT B FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT B FROM baz",
			expectStmt:   fmt.Sprintf("SELECT b FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT baz.b FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT baz.b FROM baz",
			expectStmt:   fmt.Sprintf("SELECT baz.b FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT b FROM baz WHERE c IS NULL",
			desc:         bazDesc,
			stmt:         "SELECT b FROM baz WHERE c IS NULL",
			expectErr:    "expressions can't reference columns from more than one column family",
			splitColFams: false,
		},
		{
			name:         "SELECT b, substring(c,1,2) FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT b, substring(c,1,2) FROM baz",
			expectErr:    "expressions can't reference columns from more than one column family",
			splitColFams: false,
		},
		{
			name:         "SELECT b::string = 'c' FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT b::string = 'c' FROM baz",
			expectStmt:   fmt.Sprintf("SELECT b::STRING = 'c' FROM [%d AS baz]", bazDesc.GetID()),
			splitColFams: false,
		},
		{
			name:         "SELECT *, c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT *, c FROM baz",
			expectErr:    `can't reference non-primary key columns as well as star on a multi column family table`,
			splitColFams: false,
		},
		{
			name:         "SELECT * FROM baz WITH split_column_families",
			desc:         bazDesc,
			stmt:         "SELECT * FROM baz",
			expectErr:    `split_column_families is not supported with changefeed expressions yet`,
			splitColFams: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			require.NoError(t, err)
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
			}

			_, _, err = NormalizeAndValidateSelectForTarget(ctx, execCtx, tc.desc, target, sc, false, tc.splitColFams)
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

func TestSelectClauseRequiresPrev(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE table foo (id int primary key, s string)`)
	sqlDB.Exec(t, `CREATE table cdc_prev (id int primary key, s string)`)
	sqlDB.Exec(t, `CREATE table misleading_column_name (id int primary key, cdc_prev string)`)

	descs := make(map[string]catalog.TableDescriptor)
	for _, name := range []string{`foo`, `cdc_prev`, `misleading_column_name`} {
		descs[name] = cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(name))
	}

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
		name   string
		desc   catalog.TableDescriptor
		stmt   string
		expect bool
	}{
		{
			name:   "top level call to cdc_prev",
			desc:   descs[`foo`],
			stmt:   "SELECT cdc_prev() from foo",
			expect: true,
		},
		{
			name:   "nested call to cdc_prev",
			desc:   descs[`foo`],
			stmt:   "SELECT jsonb_build_object('op',IF(cdc_is_delete(),'u',IF(cdc_prev()::string='null','c','u'))) from foo",
			expect: true,
		},
		{
			name:   "cdc_prev in the predicate",
			desc:   descs[`foo`],
			stmt:   "SELECT * from foo WHERE (cdc_prev()->'s')::string != s",
			expect: true,
		},
		{
			name:   "case insensitive",
			desc:   descs[`foo`],
			stmt:   "SELECT CDC_PREV() from foo",
			expect: true,
		},
		{
			name:   "contains misleading substring",
			desc:   descs[`foo`],
			stmt:   "SELECT 'cdc_prev()', s FROM foo",
			expect: false,
		},
		{
			name:   "misleading table name",
			desc:   descs[`cdc_prev`],
			stmt:   "SELECT * FROM cdc_prev",
			expect: false,
		},
		{
			name:   "misleading column name",
			desc:   descs[`misleading_column_name`],
			stmt:   "SELECT cdc_prev FROM misleading_column_name",
			expect: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			require.NoError(t, err)
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
			}
			normalized, _, err := NormalizeAndValidateSelectForTarget(ctx, execCtx, tc.desc, target, sc, false, false)
			require.NoError(t, err)
			actual, err := SelectClauseRequiresPrev(*execCtx.SemaCtx(), normalized)
			require.NoError(t, err)
			require.Equal(t, tc.expect, actual)
		})
	}
}
