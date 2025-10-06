// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNormalizeAndValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

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
	bazDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "baz")
	bopDesc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "bop")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	for _, tc := range []struct {
		name         string
		desc         catalog.TableDescriptor
		stmt         string
		expectErr    string
		expectStmt   string
		splitColFams bool
	}{
		{
			name:      "reject multiple tables",
			desc:      fooDesc,
			stmt:      "SELECT * FROM foo, other.foo",
			expectErr: "expected 1 table",
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
			expectErr:    `requires WITH split_column_families`,
			splitColFams: false,
		},
		{
			name: "UDTs fully qualified",
			desc: fooDesc,
			stmt: "SELECT *, 'inactive':::status FROM foo AS bar WHERE status = 'open':::status",
			expectStmt: "SELECT *, 'inactive':::public.status " +
				"FROM foo AS bar WHERE status = 'open':::public.status",
			splitColFams: false,
		},
		{
			name:         "can cast to standard type",
			desc:         fooDesc,
			stmt:         "SELECT 'cast'::string, 'type_annotation':::string FROM foo AS bar",
			expectStmt:   "SELECT 'cast'::STRING, 'type_annotation':::STRING FROM foo AS bar",
			splitColFams: false,
		},
		{
			name:         "can target one column family",
			desc:         bazDesc,
			stmt:         "SELECT a, b FROM baz",
			expectStmt:   "SELECT a, b FROM baz",
			splitColFams: false,
		},
		{
			name:         "SELECT a, b FROM bop",
			desc:         bopDesc,
			stmt:         "SELECT a, b FROM bop",
			expectStmt:   "SELECT a, b FROM bop",
			splitColFams: false,
		},
		{
			name:         "SELECT a, c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT a, c FROM baz",
			expectStmt:   "SELECT a, c FROM baz",
			splitColFams: false,
		},
		{
			name:         "SELECT b, b+1 AS c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT b, b+1 AS c FROM baz",
			expectStmt:   "SELECT b, b + 1 AS c FROM baz",
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
			expectStmt:   "SELECT b FROM baz",
			splitColFams: false,
		},
		{
			name:         "SELECT baz.b FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT baz.b FROM baz",
			expectStmt:   "SELECT baz.b FROM baz",
			splitColFams: false,
		},
		{
			name:       "SELECT baz.b, row_to_json((cdc_prev).*) FROM baz",
			desc:       bazDesc,
			stmt:       "SELECT baz.b, row_to_json((cdc_prev).*) FROM baz",
			expectStmt: "SELECT baz.b, row_to_json((cdc_prev).*) FROM baz",
			// Currently, accessing cdc_prev.* is treated in such a way as to require
			// split column families option.  This might not be needed since the above
			// expression targets main column family only. Perhaps this restriction
			// can be relaxed.
			splitColFams: true,
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
			expectStmt:   "SELECT b::STRING = 'c' FROM baz",
			splitColFams: false,
		},
		{
			name:         "SELECT *, c FROM baz",
			desc:         bazDesc,
			stmt:         "SELECT *, c FROM baz",
			expectErr:    `requires WITH split_column_families`,
			splitColFams: false,
		},
		{
			name:         "no explicit column references",
			desc:         bazDesc,
			stmt:         "SELECT pi() FROM baz",
			expectStmt:   "SELECT pi() FROM baz",
			splitColFams: false,
		},
		{
			name:      "cdc_prev is not a function",
			desc:      fooDesc,
			stmt:      "SELECT *, cdc_prev() FROM foo AS bar",
			expectErr: `unknown function: cdc_prev()`,
		},
		{ // Regression for #115245
			name:      "changefeed_creation_timestamp",
			desc:      fooDesc,
			stmt:      "SELECT * FROM foo WHERE changefeed_creation_timestamp() != changefeed_creation_timestamp()",
			expectErr: `does not match any rows`,
		},
		{ // Regression for #115245
			name: "changefeed_creation_timestamp",
			desc: fooDesc,
			stmt: fmt.Sprintf("SELECT * FROM foo WHERE changefeed_creation_timestamp() = %s",
				tree.AsStringWithFlags(eval.TimestampToDecimalDatum(hlc.Timestamp{WallTime: 42}), tree.FmtExport)),
			expectErr: `does not match any rows`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			if err != nil {
				require.NotEmpty(t, tc.expectErr, "expected no error, got %s", err)
				require.Regexp(t, tc.expectErr, err)
				return
			}
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
			}

			schemaTS := s.Clock().Now()
			norm, _, _, err := normalizeAndPlan(
				ctx, &execCfg, username.RootUserName(), defaultDBSessionData, tc.desc, schemaTS,
				target, sc, tc.splitColFams,
			)

			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
				return
			}

			require.NoError(t, err)
			serialized := AsStringUnredacted(norm)
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

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE table foo (id int primary key, s string)`)
	sqlDB.Exec(t, `CREATE table cdc_prev (id int primary key, cdc_prev string)`)
	sqlDB.Exec(t, `CREATE table cdc_prev_bad (id int primary key, cdc_prev string, crdb_internal_cdc_prev string)`)

	descs := make(map[string]catalog.TableDescriptor)
	for _, name := range []string{`foo`, `cdc_prev`, `cdc_prev_bad`} {
		descs[name] = cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), tree.Name(name))
	}

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	for _, tc := range []struct {
		name         string
		desc         catalog.TableDescriptor
		stmt         string
		requiresPrev bool
		expectErr    string
	}{
		{
			name:         "top level call to cdc_prev",
			desc:         descs[`foo`],
			stmt:         "SELECT row_to_json((cdc_prev).*) from foo",
			requiresPrev: true,
		},
		{
			name:         "nested call to cdc_prev",
			desc:         descs[`foo`],
			stmt:         "SELECT jsonb_build_object('op',IF(event_op() = 'delete','u',IF(row_to_json((cdc_prev).*)::string='null','c','u'))) from foo",
			requiresPrev: true,
		},
		{
			name:         "cdc_prev in the stmt",
			desc:         descs[`foo`],
			stmt:         "SELECT * from foo WHERE (cdc_prev).s != s",
			requiresPrev: true,
		},
		{
			name:         "cdc_prev case insensitive",
			desc:         descs[`foo`],
			stmt:         "SELECT row_to_json((CdC_pReV).*) from foo",
			requiresPrev: true,
		},
		{
			name:         "contains misleading substring",
			desc:         descs[`foo`],
			stmt:         "SELECT 'cdc_prev()', s FROM foo",
			requiresPrev: false,
		},
		{
			// cdc_prev table has cdc_prev column; attempt to access
			// cdc_prev.cdc_prev column -- this is not the same as access
			// to cdc_prev tuple (previous row).
			name:         "cdc_prev table with cdc_prev column",
			desc:         descs[`cdc_prev`],
			stmt:         "SELECT * FROM cdc_prev WHERE cdc_prev='value'",
			requiresPrev: false,
		},
		{
			// Try to access cdc_prev as if it were a tuple; it's not, so we should get
			// an error.
			name:      "cdc_prev table with cdc_prev column: cdc_prev is not a tuple",
			desc:      descs[`cdc_prev`],
			stmt:      "SELECT * FROM cdc_prev WHERE (cdc_prev).cdc_prev='value'",
			expectErr: "type string is not composite",
		},
		{
			// When table has cdc_prev column, previous row state exposed
			// via crdb_internal_cdc_prev tuple.
			name:         "cdc_prev table with cdc_prev column: cdc_prev is not a tuple",
			desc:         descs[`cdc_prev`],
			stmt:         "SELECT * FROM cdc_prev WHERE cdc_prev='value' AND (crdb_internal_cdc_prev).cdc_prev='prev value'",
			requiresPrev: true,
		},
		{
			name:         "misleading table name with alias",
			desc:         descs[`cdc_prev`],
			stmt:         "SELECT * FROM cdc_prev AS real_prev",
			requiresPrev: false,
		},
		{
			name:      "table has both cdc_prev and crdb_internal_cdc_prev",
			desc:      descs[`cdc_prev_bad`],
			stmt:      "SELECT cdc_prev FROM cdc_prev_bad",
			expectErr: "Either cdc_prev or crdb_internal_cdc_prev columns must be renamed or dropped",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseChangefeedExpression(tc.stmt)
			require.NoError(t, err)
			target := jobspb.ChangefeedTargetSpecification{
				TableID:           tc.desc.GetID(),
				StatementTimeName: tc.desc.GetName(),
			}

			schemaTS := s.Clock().Now()
			_, withDiff, _, err := normalizeAndPlan(ctx, &execCfg, username.RootUserName(),
				defaultDBSessionData, tc.desc, schemaTS, target, sc, false)

			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.requiresPrev, withDiff)
			}
		})
	}
}
