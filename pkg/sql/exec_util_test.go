// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestHideNonVirtualTableNameFunc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := cluster.MakeTestingClusterSettings()
	vt, err := NewVirtualSchemaHolder(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	tableNameFunc := hideNonVirtualTableNameFunc(vt, nil)

	testData := []struct {
		stmt     string
		expected string
	}{
		{`SELECT * FROM a.b`,
			`SELECT * FROM _`},
		{`SELECT * FROM pg_type`,
			`SELECT * FROM _`},
		{`SELECT * FROM pg_catalog.pg_type`,
			`SELECT * FROM pg_catalog.pg_type`},
	}

	for _, test := range testData {
		stmt, err := parser.ParseOne(test.stmt)
		if err != nil {
			t.Fatal(err)
		}
		f := tree.NewFmtCtx(
			tree.FmtSimple,
			tree.FmtReformatTableNames(tableNameFunc),
		)
		f.FormatNode(stmt.AST)
		actual := f.CloseAndGetString()
		require.Equal(t, test.expected, actual)
	}
}

func TestMaybeHashAppName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	secret := "secret"

	for _, tc := range []struct {
		name     string
		appName  string
		expected string
	}{
		{
			"basic app name is hashed",
			"my_app",
			"a4bc27b7",
		},
		{
			"internal app name is not hashed",
			"$ internal_app",
			"$ internal_app",
		},
		{
			"delegated app name is hashed",
			"$$ my_app",
			"$$ 9a7e689f",
		},
		{
			"delegated and reportable app name is not hashed",
			"$$ $ internal_app",
			"$$ $ internal_app",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := MaybeHashAppName(tc.appName, secret)
			require.Equal(t, tc.expected, out)
		})
	}
}

// TestSessionDefaultsSafeFormat tests the redacted output of SessionDefaults.
func TestSessionDefaultsSafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	session := SessionDefaults(make(map[string]string))
	session["database"] = "test"
	session["statement_timeout"] = "250ms"
	session["disallow_full_table_scans"] = "true"
	require.Contains(t, redact.Sprint(session), "database=‹test›")
	require.Contains(t, redact.Sprint(session).Redact(), "statement_timeout=‹×›")
}
