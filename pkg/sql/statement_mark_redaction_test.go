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
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestMarkRedactionStatement verifies that the redactable parts
// of statements are marked correctly.
func TestMarkRedactionStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		query    string
		expected string
	}{
		{
			"SELECT 1",
			"SELECT ‹1›",
		},
		{
			"SELECT * FROM t",
			"SELECT * FROM ‹\"\"›.‹\"\"›.‹t›",
		},
		{
			"CREATE TABLE defaultdb.public.t()",
			"CREATE TABLE ‹defaultdb›.public.‹t› ()",
		},
		{
			"SELECT lower('foo')",
			"SELECT ‹lower›(‹'foo'›)",
		},
		{
			"SELECT crdb_internal.node_executable_version()",
			"SELECT crdb_internal.node_executable_version()",
		},
		{
			"SHOW database",
			"SHOW database",
		},
		{
			"SET database = defaultdb",
			"SET database = ‹defaultdb›",
		},
		{
			"SET TRACING = off",
			"SET TRACING = off",
		},
		{
			"SHOW CLUSTER SETTING sql.log.admin_audit.enabled",
			"SHOW CLUSTER SETTING \"sql.log.admin_audit.enabled\"",
		},
		{
			"SET CLUSTER SETTING sql.log.admin_audit.enabled = true",
			"SET CLUSTER SETTING \"sql.log.admin_audit.enabled\" = true",
		},
	}

	s := cluster.MakeTestingClusterSettings()
	vt, err := NewVirtualSchemaHolder(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range testCases {
		stmt, err := parser.ParseOne(test.query)
		if err != nil {
			t.Fatal(err)
		}
		ann := tree.MakeAnnotations(stmt.NumAnnotations)
		f := tree.NewFmtCtx(
			tree.FmtAlwaysQualifyTableNames|tree.FmtMarkRedactionNode,
			tree.FmtAnnotations(&ann),
			tree.FmtReformatTableNames(hideNonVirtualTableNameFunc(vt, nil)))
		f.FormatNode(stmt.AST)
		redactedString := f.CloseAndGetString()
		require.Equal(t, test.expected, redactedString)
	}
}
