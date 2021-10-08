// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	tableNameFunc := hideNonVirtualTableNameFunc(vt)

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
