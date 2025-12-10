// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStatementAllowsDistributedMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name   string
		stmt   string
		expect bool
	}{
		{
			name:   "non-unique create index",
			stmt:   "CREATE INDEX idx ON t(a)",
			expect: true,
		},
		{
			name:   "create unique index",
			stmt:   "CREATE UNIQUE INDEX idx ON t(a)",
			expect: false,
		},
		{
			name:   "alter table add unique constraint",
			stmt:   "ALTER TABLE t ADD CONSTRAINT c UNIQUE (a)",
			expect: false,
		},
		{
			name:   "alter table add unique without index",
			stmt:   "ALTER TABLE t ADD CONSTRAINT c UNIQUE WITHOUT INDEX (a)",
			expect: true,
		},
		{
			name:   "alter table alter primary key",
			stmt:   "ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (a)",
			expect: false,
		},
		{
			name:   "alter table add column with unique",
			stmt:   "ALTER TABLE t ADD COLUMN b INT UNIQUE",
			expect: false,
		},
		{
			name:   "alter table add column with primary key",
			stmt:   "ALTER TABLE t ADD COLUMN b INT PRIMARY KEY",
			expect: false,
		},
		{
			name:   "alter table add column without unique",
			stmt:   "ALTER TABLE t ADD COLUMN b INT",
			expect: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseOne(tc.stmt)
			require.NoError(t, err)
			actual := StatementAllowsDistributedMerge(parsed.AST)
			require.Equal(t, tc.expect, actual)
		})
	}
}
