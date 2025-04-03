// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestIsAllowedLDRSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, tc := range []struct {
		stmt      string
		isAllowed bool
	}{
		{
			stmt:      "CREATE INDEX idx ON t (a)",
			isAllowed: true,
		},
		{
			stmt:      "ALTER INDEX idx NOT VISIBLE",
			isAllowed: true,
		},
		{
			stmt:      "ALTER INDEX idx CONFIGURE ZONE USING num_replicas = 3",
			isAllowed: true,
		},
		{
			stmt:      "ALTER INDEX idx RENAME TO idx2",
			isAllowed: true,
		},
		{
			stmt:      "CREATE UNIQUE INDEX idx ON t (a)",
			isAllowed: false,
		},
		{
			stmt:      "CREATE INDEX idx ON t (a) WHERE a > 10",
			isAllowed: false,
		},
		{
			stmt:      "DROP INDEX idx",
			isAllowed: true,
		},
		{
			stmt:      "ALTER TABLE t ALTER COLUMN a SET DEFAULT 10",
			isAllowed: true,
		},
		{
			stmt:      "ALTER TABLE t ALTER COLUMN a DROP DEFAULT",
			isAllowed: true,
		},
		{
			stmt:      "ALTER TABLE t ALTER COLUMN a SET NOT VISIBLE",
			isAllowed: true,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT NULL",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT DEFAULT 10",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT NOT NULL",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT, DROP COLUMN b",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t ADD COLUMN a INT, SET (ttl = 'on', ttl_expiration_expression = 'expires_at')",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t RENAME COLUMN a TO b",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t DROP COLUMN a",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t SET (ttl = 'on', ttl_expire_after = '5m')",
			isAllowed: false,
		},
		{
			stmt:      "ALTER TABLE t SET (ttl = 'on', ttl_expiration_expression = 'expires_at')",
			isAllowed: true,
		},
		{
			stmt:      "ALTER TABLE t RESET (ttl, ttl_expiration_expression)",
			isAllowed: false,
		},
		{
			stmt:      "DROP TABLE t",
			isAllowed: false,
		},
	} {
		t.Run(tc.stmt, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt)
			if err != nil {
				t.Fatal(err)
			}
			// Tests for virtual column checks are in
			// TestLogicalReplicationCreationChecks.
			if got := tree.IsAllowedLDRSchemaChange(stmt.AST, nil /* virtualColNames */); got != tc.isAllowed {
				t.Errorf("expected %v, got %v", tc.isAllowed, got)
			}
		})
	}
}
