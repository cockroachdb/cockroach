// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
			stmt:      "CREATE UNIQUE INDEX idx ON t (a)",
			isAllowed: false,
		},
		{
			stmt:      "DROP INDEX idx",
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
	} {
		t.Run(tc.stmt, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt)
			if err != nil {
				t.Fatal(err)
			}
			if got := tree.IsAllowedLDRSchemaChange(stmt.AST); got != tc.isAllowed {
				t.Errorf("expected %v, got %v", tc.isAllowed, got)
			}
		})
	}
}
