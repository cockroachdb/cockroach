// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var pmap = map[bool]string{
	true:  "pausable",
	false: "unpausable",
}

func TestIsAllowedToPause(t *testing.T) {
	testData := []struct {
		qry      string
		pausable bool
	}{
		{"SELECT 1;", true},
		{"WITH cte AS (SELECT * FROM t) SELECT * FROM cte;", true},
		{"INSERT INTO t VALUES (1);", false},
		{"WITH cte AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM cte;", false},
		{"WITH cte AS (SELECT * FROM t) UPDATE t SET v = cte.v FROM cte WHERE t.a = cte.a;", false},
		{"SELECT f();", false},
		{"WITH cte AS (SELECT f()) SELECT * FROM cte;", false},
		{"SELECT * FROM (VALUES (f()));", false},
	}

	for i, td := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			stmts, err := parser.Parse(td.qry)
			if err != nil {
				t.Fatal(err)
			}
			if len(stmts) != 1 {
				t.Fatalf("Expected 1 parsed statement, got %d.", len(stmts))
			}
			if pausable := tree.IsAllowedToPause(stmts[0].AST); pausable != td.pausable {
				t.Errorf("Expected statement \"%s\" to be %s, got %s", td.qry, pmap[td.pausable], pmap[pausable])
			}
		})
	}

}
