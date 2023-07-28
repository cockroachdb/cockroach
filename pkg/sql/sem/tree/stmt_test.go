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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var pmap = map[bool]string{
	true:  "pausable",
	false: "unpausable",
}

func TestIsAllowedToPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testData := []struct {
		qry         string
		funcEnabled bool
		pausable    bool
	}{
		{"SELECT 1;", false, true},
		{"WITH cte AS (SELECT * FROM t) SELECT * FROM cte;", false, true},
		{"INSERT INTO t VALUES (1);", false, false},
		{"WITH cte AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM cte;", false, false},
		{"WITH cte AS (SELECT * FROM t) UPDATE t SET v = cte.v FROM cte WHERE t.a = cte.a;", false, false},
		{"SELECT f();", false, false},
		{"WITH cte AS (SELECT f()) SELECT * FROM cte;", false, false},
		{"SELECT * FROM (VALUES (f()));", false, false},
		{"SELECT f();", true, true},
		{"WITH cte AS (SELECT f()) SELECT * FROM cte;", true, true},
		{"SELECT * FROM (VALUES (f()));", true, true},
		{"INSERT INTO t VALUES(1);", true, false},
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
			if pausable := tree.IsAllowedToPause(stmts[0].AST, td.funcEnabled); pausable != td.pausable {
				t.Errorf("Expected statement \"%s\" to be %s, got %s", td.qry, pmap[td.pausable], pmap[pausable])
			}
		})
	}

}
