// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that starting the subqueries returns an error if the evaluation of a
// subquery returns an error.
func TestStartSubqueriesReturnsError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sql := "SELECT 1 WHERE (SELECT CRDB_INTERNAL.FORCE_ERROR('xxx', 'forced') > 0)"
	p := makeTestPlanner()
	stmts, err := p.parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected to parse 1 statement, got: %d", len(stmts))
	}
	stmt := stmts[0]
	if err := p.makePlan(context.TODO(), Statement{AST: stmt}); err != nil {
		t.Fatal(err)
	}
	params := runParams{ctx: context.TODO(), p: p, extendedEvalCtx: &p.extendedEvalCtx}
	if err := p.curPlan.start(params); !testutils.IsError(err, `forced`) {
		t.Fatalf("expected error from force_error(), got: %v", err)
	}
	p.curPlan.close(context.TODO())
}
