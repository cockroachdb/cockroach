// Copyright 2018 The Cockroach Authors.
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

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
)

func TestLogicalPropsBuilder(t *testing.T) {
	runDataDrivenTest(t, "testdata/logprops/", memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps)
	runDataDrivenTest(t, "testdata/stats/", memo.ExprFmtHideCost|memo.ExprFmtHideRuleProps)
}

// Test HasCorrelatedSubquery flag manually since it's not important enough
// to add to the ExprView string representation in order to use data-driven
// tests.
func TestHasCorrelatedSubquery(t *testing.T) {
	cat := createLogPropsCatalog(t)

	testCases := []struct {
		sql      string
		expected bool
	}{
		{sql: "SELECT y FROM a WHERE 1=1", expected: false},
		{sql: "SELECT y FROM a WHERE (SELECT COUNT(*) FROM b) > 0", expected: false},
		{sql: "SELECT y FROM a WHERE (SELECT y) > 5", expected: true},
		{sql: "SELECT y FROM a WHERE (SELECT True FROM b WHERE z=y)", expected: true},
		{sql: "SELECT y FROM a WHERE (SELECT z FROM b WHERE z=y) = 5", expected: true},
		{sql: "SELECT y FROM a WHERE 1=1 AND (SELECT z FROM b WHERE z=y)+1 = 10", expected: true},
		{sql: "SELECT (SELECT z FROM b WHERE z=y) FROM a WHERE False", expected: false},
		{sql: "SELECT y FROM a WHERE EXISTS(SELECT z FROM b WHERE z=y)", expected: true},
		{sql: "SELECT y FROM a WHERE EXISTS(SELECT z FROM b)", expected: false},
		{sql: "SELECT y FROM a WHERE 5 = ANY(SELECT z FROM b WHERE z=y)", expected: true},
		{sql: "SELECT y FROM a WHERE 5 = ANY(SELECT z FROM b)", expected: false},
	}

	for _, tc := range testCases {
		tester := testutils.NewOptTester(cat, tc.sql)
		ev, err := tester.OptBuild()
		if err != nil {
			t.Fatalf("%v", err)
		}

		// Dig down through input of Project operator and get Select filter.
		child := ev.Child(0).Child(1)
		if child.Logical().Scalar.HasCorrelatedSubquery != tc.expected {
			t.Errorf("expected HasCorrelatedSubquery to be %v, got %v", tc.expected, !tc.expected)
		}
	}
}

func createLogPropsCatalog(t *testing.T) *testcat.Catalog {
	cat := testcat.New()
	if _, err := cat.ExecuteDDL("CREATE TABLE a (x INT PRIMARY KEY, y INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.ExecuteDDL("CREATE TABLE b (x INT PRIMARY KEY, z INT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return cat
}
