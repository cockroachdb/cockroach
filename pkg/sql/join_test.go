// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func newTestScanNode(kvDB *client.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tableName)

	p := planner{}
	scan := p.Scan()
	scan.desc = desc
	err := scan.initDescDefaults(p.curPlan.deps, publicColumnsCfg)
	if err != nil {
		return nil, err
	}
	scan.initOrdering(0 /* exactPrefix */, p.EvalContext())
	return scan, nil
}

func newTestJoinNode(kvDB *client.DB, leftName, rightName string) (*joinNode, error) {
	left, err := newTestScanNode(kvDB, leftName)
	if err != nil {
		return nil, err
	}
	right, err := newTestScanNode(kvDB, rightName)
	if err != nil {
		return nil, err
	}
	return &joinNode{
		left:  planDataSource{plan: left},
		right: planDataSource{plan: right},
	}, nil
}

func TestInterleavedNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	for _, tc := range []struct {
		table1     string
		table2     string
		ancestor   string
		descendant string
	}{
		// Refer to comment above CreateTestInterleavedHierarchy for
		// table schemas.

		{"parent1", "child1", "parent1", "child1"},
		{"parent1", "child2", "parent1", "child2"},
		{"parent1", "grandchild1", "parent1", "grandchild1"},
		{"child1", "child2", "", ""},
		{"child1", "grandchild1", "child1", "grandchild1"},
		{"child2", "grandchild1", "", ""},
		{"parent1", "parent2", "", ""},
		{"parent2", "child1", "", ""},
		{"parent2", "grandchild1", "", ""},
		{"parent2", "child2", "", ""},
	} {
		// Run the subtests with the tables in both positions (left
		// and right).
		for i := 0; i < 2; i++ {
			testName := fmt.Sprintf("%s-%s", tc.table1, tc.table2)
			t.Run(testName, func(t *testing.T) {
				join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
				if err != nil {
					t.Fatal(err)
				}

				ancestor, descendant := join.interleavedNodes()

				if tc.ancestor == tc.descendant && tc.ancestor == "" {
					if ancestor != nil || descendant != nil {
						t.Errorf("expected ancestor and descendant to both be nil")
					}
					return
				}

				if ancestor == nil || descendant == nil {
					t.Fatalf("expected ancestor and descendant to not be nil")
				}

				if tc.ancestor != ancestor.desc.Name || tc.descendant != descendant.desc.Name {
					t.Errorf(
						"unexpected ancestor and descendant nodes.\nexpected: %s (ancestor), %s (descendant)\nactual: %s (ancestor), %s (descendant)",
						tc.ancestor, tc.descendant,
						ancestor.desc.Name, descendant.desc.Name,
					)
				}
			})
			// Rerun the same subtests but flip the tables
			tc.table1, tc.table2 = tc.table2, tc.table1
		}
	}
}
