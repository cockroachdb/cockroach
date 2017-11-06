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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func newTestScanNode(kvDB *client.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tableName)

	scan := &scanNode{desc: desc, p: &planner{}}
	err := scan.initDescDefaults(publicColumns, nil)
	if err != nil {
		return nil, err
	}
	scan.initOrdering(0)
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

func setTestEqCols(n *joinNode, colNames []string) error {
	left := n.left.plan.(*scanNode).index
	right := n.right.plan.(*scanNode).index

	n.pred = &joinPredicate{}
	n.mergeJoinOrdering = nil

	for _, colName := range colNames {
		if colName == "" {
			continue
		}

		colFound := false
		for i, leftCol := range left.ColumnNames {
			if colName == leftCol {
				n.pred.leftEqualityIndices = append(
					n.pred.leftEqualityIndices,
					i,
				)
				colFound = true
				break
			}
		}
		if !colFound {
			return errors.Errorf("column %s not found in index %s", colName, left.Name)
		}

		colFound = false
		for i, rightCol := range right.ColumnNames {
			if colName == rightCol {
				n.pred.rightEqualityIndices = append(
					n.pred.rightEqualityIndices,
					i,
				)
				colFound = true
				break
			}
		}
		if !colFound {
			return errors.Errorf("column %s not found in index %s", colName, left.Name)
		}
	}

	n.mergeJoinOrdering = computeMergeJoinOrdering(
		planPhysicalProps(n.left.plan),
		planPhysicalProps(n.right.plan),
		n.pred.leftEqualityIndices,
		n.pred.rightEqualityIndices,
	)

	return nil
}

func genPermutations(slice []string) [][]string {
	if len(slice) == 0 {
		return [][]string{[]string{}}
	}

	var out [][]string
	for i, str := range slice {
		recurse := append([]string{}, slice[:i]...)
		recurse = append(recurse, slice[i+1:]...)
		for _, subperms := range genPermutations(recurse) {
			out = append(out, append([]string{str}, subperms...))
		}
	}

	return out
}

func TestInterleaveNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleaveHierarchy(t, sqlDB)

	for _, tc := range []struct {
		table1     string
		table2     string
		ancestor   string
		descendant string
	}{
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

				ancestor, descendant := join.interleaveNodes()

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

func TestComputeJoinHint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleaveHierarchy(t, sqlDB)

	for _, tc := range []struct {
		table1     string
		table2     string
		eqCols     string
		interleave bool
	}{
		// Simple parent-child case.
		{"parent1", "child1", "", false},
		{"parent1", "child1", "pid1", true},
		{"parent1", "child1", "v", false},
		{"parent1", "child1", "pid1,v", true},
		// Parent-grandchild case.
		{"parent1", "grandchild1", "", false},
		{"parent1", "grandchild1", "pid1", true},
		{"parent1", "grandchild1", "v", false},
		{"parent1", "grandchild1", "pid1,v", true},
		// Multiple-column interleave prefix.
		{"child1", "grandchild1", "", false},
		{"child1", "grandchild1", "cid1,cid2", true},
		{"child1", "grandchild1", "v", false},
		{"child1", "grandchild1", "cid1,cid2,v", true},
		// TODO(richardwu): update these once prefix/subset of
		// interleave prefixes are permitted.
		{"child1", "grandchild1", "cid1", false},
		{"child1", "grandchild1", "cid2", false},
		{"child1", "grandchild1", "cid1,v", false},
		{"child1", "grandchild1", "cid2,v", false},
		// Common ancestor example.
		{"child1", "child2", "", false},
		// TODO(richardwu): update this when common ancestor interleave
		// joins are possible.
		{"child1", "child2", "pid1", false},
	} {
		// Run the subtests with the tables in both positions (left and
		// right).
		for i := 0; i < 2; i++ {
			// Run every permutation of the equality columns (just
			// to ensure mergeJoinOrdering is invariant since we
			// rely on it to correspond with the primary index of
			// the ancestor).
			eqCols := strings.Split(tc.eqCols, ",")
			log.Warningf(context.TODO(), "%#v\n", eqCols)
			for _, colNames := range genPermutations(eqCols) {
				log.Warningf(context.TODO(), "%#v\n", colNames)
				testName := fmt.Sprintf("%s-%s-%s", tc.table1, tc.table2, strings.Join(colNames, ","))
				t.Run(testName, func(t *testing.T) {
					join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
					if err != nil {
						t.Fatal(err)
					}
					if err := setTestEqCols(join, colNames); err != nil {
						t.Fatal(err)
					}

					joinHint := join.computeJoinHint()

					if tc.interleave && joinHint != joinHintInterleave {
						t.Errorf("expected join hint to be joinHintInterleave")
					}
				})
			}
			// Rerun the same subtests but flip the tables
			tc.table1, tc.table2 = tc.table2, tc.table1
		}
	}
}
