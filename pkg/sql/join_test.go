// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func newTestScanNode(kvDB *client.DB, tableName string) (*scanNode, error) {
	desc := sqlbase.GetImmutableTableDescriptor(kvDB, sqlutils.TestDB, tableName)

	p := planner{}
	scan := p.Scan()
	scan.desc = desc
	err := scan.initDescDefaults(p.curPlan.deps, publicColumnsCfg)
	if err != nil {
		return nil, err
	}
	// Initialize the required ordering.
	columnIDs, dirs := scan.index.FullColumnIDs()
	ordering := make(sqlbase.ColumnOrdering, len(columnIDs))
	for i, colID := range columnIDs {
		idx, ok := scan.colIdxMap[colID]
		if !ok {
			panic(fmt.Sprintf("index refers to unknown column id %d", colID))
		}
		ordering[i].ColIdx = idx
		ordering[i].Direction, err = dirs[i].ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	scan.props.ordering = ordering

	scan.spans, err = spansFromConstraint(
		desc, &desc.PrimaryIndex, nil /* constraint */, exec.ColumnOrdinalSet{}, false /* forDelete */)
	if err != nil {
		return nil, err
	}
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

// computeMergeJoinOrdering determines if merge-join can be used to perform a join.
//
// It takes the orderings of the two data sources that are to be joined on a set
// of equality columns (the join condition is that the value for the column
// colA[i] equals the value for column colB[i]).
//
// If merge-join can be used, the function returns a ColumnOrdering that refers
// to the equality columns by their index in colA/ColB. Specifically column i in
// the returned ordering refers to column colA[i] for A and colB[i] for B. This
// is the ordering that must be used by the merge-join.
//
// The returned ordering can be partial, i.e. only contains a subset of the
// equality columns.
func computeMergeJoinOrdering(
	a, b sqlbase.ColumnOrdering, colA, colB []int,
) sqlbase.ColumnOrdering {
	if len(colA) != len(colB) {
		panic(fmt.Sprintf("invalid column lists %v; %v", colA, colB))
	}
	var result sqlbase.ColumnOrdering
	for i := 0; i < len(a) && i < len(b); i++ {
		found := false
		if a[i].Direction != b[i].Direction {
			break
		}
		for j := range colA {
			if colA[j] == a[i].ColIdx && colB[j] == b[i].ColIdx {
				result = append(result, sqlbase.ColumnOrderInfo{
					ColIdx:    j,
					Direction: a[i].Direction,
				})
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	return result
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
