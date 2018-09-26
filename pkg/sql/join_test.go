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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	scan.spans, err = spansFromConstraint(
		desc, &desc.PrimaryIndex, nil /* constraint */, exec.ColumnOrdinalSet{})
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

func TestSemiAntiJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)
	sqlutils.CreateTable(
		t, sqlDB, "t1", "a INT PRIMARY KEY, b INT, c INT", 10,
		sqlutils.ToRowFn(
			sqlutils.RowIdxFn,
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row * row))
			},
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row * row * row))
			},
		),
	)
	sqlutils.CreateTable(
		t, sqlDB, "t2", "a INT PRIMARY KEY, b INT", 10,
		sqlutils.ToRowFn(
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt(row + 5))
			},
			func(row int) tree.Datum {
				return tree.NewDInt(tree.DInt((row - 5) * (row - 5)))
			},
		),
	)
	testCases := []struct {
		typ         sqlbase.JoinType
		left, right string
		using       tree.NameList
		expected    []string
	}{
		{ // 0
			typ:   sqlbase.JoinType_LEFT_SEMI,
			left:  "t1",
			right: "t2",
			using: tree.NameList{"a"},
			expected: []string{
				"[6 36 216]",
				"[7 49 343]",
				"[8 64 512]",
				"[9 81 729]",
				"[10 100 1000]",
			},
		},
		{ // 1
			typ:   sqlbase.JoinType_LEFT_ANTI,
			left:  "t1",
			right: "t2",
			using: tree.NameList{"a"},
			expected: []string{
				"[1 1 1]",
				"[2 4 8]",
				"[3 9 27]",
				"[4 16 64]",
				"[5 25 125]",
			},
		},
		{ // 2
			typ:   sqlbase.JoinType_LEFT_SEMI,
			left:  "t1",
			right: "t2",
			using: tree.NameList{"b"},
			expected: []string{
				"[1 1 1]",
				"[2 4 8]",
				"[3 9 27]",
				"[4 16 64]",
				"[5 25 125]",
			},
		},
		{ // 3
			typ:   sqlbase.JoinType_LEFT_ANTI,
			left:  "t1",
			right: "t2",
			using: tree.NameList{"b"},
			expected: []string{
				"[6 36 216]",
				"[7 49 343]",
				"[8 64 512]",
				"[9 81 729]",
				"[10 100 1000]",
			},
		},
		{ // 4
			typ:   sqlbase.JoinType_LEFT_SEMI,
			left:  "t2",
			right: "t1",
			using: tree.NameList{"a"},
			expected: []string{
				"[6 16]",
				"[7 9]",
				"[8 4]",
				"[9 1]",
				"[10 0]",
			},
		},
		{ // 5
			typ:   sqlbase.JoinType_LEFT_ANTI,
			left:  "t2",
			right: "t1",
			using: tree.NameList{"a"},
			expected: []string{
				"[11 1]",
				"[12 4]",
				"[13 9]",
				"[14 16]",
				"[15 25]",
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				left, err := newTestScanNode(kvDB, tc.left)
				if err != nil {
					return err
				}
				right, err := newTestScanNode(kvDB, tc.right)
				if err != nil {
					return err
				}

				execCfg := s.ExecutorConfig().(ExecutorConfig)
				p, cleanup := newInternalPlanner(
					"TestSemiAntiJoin", txn, security.RootUser, &MemoryMetrics{}, &execCfg,
				)
				defer cleanup()
				leftSrc := planDataSource{
					plan: left,
					info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(left)},
				}
				rightSrc := planDataSource{
					plan: right,
					info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(right)},
				}
				pred, _, err := p.makeJoinPredicate(
					ctx, leftSrc.info, rightSrc.info, tc.typ,
					&tree.UsingJoinCond{Cols: tc.using},
				)
				if err != nil {
					return err
				}
				join := p.makeJoinNode(planDataSource{plan: left}, planDataSource{plan: right}, pred)
				params := runParams{
					ctx:             ctx,
					extendedEvalCtx: p.ExtendedEvalContext(),
					p:               p,
				}
				defer join.Close(ctx)
				if err := startPlan(params, join); err != nil {
					return err
				}
				var res []string
				for {
					ok, err := join.Next(params)
					if err != nil {
						return err
					}
					if !ok {
						break
					}
					res = append(res, fmt.Sprintf("%v", join.Values()))
				}
				if !reflect.DeepEqual(res, tc.expected) {
					t.Errorf("expected:\n%v\n got:\n%v", tc.expected, res)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
