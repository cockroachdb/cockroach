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

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestPlanToTreeAndPlanToString verifies that output from planToTree()
// 1) matches expected ExplainTreePlanNode structure, and
// 2) is similar to the output of the pre-existing planToString().
func TestPlanToTreeAndPlanToString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sqlSetup := "CREATE DATABASE t; CREATE TABLE t.orders (oid INT PRIMARY KEY, cid INT, value DECIMAL, date DATE);"
	plansToTest := []*TestData{
		{
			// Test select statement 1.
			SQL: "SELECT CId, Date, Value FROM t.Orders",
			ExpectedPlanString: `0 render  (cid int, date date, value decimal) 
0 .render 0 (@2)[int] (cid int, date date, value decimal) 
0 .render 1 (@4)[date] (cid int, date date, value decimal) 
0 .render 2 (@3)[decimal] (cid int, date date, value decimal) 
1 scan  (cid int, date date, value decimal) 
1 .table orders@primary (cid int, date date, value decimal) 
1 .spans ALL (cid int, date date, value decimal) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "render",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "render",
						Value: "cid",
					},
					{
						Key:   "render",
						Value: "date",
					},
					{
						Key:   "render",
						Value: "value",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "scan",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "table",
								Value: "orders@primary",
							},
							{
								Key:   "spans",
								Value: "ALL",
							},
						},
					},
				},
			},
		},
		{
			// Test select statement 2.
			SQL: "SELECT CId, sum(Value) FROM t.Orders WHERE Date > '2015-01-01' GROUP BY CId ORDER BY 1 - sum(Value);",
			ExpectedPlanString: `0 sort  (cid int, sum decimal) weak-key(cid)
0 .order +"?column?" (cid int, sum decimal) weak-key(cid)
1 render  (cid int, sum decimal) weak-key(cid)
1 .render 0 (@1)[int] (cid int, sum decimal) weak-key(cid)
1 .render 1 (@2)[decimal] (cid int, sum decimal) weak-key(cid)
1 .render 2 ((1)[decimal] - (@2)[decimal])[decimal] (cid int, sum decimal) weak-key(cid)
2 group  (cid int, sum decimal) weak-key(cid)
2 .aggregate 0 cid (cid int, sum decimal) weak-key(cid)
2 .aggregate 1 sum(value) (cid int, sum decimal) weak-key(cid)
2 .group by @1 (cid int, sum decimal) weak-key(cid)
3 render  (cid int, sum decimal) weak-key(cid)
3 .render 0 (@2)[int] (cid int, sum decimal) weak-key(cid)
3 .render 1 (@3)[decimal] (cid int, sum decimal) weak-key(cid)
4 scan  (cid int, sum decimal) weak-key(cid)
4 .table orders@primary (cid int, sum decimal) weak-key(cid)
4 .spans ALL (cid int, sum decimal) weak-key(cid)
4 .filter ((@4)[date] > ('2015-01-01')[date])[bool] (cid int, sum decimal) weak-key(cid)
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "sort",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "order",
						Value: "+\"?column?\"",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "render",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "render",
								Value: "agg0",
							},
							{
								Key:   "render",
								Value: "agg1",
							},
							{
								Key:   "render",
								Value: "1 - agg1",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "group",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
									{
										Key:   "aggregate 0",
										Value: "cid",
									},
									{
										Key:   "aggregate 1",
										Value: "sum(value)",
									},
									{
										Key:   "group by",
										Value: "@1",
									},
								},
								Children: []*roachpb.ExplainTreePlanNode{
									{
										Name: "render",
										Attrs: []*roachpb.ExplainTreePlanNode_Attr{
											{
												Key:   "render",
												Value: "cid",
											},
											{
												Key:   "render",
												Value: "value",
											},
										},
										Children: []*roachpb.ExplainTreePlanNode{
											{
												Name: "scan",
												Attrs: []*roachpb.ExplainTreePlanNode_Attr{
													{
														Key:   "table",
														Value: "orders@primary",
													},
													{
														Key:   "spans",
														Value: "ALL",
													},
													{
														Key:   "filter",
														Value: "date > '2015-01-01'",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			SQL: "SELECT Value FROM (SELECT CId, Date, Value FROM t.Orders)",
			ExpectedPlanString: `0 render  (value decimal) 
0 .render 0 (@3)[decimal] (value decimal) 
1 render  (value decimal) 
1 .render 0 (NULL)[unknown] (value decimal) 
1 .render 1 (NULL)[unknown] (value decimal) 
1 .render 2 (@3)[decimal] (value decimal) 
2 scan  (value decimal) 
2 .table orders@primary (value decimal) 
2 .spans ALL (value decimal) 
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "render",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "render",
						Value: "value",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "render",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "render",
								Value: "NULL",
							},
							{
								Key:   "render",
								Value: "NULL",
							},
							{
								Key:   "render",
								Value: "value",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "scan",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
									{
										Key:   "table",
										Value: "orders@primary",
									},
									{
										Key:   "spans",
										Value: "ALL",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			SQL: "SELECT cid, date, value FROM t.Orders WHERE date IN (SELECT date FROM t.Orders)",
			ExpectedPlanString: `0 root  (cid int, date date, value decimal) date!=NULL
1 render  (cid int, date date, value decimal) date!=NULL
1 .render 0 (@2)[int] (cid int, date date, value decimal) date!=NULL
1 .render 1 (@4)[date] (cid int, date date, value decimal) date!=NULL
1 .render 2 (@3)[decimal] (cid int, date date, value decimal) date!=NULL
2 scan  (cid int, date date, value decimal) date!=NULL
2 .table orders@primary (cid int, date date, value decimal) date!=NULL
2 .spans ALL (cid int, date date, value decimal) date!=NULL
2 .filter ((@4)[date] IN (@S1)[tuple{date}])[bool] (cid int, date date, value decimal) date!=NULL
1 subquery  (cid int, date date, value decimal) date!=NULL
1 .id @S1 (cid int, date date, value decimal) date!=NULL
1 .original sql (SELECT date FROM t.public.orders) (cid int, date date, value decimal) date!=NULL
1 .exec mode all rows normalized (cid int, date date, value decimal) date!=NULL
2 render  (cid int, date date, value decimal) date!=NULL
2 .render 0 (@4)[date] (cid int, date date, value decimal) date!=NULL
3 scan  (cid int, date date, value decimal) date!=NULL
3 .table orders@primary (cid int, date date, value decimal) date!=NULL
3 .spans ALL (cid int, date date, value decimal) date!=NULL
`,
			ExpectedPlanTree: &roachpb.ExplainTreePlanNode{
				Name: "root",
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "render",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "render",
								Value: "cid",
							},
							{
								Key:   "render",
								Value: "date",
							},
							{
								Key:   "render",
								Value: "value",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "scan",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
									{
										Key:   "table",
										Value: "orders@primary",
									},
									{
										Key:   "spans",
										Value: "ALL",
									},
									{
										Key:   "filter",
										Value: "date IN (SELECT date FROM t.public.orders)",
									},
								},
							},
						},
					},
					{
						Name: "subquery",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "id",
								Value: "@S1",
							},
							{
								Key:   "original sql",
								Value: "(SELECT date FROM t.public.orders)",
							},
							{
								Key:   "exec mode",
								Value: "all rows normalized",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "render",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
									{
										Key:   "render",
										Value: "date",
									},
								},
								Children: []*roachpb.ExplainTreePlanNode{
									{
										Name: "scan",
										Attrs: []*roachpb.ExplainTreePlanNode_Attr{
											{
												Key:   "table",
												Value: "orders@primary",
											},
											{
												Key:   "spans",
												Value: "ALL",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	assertExpectedPlansForTests(t, sqlSetup, plansToTest)
}

func assertExpectedPlansForTests(t *testing.T, sqlSetup string, plansToTest []*TestData) {
	// Setup.
	ctx := context.Background()
	s, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.ExecContext(
		ctx, sqlSetup); err != nil {
		t.Fatal(err)
	}
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	internalPlanner, cleanup := NewInternalPlanner(
		"test",
		client.NewTxn(ctx, db, s.NodeID(), client.RootTxn),
		security.RootUser,
		&MemoryMetrics{},
		&execCfg,
	)
	defer cleanup()
	p := internalPlanner.(*planner)

	// Run planToTree() and assert expected value for each plan in plansToTest.
	for _, test := range plansToTest {
		stmt, err := parser.ParseOne(test.SQL)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.makePlan(ctx, Statement{SQL: test.SQL, AST: stmt}); err != nil {
			t.Fatal(err)
		}
		actualPlanTree := planToTree(ctx, p.curPlan)
		assert.Equal(t, test.ExpectedPlanTree, actualPlanTree,
			"planToTree for %s:\nexpected:%s\nactual:%s", test.SQL, test.ExpectedPlanTree, actualPlanTree)
		actualPlanString := planToString(ctx, p.curPlan.plan, p.curPlan.subqueryPlans)
		assert.Equal(t, test.ExpectedPlanString, actualPlanString,
			"planToString for %s:\nexpected:%s\nactual:%s", test.SQL, test.ExpectedPlanString, actualPlanString)
	}
}

type TestData struct {
	SQL                string
	ExpectedPlanString string
	ExpectedPlanTree   *roachpb.ExplainTreePlanNode
}
