// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestScan(t *testing.T) {
	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2), INDEX(c3 DESC, c4))",
	); err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext(nil /* st */)
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)

	if c1 := tab.ColumnID(0); c1 != 1 {
		t.Fatalf("unexpected ID for column c1: %d\n", c1)
	}

	// Make a constraint for the index.
	var columns constraint.Columns
	columns.Init([]opt.OrderingColumn{-3, +4, +1, +2})
	var c constraint.Constraint
	keyCtx := constraint.MakeKeyContext(&columns, evalCtx)
	var span constraint.Span
	span.Init(
		constraint.MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(10)),
		constraint.IncludeBoundary,
		constraint.MakeCompositeKey(tree.NewDInt(1), tree.NewDInt(20)),
		constraint.IncludeBoundary,
	)
	c.InitSingleSpan(&keyCtx, &span)

	// We have groups of test cases for various ScanPrivates.
	type testCase struct {
		req  string // required ordering
		exp  string // "no", "fwd", or "rev"
		prov string // provided ordering
	}

	type testGroup struct {
		p     memo.ScanPrivate
		c     *constraint.Constraint
		cases []testCase
	}

	tests := []testGroup{
		{ // group 1: primary index scan.
			p: memo.ScanPrivate{
				Table: tab,
				Index: cat.PrimaryIndex,
				Cols:  opt.MakeColSet(1, 2, 3, 4),
			},
			cases: []testCase{
				{req: "", exp: "fwd", prov: ""},               // case 1
				{req: "+1", exp: "fwd", prov: "+1"},           // case 2
				{req: "-1", exp: "rev", prov: "-1"},           // case 3
				{req: "+2", exp: "no"},                        // case 4
				{req: "+1,+2", exp: "fwd", prov: "+1,+2"},     // case 5
				{req: "-1,-2", exp: "rev", prov: "-1,-2"},     // case 6
				{req: "+1,-2", exp: "no", prov: "+1,-2"},      // case 7
				{req: "+(1|2)", exp: "fwd", prov: "+1"},       // case 8
				{req: "+2 opt(1)", exp: "fwd", prov: "+1,+2"}, // case 9
				{req: "-2 opt(1)", exp: "rev", prov: "-1,-2"}, // case 10
			},
		},
		{ // group 2: secondary index scan.
			p: memo.ScanPrivate{
				Table: tab,
				Index: 1,
				Cols:  opt.MakeColSet(1, 2, 3, 4),
			},
			cases: []testCase{
				{req: "", exp: "fwd", prov: ""},                          // case 1
				{req: "-3", exp: "fwd", prov: "-3"},                      // case 2
				{req: "+3", exp: "rev", prov: "+3"},                      // case 3
				{req: "-3,+4", exp: "fwd", prov: "-3,+4"},                // case 4
				{req: "+3,-4", exp: "rev", prov: "+3,-4"},                // case 5
				{req: "+3,+4", exp: "no"},                                // case 6
				{req: "-3,+4,+1", exp: "fwd", prov: "-3,+4,+1"},          // case 7
				{req: "-3,+4,+1,+2", exp: "fwd", prov: "-3,+4,+1,+2"},    // case 8
				{req: "-3,+2 opt(1,4)", exp: "fwd", prov: "-3,+4,+1,+2"}, // case 9
				{req: "+3,-2 opt(1,4)", exp: "rev", prov: "+3,-4,-1,-2"}, // case 10
			},
		},
		{ // group 3: scan with limit (forces forward scan).
			p: memo.ScanPrivate{
				Table:     tab,
				Index:     cat.PrimaryIndex,
				Cols:      opt.MakeColSet(1, 2, 3, 4),
				HardLimit: +10,
			},
			cases: []testCase{
				{req: "", exp: "fwd", prov: ""},               // case 1
				{req: "+1", exp: "fwd", prov: "+1"},           // case 2
				{req: "-1", exp: "no"},                        // case 3
				{req: "+2", exp: "no"},                        // case 4
				{req: "+1,+2", exp: "fwd", prov: "+1,+2"},     // case 5
				{req: "-1,-2", exp: "no"},                     // case 6
				{req: "+1,-2", exp: "no"},                     // case 7
				{req: "+(1|2)", exp: "fwd", prov: "+1"},       // case 8
				{req: "+2 opt(1)", exp: "fwd", prov: "+1,+2"}, // case 9
				{req: "-2 opt(1)", exp: "no"},                 // case 10
			},
		},
		{ // group 4: scan with reverse limit.
			p: memo.ScanPrivate{
				Table:     tab,
				Index:     cat.PrimaryIndex,
				Cols:      opt.MakeColSet(1, 2, 3, 4),
				HardLimit: -10,
			},
			cases: []testCase{
				{req: "", exp: "rev", prov: ""},               // case 1
				{req: "+1", exp: "no"},                        // case 2
				{req: "-1", exp: "rev", prov: "-1"},           // case 3
				{req: "+2", exp: "no"},                        // case 4
				{req: "+1,+2", exp: "no"},                     // case 5
				{req: "-1,-2", exp: "rev", prov: "-1,-2"},     // case 6
				{req: "+1,-2", exp: "no"},                     // case 7
				{req: "+(1|2)", exp: "no"},                    // case 8
				{req: "+2 opt(1)", exp: "no"},                 // case 9
				{req: "-2 opt(1)", exp: "rev", prov: "-1,-2"}, // case 10
			},
		},
		{ // group 5: scan with constraint.
			p: memo.ScanPrivate{
				Table: tab,
				Index: 1,
				Cols:  opt.MakeColSet(1, 2, 3, 4),
			},
			c: &c,
			cases: []testCase{
				{req: "-3", exp: "fwd", prov: ""},                   // case 1
				{req: "-3,+4", exp: "fwd", prov: "+4"},              // case 2
				{req: "-1 opt(3,4)", exp: "rev", prov: "-4,-1"},     // case 3
				{req: "+1 opt(3)", exp: "no"},                       // case 4
				{req: "-(1|2) opt(3,4)", exp: "rev", prov: "-4,-1"}, // case 5
			},
		},
	}

	for gIdx, g := range tests {
		t.Run(fmt.Sprintf("group%d", gIdx+1), func(t *testing.T) {
			for tcIdx, tc := range g.cases {
				t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
					req := props.ParseOrderingChoice(tc.req)
					g.p.SetConstraint(evalCtx, g.c)
					ok, rev := ScanPrivateCanProvide(md, &g.p, &req)
					res := "no"
					if ok {
						if rev {
							res = "rev"
						} else {
							res = "fwd"
						}
					}
					if res != tc.exp {
						t.Errorf("expected %s, got %s", tc.exp, res)
					}
					if ok {
						scan := f.ConstructScan(&g.p)
						prov := scanBuildProvided(scan, &req).String()
						if prov != tc.prov {
							t.Errorf("expected provided '%s', got '%s'", tc.prov, prov)
						}
					}
				})
			}
		})
	}
}
