// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pretty_test

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/pretty"
)

// Example_align demonstrates alignment.
func Example_align() {
	testData := []pretty.Doc{
		pretty.JoinGroupAligned("SELECT", ",",
			pretty.Text("aaa"),
			pretty.Text("bbb"),
			pretty.Text("ccc")),
		pretty.Table(pretty.TableRightAlignFirstColumn, pretty.Text,
			pretty.TableRow{Label: "SELECT",
				Doc: pretty.Join(",",
					pretty.Text("aaa"),
					pretty.Text("bbb"),
					pretty.Text("ccc")),
			},
			pretty.TableRow{Label: "FROM",
				Doc: pretty.Join(",",
					pretty.Text("t"),
					pretty.Text("u"),
					pretty.Text("v")),
			}),
		pretty.Table(pretty.TableLeftAlignFirstColumn, pretty.Text,
			pretty.TableRow{Label: "SELECT",
				Doc: pretty.Join(",",
					pretty.Text("aaa"),
					pretty.Text("bbb"),
					pretty.Text("ccc")),
			},
			pretty.TableRow{Label: "FROM",
				Doc: pretty.Join(",",
					pretty.Text("t"),
					pretty.Text("u"),
					pretty.Text("v")),
			}),
		pretty.Table(pretty.TableRightAlignFirstColumn, pretty.Text,
			pretty.TableRow{Label: "woo", Doc: nil}, // check nil rows are omitted
			pretty.TableRow{Label: "", Doc: pretty.Nil},
			pretty.TableRow{Label: "KEY", Doc: pretty.Text("VALUE")},
			pretty.TableRow{Label: "", Doc: pretty.Text("OTHERVALUE")},
			pretty.TableRow{Label: "AAA", Doc: pretty.Nil}, // check no extra space is added
		),
	}
	for _, n := range []int{1, 15, 30, 80} {
		fmt.Printf("%d:\n", n)
		for _, doc := range testData {
			p := pretty.Pretty(doc, n, true /*useTabs*/, 4 /*tabWidth*/, nil /*keywordTransform*/)
			fmt.Printf("%s\n\n", p)
		}
	}

	// Output:
	// 1:
	// SELECT
	// 	aaa,
	// 	bbb,
	// 	ccc
	//
	// SELECT
	// 	aaa,
	// 	bbb,
	// 	ccc
	// FROM
	// 	t,
	// 	u,
	// 	v
	//
	// SELECT
	// 	aaa,
	// 	bbb,
	// 	ccc
	// FROM
	// 	t,
	// 	u,
	// 	v
	//
	// KEY
	// 	VALUE
	// OTHERVALUE
	// AAA
	//
	// 15:
	// SELECT aaa,
	//        bbb,
	//        ccc
	//
	// SELECT aaa,
	//        bbb,
	//        ccc
	//   FROM t, u, v
	//
	// SELECT aaa,
	//        bbb,
	//        ccc
	// FROM   t, u, v
	//
	// KEY VALUE
	//     OTHERVALUE
	// AAA
	//
	// 30:
	// SELECT aaa, bbb, ccc
	//
	// SELECT aaa, bbb, ccc
	//   FROM t, u, v
	//
	// SELECT aaa, bbb, ccc
	// FROM   t, u, v
	//
	// KEY VALUE OTHERVALUE AAA
	//
	// 80:
	// SELECT aaa, bbb, ccc
	//
	// SELECT aaa, bbb, ccc FROM t, u, v
	//
	// SELECT aaa, bbb, ccc FROM t, u, v
	//
	// KEY VALUE OTHERVALUE AAA
}

// ExampleTree demonstrates the Tree example from the paper.
func Example_tree() {
	type Tree struct {
		s  string
		n  []Tree
		op string
	}
	tree := Tree{
		s: "aaa",
		n: []Tree{
			{
				s: "bbbbb",
				n: []Tree{
					{s: "ccc"},
					{s: "dd"},
					{s: "ee", op: "*", n: []Tree{
						{s: "some"},
						{s: "another", n: []Tree{{s: "2a"}, {s: "2b"}}},
						{s: "final"},
					}},
				},
			},
			{s: "eee"},
			{
				s: "ffff",
				n: []Tree{
					{s: "gg"},
					{s: "hhh"},
					{s: "ii"},
				},
			},
		},
	}
	var (
		showTree    func(Tree) pretty.Doc
		showTrees   func([]Tree) pretty.Doc
		showBracket func([]Tree) pretty.Doc
	)
	showTrees = func(ts []Tree) pretty.Doc {
		if len(ts) == 1 {
			return showTree(ts[0])
		}
		return pretty.Fold(pretty.Concat,
			showTree(ts[0]),
			pretty.Text(","),
			pretty.Line,
			showTrees(ts[1:]),
		)
	}
	showBracket = func(ts []Tree) pretty.Doc {
		if len(ts) == 0 {
			return pretty.Nil
		}
		return pretty.Fold(pretty.Concat,
			pretty.Text("["),
			pretty.NestT(showTrees(ts)),
			pretty.Text("]"),
		)
	}
	showTree = func(t Tree) pretty.Doc {
		var doc pretty.Doc
		if t.op != "" {
			var operands []pretty.Doc
			for _, o := range t.n {
				operands = append(operands, showTree(o))
			}
			doc = pretty.Fold(pretty.Concat,
				pretty.Text("("),
				pretty.JoinNestedRight(
					pretty.Text(t.op), operands...),
				pretty.Text(")"),
			)
		} else {
			doc = showBracket(t.n)
		}
		return pretty.Group(pretty.Concat(
			pretty.Text(t.s),
			pretty.NestS(int16(len(t.s)), doc),
		))
	}
	for _, n := range []int{1, 30, 80} {
		p := pretty.Pretty(showTree(tree), n, false /*useTabs*/, 4 /*tabWidth*/, nil /*keywordTransform*/)
		fmt.Printf("%d:\n%s\n\n", n, p)
	}
	// Output:
	// 1:
	// aaa[bbbbb[ccc,
	//             dd,
	//             ee(some
	//               * another[2a,
	//                         2b]
	//               * final)],
	//     eee,
	//     ffff[gg,
	//             hhh,
	//             ii]]
	//
	// 30:
	// aaa[bbbbb[ccc,
	//             dd,
	//             ee(some
	//               * another[2a,
	//                         2b]
	//               * final)],
	//     eee,
	//     ffff[gg, hhh, ii]]
	//
	// 80:
	// aaa[bbbbb[ccc, dd, ee(some * another[2a, 2b] * final)], eee, ffff[gg, hhh, ii]]
}
