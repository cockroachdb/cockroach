// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testEdge struct {
	// joinOp is the type of join this edge represents.
	joinOp opt.Operator

	// left and right are strings of the form "AB" which represent the sets of
	// base relations that make up the left and right inputs, respectively.
	left  string
	right string

	// notNull is a string of the form "AB" which represents the set of base
	// relations on which nulls are rejected by the edge's predicate.
	notNull string

	// ses is a string of the form "AB" which represents the set of base relations
	// referenced by the edge's predicate.
	ses string
}

func TestJoinOrderBuilder_CalcTES(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		rootEdge        testEdge
		leftChildEdges  []testEdge
		rightChildEdges []testEdge
		expectedTES     string
		expectedRules   string
	}{
		{ // 0
			// SELECT * FROM A
			// INNER JOIN (SELECT * FROM B INNER JOIN C ON B.x = C.x)
			// ON A.y = B.y
			rootEdge:       testEdge{joinOp: opt.InnerJoinOp, left: "A", right: "BC", ses: "AB"},
			leftChildEdges: []testEdge{},
			rightChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "B", right: "C", ses: "BC"},
			},
			expectedTES:   "AB",
			expectedRules: "",
		},
		{ // 1
			// SELECT * FROM A
			// INNER JOIN (SELECT * FROM B LEFT JOIN C ON B.x = C.x)
			// ON A.y = B.y
			rootEdge:       testEdge{joinOp: opt.InnerJoinOp, left: "A", right: "BC", ses: "AB"},
			leftChildEdges: []testEdge{},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "B", right: "C", ses: "BC"},
			},
			expectedTES:   "AB",
			expectedRules: "",
		},
		{ // 2
			// SELECT * FROM A
			// INNER JOIN (SELECT * FROM B LEFT JOIN C ON B.x = C.x)
			// ON A.y = C.y
			rootEdge:       testEdge{joinOp: opt.InnerJoinOp, left: "A", right: "BC", ses: "AC"},
			leftChildEdges: []testEdge{},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "B", right: "C", ses: "BC"},
			},
			expectedTES:   "ABC",
			expectedRules: "",
		},
		{ // 3
			// SELECT *
			// FROM (SELECT * FROM A INNER JOIN B ON a.x = b.x)
			// INNER JOIN (SELECT * FROM C LEFT JOIN D ON c.x = d.x)
			// ON A.y = C.y
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "AB", right: "CD", ses: "AC"},
			leftChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "C", right: "D", ses: "CD"},
			},
			expectedTES:   "AC",
			expectedRules: "",
		},
		{ // 4
			// SELECT * FROM A
			// LEFT JOIN B ON A.x = B.x
			// WHERE EXISTS (SELECT * FROM C WHERE B.y = C.y)
			rootEdge: testEdge{joinOp: opt.SemiJoinOp, left: "AB", right: "C", ses: "BC"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "ABC",
			expectedRules:   "",
		},
		{ // 5
			// SELECT * FROM
			// (
			//    SELECT * FROM A
			//    LEFT JOIN B ON A.x = B.x
			//    WHERE EXISTS (SELECT * FROM C WHERE B.y = C.y)
			// )
			// INNER JOIN D ON A.z = D.z
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "ABC", right: "D", ses: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "A", right: "B", ses: "AB"},
				{joinOp: opt.SemiJoinOp, left: "AB", right: "C", ses: "BC"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "AD",
			expectedRules:   "C -> B",
		},
		{ // 6
			// SELECT * FROM A
			// WHERE EXISTS
			// (
			//   SELECT * FROM B
			//   LEFT JOIN C ON B.x = C.x
			//   WHERE A.y = B.y
			// )
			rootEdge: testEdge{joinOp: opt.SemiJoinOp, left: "A", right: "BC", ses: "AB"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "B", right: "C", ses: "BC"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "AB",
			expectedRules:   "",
		},
		{ // 7
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   WHERE NOT EXISTS (SELECT * FROM B WHERE A.x = B.x)
			// )
			// WHERE EXISTS (SELECT * FROM C WHERE A.y = C.y)
			rootEdge: testEdge{joinOp: opt.SemiJoinOp, left: "AB", right: "C", ses: "AC"},
			leftChildEdges: []testEdge{
				{joinOp: opt.AntiJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "AC",
			expectedRules:   "",
		},
		{ // 8
			// SELECT * FROM A
			// INNER JOIN B ON A.x = B.x
			// FULL JOIN C ON A.y = C.y
			rootEdge: testEdge{joinOp: opt.FullJoinOp, left: "AB", right: "C", ses: "AC"},
			leftChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "ABC",
			expectedRules:   "",
		},
		{ // 9
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   INNER JOIN B ON True
			// )
			// INNER JOIN C ON True
			// INNER JOIN D ON True
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "AB", right: "CD", ses: ""},
			leftChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "A", right: "B", ses: ""},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "C", right: "D", ses: ""},
			},
			expectedTES:   "ABCD",
			expectedRules: "",
		},
		{ // 10
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   WHERE NOT EXISTS (SELECT * FROM B WHERE A.x = B.x)
			// )
			// WHERE EXISTS (SELECT * FROM C WHERE A.y = C.y)
			// LEFT JOIN D ON A.z = D.z
			// INNER JOIN E ON B.z = E.z
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "ABCD", right: "E", ses: "BE"},
			leftChildEdges: []testEdge{
				{joinOp: opt.AntiJoinOp, left: "A", right: "B", ses: "AB"},
				{joinOp: opt.SemiJoinOp, left: "AB", right: "C", ses: "AC"},
				{joinOp: opt.LeftJoinOp, left: "ABC", right: "D", ses: "AD"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "ABE",
			expectedRules:   "",
		},
		{ // 11
			// SELECT *
			// FROM (SELECT * FROM A LEFT JOIN B ON A.x = B.x)
			// INNER JOIN (SELECT * FROM C LEFT JOIN D ON C.y = D.y)
			// ON A.z = D.z
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "AB", right: "CD", ses: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "C", right: "D", ses: "CD"},
			},
			expectedTES:   "ACD",
			expectedRules: "",
		},
		{ // 12
			// SELECT * FROM A
			// INNER JOIN
			// (
			//   SELECT * FROM
			//   (
			//     SELECT * FROM B
			//     INNER JOIN C ON B.x = C.x
			//   )
			//   WHERE EXISTS (SELECT * FROM D WHERE B.y = D.y)
			// )
			// ON A.z = B.z
			rootEdge:       testEdge{joinOp: opt.InnerJoinOp, left: "A", right: "BCD", ses: "AB"},
			leftChildEdges: []testEdge{},
			rightChildEdges: []testEdge{
				{joinOp: opt.SemiJoinOp, left: "BC", right: "D", ses: "BD"},
				{joinOp: opt.InnerJoinOp, left: "B", right: "C", ses: "BC"},
			},
			expectedTES:   "AB",
			expectedRules: "",
		},
		{ // 13
			// SELECT * FROM A
			// INNER JOIN
			// (
			//   SELECT * FROM B
			//   INNER JOIN (SELECT * FROM C LEFT JOIN D ON C.x = D.x)
			//   ON B.y = D.y
			// )
			// ON A.z = B.z
			rootEdge:       testEdge{joinOp: opt.InnerJoinOp, left: "A", right: "BCD", ses: "AB"},
			leftChildEdges: []testEdge{},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "C", right: "D", ses: "CD"},
				{joinOp: opt.InnerJoinOp, left: "B", right: "CD", ses: "BD"},
			},
			expectedTES:   "AB",
			expectedRules: "D -> C",
		},
		{ // 14
			// SELECT *
			// FROM (SELECT * FROM A INNER JOIN B ON True)
			// FULL JOIN (SELECT * FROM C INNER JOIN D ON True)
			// ON A.x = C.x
			rootEdge: testEdge{joinOp: opt.FullJoinOp, left: "AB", right: "CD", ses: "AC"},
			leftChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "A", right: "B", ses: ""},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "C", right: "D", ses: ""},
			},
			expectedTES:   "ABCD",
			expectedRules: "",
		},
		{ // 15
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   FULL JOIN B
			//   ON (A.x = B.x OR A.x IS NULL OR B.x IS NULL)
			// )
			// FULL JOIN
			// (
			//   SELECT * FROM C
			//   FULL JOIN D
			//   ON (C.x = D.x OR C.x IS NULL OR D.x IS NULL)
			// )
			// ON (A.y = D.y OR A.y IS NULL OR D.y IS NULL)
			rootEdge: testEdge{joinOp: opt.FullJoinOp, left: "AB", right: "CD", ses: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.FullJoinOp, left: "A", right: "B", ses: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.FullJoinOp, left: "C", right: "D", ses: "CD"},
			},
			expectedTES:   "ABCD",
			expectedRules: "",
		},
		{ // 16
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   FULL JOIN B ON A.x = B.x
			// )
			// FULL JOIN
			// (
			//   SELECT * FROM C
			//   FULL JOIN D ON C.x = D.x
			// )
			// ON A.y = C.y
			rootEdge: testEdge{joinOp: opt.FullJoinOp, left: "AB", right: "CD", ses: "AD", notNull: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.FullJoinOp, left: "A", right: "B", ses: "AB", notNull: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.FullJoinOp, left: "C", right: "D", ses: "CD", notNull: "CD"},
			},
			expectedTES:   "AD",
			expectedRules: "",
		},
		{ // 17
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   LEFT JOIN B ON A.x = B.x
			// )
			// LEFT JOIN
			// (
			//   SELECT * FROM C
			//   LEFT JOIN D ON C.x = D.x
			// )
			// ON A.y = C.y
			rootEdge: testEdge{joinOp: opt.LeftJoinOp, left: "AB", right: "CD", ses: "AD", notNull: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "A", right: "B", ses: "AB", notNull: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "C", right: "D", ses: "CD", notNull: "CD"},
			},
			expectedTES:   "ACD",
			expectedRules: "",
		},
		{ // 18
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   LEFT JOIN B ON A.x = B.x
			// )
			// LEFT JOIN
			// (
			//   SELECT * FROM C
			//   LEFT JOIN D ON C.x = D.x
			// )
			// ON B.y = C.y
			rootEdge: testEdge{joinOp: opt.LeftJoinOp, left: "AB", right: "CD", ses: "BD", notNull: "BD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "A", right: "B", ses: "AB", notNull: "AB"},
			},
			rightChildEdges: []testEdge{
				{joinOp: opt.LeftJoinOp, left: "C", right: "D", ses: "CD", notNull: "CD"},
			},
			expectedTES:   "BCD",
			expectedRules: "",
		},
		{ // 19
			// SELECT * FROM
			// (
			//   SELECT * FROM A
			//   FULL JOIN B ON A.x = B.x
			// )
			// FULL JOIN C ON B.y = C.y OR B IS NULL
			rootEdge: testEdge{joinOp: opt.FullJoinOp, left: "AB", right: "C", ses: "BC", notNull: "C"},
			leftChildEdges: []testEdge{
				{joinOp: opt.FullJoinOp, left: "A", right: "B", ses: "AB", notNull: "AB"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "ABC",
			expectedRules:   "",
		},
		{ // 20
			// SELECT * FROM (
			//   SELECT * FROM (
			//     SELECT * FROM A
			//     INNER JOIN B ON A.u = B.u
			//   ) INNER JOIN C ON B.v = C.v
			// ) INNER JOIN D ON A.w = D.w
			rootEdge: testEdge{joinOp: opt.InnerJoinOp, left: "ABC", right: "D", ses: "AD", notNull: "AD"},
			leftChildEdges: []testEdge{
				{joinOp: opt.InnerJoinOp, left: "AB", right: "C", ses: "BC", notNull: "BC"},
				{joinOp: opt.InnerJoinOp, left: "A", right: "B", ses: "AB", notNull: "AB"},
			},
			rightChildEdges: []testEdge{},
			expectedTES:     "AD",
			expectedRules:   "",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Test case %d", i), func(t *testing.T) {
			edges := makeEdgesSlice(tc.leftChildEdges, tc.rightChildEdges)
			var leftEdges, rightEdges edgeSet
			leftLen := len(tc.leftChildEdges)
			rightLen := len(tc.rightChildEdges)
			if leftLen > 0 {
				leftEdges.AddRange(0, leftLen-1)
			}
			if rightLen > 0 {
				rightEdges.AddRange(leftLen, leftLen+rightLen-1)
			}
			rootEdge := makeEdge(tc.rootEdge)
			rootEdge.op.leftEdges = leftEdges
			rootEdge.op.rightEdges = rightEdges
			rootEdge.calcTES(edges)

			if printVertexSet(rootEdge.tes) != tc.expectedTES {
				t.Fatalf(
					"\nexpected TES: %s\nactual TES:   %s", /* format */
					tc.expectedTES,
					printVertexSet(rootEdge.tes),
				)
			}
			if printRules(rootEdge) != tc.expectedRules {
				t.Fatalf(
					"\nexpected Rules: %s\nactual Rules: %s", /* format */
					tc.expectedRules,
					printRules(rootEdge),
				)
			}
		})
	}
}

func makeEdge(e testEdge) *edge {
	operator := &operator{
		joinType:      e.joinOp,
		leftVertexes:  parseVertexSet(e.left),
		rightVertexes: parseVertexSet(e.right),
	}
	return &edge{
		op:               operator,
		nullRejectedRels: parseVertexSet(e.notNull),
		ses:              parseVertexSet(e.ses),
	}
}

func makeEdgesSlice(leftEdges, rightEdges []testEdge) []edge {
	edges := make([]edge, 0, len(leftEdges)+len(rightEdges))
	for i := range leftEdges {
		edges = append(edges, *makeEdge(leftEdges[i]))
	}
	for i := range rightEdges {
		edges = append(edges, *makeEdge(rightEdges[i]))
	}
	return edges
}

func parseVertexSet(sesStr string) vertexSet {
	var ses vertexSet
	for i := range sesStr {
		ses = ses.add(vertexIndex(sesStr[i] - 'A'))
	}
	return ses
}

func printVertexSet(set vertexSet) string {
	buf := bytes.Buffer{}
	for idx, ok := set.next(0); ok; idx, ok = set.next(idx + 1) {
		buf.WriteString(string(rune('A' + idx)))
	}
	return buf.String()
}

func printRules(e *edge) string {
	buf := bytes.Buffer{}
	for i := range e.rules {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(
			fmt.Sprintf(
				"%s -> %s", /* format */
				printVertexSet(e.rules[i].from),
				printVertexSet(e.rules[i].to),
			),
		)
	}
	return buf.String()
}
