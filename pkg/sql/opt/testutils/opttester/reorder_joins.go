// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opttester

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/errors"
)

// ReorderJoins optimizes the given query and outputs intermediate steps taken
// during join enumeration. For each call to joinOrderBuilder.Reorder, the
// output is as follows:
//   1. The original join tree that is used to form the join graph.
//   2. The vertexes of the join graph (as well as compact aliases that will be
//      used to output joins added to the memo).
//   3. The edges of the join graph.
//   4. The joins which joinOrderBuilder attempts to add to the memo. An output
//      like 'AB CD' means a join tree containing relations A and B is being
//      joined to a join tree containing relations C and D. There is also a
//      'refs' field containing all relations that are referenced by the join's
//      ON condition.
// The final optimized plan is then output.
func (ot *OptTester) ReorderJoins() (string, error) {
	ot.builder.Reset()
	o := ot.makeOptimizer()

	// Map from the first ColumnID of each base relation to its assigned name.
	// this ensures that aliases are consistent across different calls to Reorder.
	names := map[opt.ColumnID]string{}

	// joinsConsidered counts the number of joins which joinOrderBuilder attempts
	// to add to the memo during each call to Reorder.
	var joinsConsidered int
	var treeNum = 1
	var relsJoinedLast string

	o.JoinOrderBuilder().NotifyOnReorder(
		func(
			join memo.RelExpr,
			vertexes []memo.RelExpr,
			edges []memo.FiltersExpr,
			edgeOps []opt.Operator,
		) {
			if treeNum > 1 {
				// This isn't the first Reorder call. Output the number of joins added to
				// the memo by the last call to Reorder.
				ot.builder.WriteString(fmt.Sprintf("\nJoins Considered: %v\n", joinsConsidered))
				joinsConsidered = 0
			}
			ot.builder.WriteString(separator("-"))
			ot.builder.WriteString(fmt.Sprintf("----Join Tree #%v----\n", treeNum))
			ot.builder.WriteString(o.FormatExpr(join, memo.ExprFmtHideAll))
			ot.builder.WriteString("\n----Vertexes----\n")
			ot.builder.WriteString(outputVertexes(vertexes, names, o))
			ot.builder.WriteString("----Edges----\n")
			for i := range edges {
				ot.builder.WriteString(outputEdge(edges[i], edgeOps[i], o))
			}
			ot.builder.WriteString("\n")
			treeNum++
			relsJoinedLast = ""
		})

	o.JoinOrderBuilder().NotifyOnAddJoin(func(left, right, all, refs []memo.RelExpr, op opt.Operator) {
		relsToJoin := outputRels(all, names)
		if relsToJoin != relsJoinedLast {
			ot.builder.WriteString(
				fmt.Sprintf(
					"----Joining %s----\n",
					relsToJoin,
				),
			)
			relsJoinedLast = relsToJoin
		}
		ot.builder.WriteString(
			fmt.Sprintf(
				"%s %s    refs [%s] [%s]\n",
				outputRels(left, names),
				outputRels(right, names),
				outputRels(refs, names),
				outputOp(op),
			),
		)
		joinsConsidered++
	})

	expr, err := ot.optimizeExpr(o)
	if err != nil {
		return "", err
	}
	ot.builder.WriteString(fmt.Sprintf("\nJoins Considered: %v\n", joinsConsidered))
	ot.builder.WriteString(separator("-"))
	ot.builder.WriteString("----Final Plan----\n")
	ot.builder.WriteString(ot.FormatExpr(expr))
	ot.builder.WriteString(separator("-"))
	return ot.builder.String(), err
}

// outputVertexes outputs each base relation in the vertexes slice along with
// its alias.
func outputVertexes(
	vertexes []memo.RelExpr, names map[opt.ColumnID]string, o *xform.Optimizer,
) string {
	buf := bytes.Buffer{}
	for i := range vertexes {
		firstCol, ok := vertexes[i].Relational().OutputCols.Next(0)
		if !ok {
			panic(errors.AssertionFailedf("failed to retrieve column from %v", vertexes[i].Op()))
		}
		name, ok := names[firstCol]
		if !ok {
			name = getRelationName(len(names))
			names[firstCol] = name
		}
		buf.WriteString(
			fmt.Sprintf(
				"%s:\n%s",
				name,
				o.FormatExpr(vertexes[i], memo.ExprFmtHideAll),
			),
		)
		buf.WriteString("\n")
	}
	return buf.String()
}

// outputEdge returns a formatted string for the given FiltersItem along with
// the type of join the edge came from, like so: "x = a left".
func outputEdge(edge memo.FiltersExpr, op opt.Operator, o *xform.Optimizer) string {
	buf := bytes.Buffer{}
	if len(edge) == 0 {
		buf.WriteString("cross")
	} else {
		for i := range edge {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(strings.TrimSuffix(o.FormatExpr(&edge[i], memo.ExprFmtHideAll), "\n"))
		}
	}
	buf.WriteString(fmt.Sprintf(" [%s]\n", outputOp(op)))
	return buf.String()
}

func outputOp(op opt.Operator) string {
	switch op {
	case opt.InnerJoinOp:
		return "inner"

	case opt.SemiJoinOp:
		return "semi"

	case opt.AntiJoinOp:
		return "anti"

	case opt.LeftJoinOp:
		return "left"

	case opt.FullJoinOp:
		return "full"

	default:
		panic(errors.AssertionFailedf("unexpected operator: %v", op))
	}
}

// outputRels returns a string with the aliases of the given base relations
// concatenated together. Panics if there is no alias for a base relation.
func outputRels(baseRels []memo.RelExpr, names map[opt.ColumnID]string) string {
	buf := bytes.Buffer{}
	for i := range baseRels {
		firstCol, ok := baseRels[i].Relational().OutputCols.Next(0)
		if !ok {
			panic(errors.AssertionFailedf("failed to retrieve column from %v", baseRels[i].Op()))
		}
		buf.WriteString(names[firstCol])
	}
	return buf.String()
}

// getRelationName returns a simple alias for a base relation given the number
// of names generated so far.
func getRelationName(nameCount int) string {
	const lenAlphabet = 26
	name := string(rune(int('A') + (nameCount % lenAlphabet)))
	number := nameCount / lenAlphabet
	if number > 0 {
		// Names will follow the pattern: A, B, ..., Z, A1, B1, etc.
		name += strconv.Itoa(number)
	}
	return name
}

func separator(sep string) string {
	return fmt.Sprintf("%s\n", strings.Repeat(sep, 80))
}
