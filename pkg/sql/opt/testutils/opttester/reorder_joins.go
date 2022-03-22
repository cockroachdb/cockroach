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
	jof := newJoinOrderFormatter(o)

	// joinsConsidered counts the number of joins which joinOrderBuilder attempts
	// to add to the memo during each call to Reorder.
	var joinsConsidered int
	var treeNum = 1
	var relsJoinedLast string

	o.JoinOrderBuilder().NotifyOnReorder(
		func(
			join memo.RelExpr,
			vertexes []memo.RelExpr,
			edges []xform.OnReorderEdgeParam,
		) {
			if treeNum > 1 {
				// This isn't the first Reorder call. Output the number of joins added to
				// the memo by the last call to Reorder.
				ot.output(fmt.Sprintf("Joins Considered: %v\n", joinsConsidered))
				joinsConsidered = 0
			}
			ot.separator("-")
			ot.output(fmt.Sprintf("Join Tree #%d\n", treeNum))
			ot.separator("-")
			ot.indent(o.FormatExpr(join, memo.ExprFmtHideAll))
			ot.output("Vertexes\n")
			for i := range vertexes {
				ot.indent(jof.formatVertex(vertexes[i]))
			}
			ot.output("Edges\n")
			for i := range edges {
				ot.indent(jof.formatEdge(edges[i]))
			}
			treeNum++
			relsJoinedLast = ""
		})

	o.JoinOrderBuilder().NotifyOnAddJoin(func(left, right, all, refs []memo.RelExpr, op opt.Operator) {
		relsToJoin := jof.formatVertexSet(all)
		if relsToJoin != relsJoinedLast {
			ot.output(fmt.Sprintf("Joining %s\n", relsToJoin))
			relsJoinedLast = relsToJoin
		}
		ot.indent(
			fmt.Sprintf(
				"%s %s [%s, refs=%s]",
				jof.formatVertexSet(left),
				jof.formatVertexSet(right),
				joinOpLabel(op),
				jof.formatVertexSet(refs),
			),
		)
		joinsConsidered++
	})

	expr, err := ot.optimizeExpr(o, nil)
	if err != nil {
		return "", err
	}
	ot.output(fmt.Sprintf("Joins Considered: %d\n", joinsConsidered))
	ot.separator("=")
	ot.output("Final Plan\n")
	ot.separator("=")
	ot.output(ot.FormatExpr(expr))
	return ot.builder.String(), err
}

type joinOrderFormatter struct {
	o *xform.Optimizer

	// relLabels is a map from the first ColumnID of each base relation to its
	// assigned label.
	relLabels map[opt.ColumnID]string
}

// newJoinOrderFormatter returns an initialized joinOrderFormatter.
func newJoinOrderFormatter(o *xform.Optimizer) *joinOrderFormatter {
	return &joinOrderFormatter{
		o:         o,
		relLabels: make(map[opt.ColumnID]string),
	}
}

// formatVertex outputs each base relation in the vertexes slice along with
// its alias.
func (jof *joinOrderFormatter) formatVertex(vertex memo.RelExpr) string {
	var b strings.Builder
	b.WriteString(jof.relLabel(vertex))
	b.WriteString(":\n")
	expr := jof.o.FormatExpr(vertex, memo.ExprFmtHideAll)
	expr = strings.TrimRight(expr, " \n\t\r")
	lines := strings.Split(expr, "\n")
	for _, line := range lines {
		b.WriteString(fmt.Sprintf("  %s\n", line))
	}
	return b.String()
}

// formatVertexSet outputs each base relation in the vertexes slice along with
// its alias.
func (jof *joinOrderFormatter) formatVertexSet(vertexSet []memo.RelExpr) string {
	var b strings.Builder
	for i := range vertexSet {
		b.WriteString(jof.relLabel(vertexSet[i]))
	}
	return b.String()
}

// formatEdge returns a formatted string for the given FiltersItem along with
// the type of join the edge came from, like so: "x = a left".
func (jof *joinOrderFormatter) formatEdge(edge xform.OnReorderEdgeParam) string {
	var b strings.Builder
	if len(edge.Filters) == 0 {
		b.WriteString("cross")
	} else {
		for i := range edge.Filters {
			if i != 0 {
				b.WriteString(", ")
			}
			b.WriteString(strings.TrimSuffix(jof.o.FormatExpr(&edge.Filters[i], memo.ExprFmtHideAll), "\n"))
		}
	}
	b.WriteString(fmt.Sprintf(
		" [%s, ses=%s, tes=%s, rules=%s]",
		joinOpLabel(edge.Op),
		jof.formatVertexSet(edge.SES),
		jof.formatVertexSet(edge.TES),
		jof.formatRules(edge.Rules),
	))
	return b.String()
}

func (jof *joinOrderFormatter) formatRules(rules []xform.OnReorderRuleParam) string {
	var b strings.Builder
	b.WriteRune('(')
	for i, rule := range rules {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteString(fmt.Sprintf(
			"%s->%s",
			jof.formatVertexSet(rule.From),
			jof.formatVertexSet(rule.To),
		))
	}
	b.WriteRune(')')
	return b.String()
}

// relLabel returns the label for the given relation. Labels will follow the
// pattern A, B, ..., Z, A1, B1, etc.
func (jof *joinOrderFormatter) relLabel(e memo.RelExpr) string {
	firstCol, ok := e.Relational().OutputCols.Next(0)
	if !ok {
		panic(errors.AssertionFailedf("failed to retrieve column from %v", e.Op()))
	}
	if label, ok := jof.relLabels[firstCol]; ok {
		return label
	}
	const lenAlphabet = 26
	labelCount := len(jof.relLabels)
	label := string(rune(int('A') + (labelCount % lenAlphabet)))
	number := labelCount / lenAlphabet
	if number > 0 {
		// Names will follow the pattern: A, B, ..., Z, A1, B1, etc.
		label += strconv.Itoa(number)
	}
	jof.relLabels[firstCol] = label
	return label
}

// joinOpLabel returns an abbreviated string representation of a join operator.
func joinOpLabel(op opt.Operator) string {
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
