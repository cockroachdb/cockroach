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
//
//

package parser

import "bytes"

// formatNodeOrHideConstants recurses into a node for pretty-printing,
// unless hideConstants is set in the flags and the node is a datum or
// a literal.
func formatNodeOrHideConstants(buf *bytes.Buffer, f FmtFlags, n NodeFormatter) {
	if f.hideConstants {
		switch v := n.(type) {
		case *ValuesClause:
			v.formatHideConstants(buf, f)
			return
		case *ComparisonExpr:
			if v.Operator == In || v.Operator == NotIn {
				if t, ok := v.Right.(*Tuple); ok {
					v.formatInTupleAndHideConstants(buf, f, t)
					return
				}
			}
		case Datum, Constant:
			buf.WriteByte('_')
			return
		}
	}
	n.Format(buf, f)
}

// formatInTupleAndHideConstants formats an "a IN (...)" expression
// and collapses the tuple on the right to contain at most 2 elements
// if it otherwise only contains literals or placeholders.
// e.g.:
//    a IN (1, 2, 3)       -> a IN (_, _)
//    a IN (x+1, x+2, x+3) -> a IN (x+_, x+_, x+_)
func (node *ComparisonExpr) formatInTupleAndHideConstants(
	buf *bytes.Buffer, f FmtFlags, rightTuple *Tuple,
) {
	exprFmtWithParen(buf, f, node.Left)
	buf.WriteByte(' ')
	buf.WriteString(node.Operator.String())
	buf.WriteByte(' ')
	rightTuple.formatHideConstants(buf, f)
}

// formatHideConstants shortens multi-valued VALUES clauses to a
// VALUES clause with a single value.
// e.g. VALUES (a,b,c), (d,e,f) -> VALUES (_, _, _)
func (node *ValuesClause) formatHideConstants(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("VALUES ")
	node.Tuples[0].Format(buf, f)
}

// formatHideConstants formats tuples containing only literals or
// placeholders and longer than 1 element as a tuple of its first
// two elements, scrubbed.
// e.g. (1)               -> (_)
//      (1, 2)            -> (_, _)
//      (1, 2, 3)         -> (_, _)
//      ROW()             -> ROW()
//      ROW($1, $2, $3)   -> ROW($1, $2)
//      (1+2, 2+3, 3+4)   -> (_ + _, _ + _, _ + _)
//      (1+2, b, c)       -> (_ + _, b, c)
func (node *Tuple) formatHideConstants(buf *bytes.Buffer, f FmtFlags) {
	if len(node.Exprs) < 2 {
		node.Format(buf, f)
		return
	}

	// First, determine if there are only literals/placeholders.
	var i int
	for i = 0; i < len(node.Exprs); i++ {
		switch node.Exprs[i].(type) {
		case Datum, Constant, *Placeholder:
			continue
		}
		break
	}
	// If so, then use the special representation.
	if i == len(node.Exprs) {
		// We copy the node to preserve the "row" boolean flag.
		v2 := *node
		v2.Exprs = v2.Exprs[:2]
		v2.Format(buf, f)
		return
	}
	node.Format(buf, f)
}
