// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sql

// This file implements the necessary machinery to allow filter expressions to
// be evaluated at the scanNode level.

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// scanQValue implements the parser.VariableExpr interface and is used as a replacement node for
// QualifiedNames in expressions that can change their values for each row. Analogous to qvalue but
// works in the context of a scanNode.
type scanQValue struct {
	scan   *scanNode
	colIdx int
}

var _ parser.VariableExpr = &scanQValue{}

func (*scanQValue) Variable() {}

func (q *scanQValue) String() string {
	return string(q.scan.resultColumns[q.colIdx].Name)
}

func (q *scanQValue) Walk(_ parser.Visitor) parser.Expr {
	panic("not implemented")
}

func (q *scanQValue) TypeCheck(args parser.MapArgs) (parser.Datum, error) {
	return q.scan.resultColumns[q.colIdx].Typ.TypeCheck(args)
}

func (q *scanQValue) Eval(ctx parser.EvalContext) (parser.Datum, error) {
	return q.scan.row[q.colIdx].Eval(ctx)
}

func (s *scanNode) makeQValue(colIdx int) scanQValue {
	if colIdx < 0 || colIdx >= len(s.row) {
		panic(fmt.Sprintf("invalid colIdx %d (columns: %d)", colIdx, len(s.row)))
	}
	return scanQValue{s, colIdx}
}

func (s *scanNode) getQValue(colIdx int) *scanQValue {
	return &s.qvals[colIdx]
}
