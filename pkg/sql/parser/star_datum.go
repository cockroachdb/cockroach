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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import "bytes"

// StarDatum is a VariableExpr implementation used as a dummy argument
// for the special case COUNT(*).  This ends up being processed
// correctly by the count aggregator since it is not DNull. Other
// instances of '*' as render expressions are handled by expandStar().
//
// We need to implement enough functionality to satisfy the type checker and to
// allow the intermediate rendering of the value (before the group
// aggregation).
type StarDatum struct{}

// StarDatumInstance can be used as common instance for all uses
// of StarDatum.
var StarDatumInstance = &StarDatum{}
var _ TypedExpr = StarDatumInstance
var _ VariableExpr = StarDatumInstance

// Variable implements the VariableExpr interface.
func (*StarDatum) Variable() {}

// Format implements the NodeFormatter interface.
func (*StarDatum) Format(buf *bytes.Buffer, f FmtFlags) {
	if f.starDatumFormat != nil {
		f.starDatumFormat(buf, f)
	} else {
		buf.WriteByte('*')
	}
}

func (s *StarDatum) String() string { return AsString(s) }

// Walk implements the Expr interface.
func (s *StarDatum) Walk(v Visitor) Expr { return s }

// TypeCheck implements the Expr interface.
func (s *StarDatum) TypeCheck(_ *SemaContext, _ Type) (TypedExpr, error) {
	return s, nil
}

var constDInt = DInt(0)

// Eval implements the TypedExpr interface.
func (*StarDatum) Eval(ctx *EvalContext) (Datum, error) {
	return &constDInt, nil
}

// ResolvedType implements the TypedExpr interface.
func (*StarDatum) ResolvedType() Type {
	return TypeInt
}
