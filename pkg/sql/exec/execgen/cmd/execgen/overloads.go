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

package main

import (
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var binaryOpName = map[tree.BinaryOperator]string{
	tree.Plus:  "Plus",
	tree.Minus: "Minus",
	tree.Mult:  "Mult",
	tree.Div:   "Div",
}

var comparisonOpName = map[tree.ComparisonOperator]string{
	tree.EQ: "EQ",
	tree.NE: "NE",
	tree.LT: "LT",
	tree.LE: "LE",
	tree.GT: "GT",
	tree.GE: "GE",
}

var binaryOpInfix = map[tree.BinaryOperator]string{
	tree.Plus:  "+",
	tree.Minus: "-",
	tree.Mult:  "*",
	tree.Div:   "/",
}

var comparisonOpInfix = map[tree.ComparisonOperator]string{
	tree.EQ: "==",
	tree.NE: "!=",
	tree.LT: "<",
	tree.LE: "<=",
	tree.GT: ">",
	tree.GE: ">=",
}

type overload struct {
	Name    string
	OpStr   string
	LTyp    types.T
	RTyp    types.T
	RGoType string
	RetTyp  types.T
}

var binaryOpOverloads []overload
var comparisonOpOverloads []overload

func init() {
	// Build overload definitions.
	inputTypes := []types.T{
		types.Int8, types.Int16, types.Int32, types.Int64, types.Float32, types.Float64}
	binOps := []tree.BinaryOperator{tree.Plus, tree.Minus, tree.Mult, tree.Div}
	cmpOps := []tree.ComparisonOperator{tree.EQ, tree.NE, tree.LT, tree.LE, tree.GT, tree.GE}
	for _, t := range inputTypes {
		for _, op := range binOps {
			ov := overload{
				Name:    binaryOpName[op],
				OpStr:   binaryOpInfix[op],
				LTyp:    t,
				RTyp:    t,
				RGoType: t.GoTypeName(),
				RetTyp:  t,
			}
			binaryOpOverloads = append(binaryOpOverloads, ov)
		}
		for _, op := range cmpOps {
			ov := overload{
				Name:    comparisonOpName[op],
				OpStr:   comparisonOpInfix[op],
				LTyp:    t,
				RTyp:    t,
				RGoType: t.GoTypeName(),
				RetTyp:  types.Bool,
			}
			comparisonOpOverloads = append(comparisonOpOverloads, ov)
		}
	}
}
