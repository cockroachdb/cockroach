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
)

type op int

const (
	invalidOp op = iota
	plusOp
	minusOp
	mulOp
	divOp

	eqOp
	neOp
	ltOp
	lteOp
	gtOp
	gteOp
)

var opNames = map[op]string{
	plusOp:  "Plus",
	minusOp: "Minus",
	mulOp:   "Mul",
	eqOp:    "EQ",
	neOp:    "NE",
	ltOp:    "LT",
	lteOp:   "LTE",
	gtOp:    "GT",
	gteOp:   "GTE",
}

type overload struct {
	Name   string
	OpStr  string
	LTyp   types.T
	RTyp   types.T
	RetTyp types.T
}

func makeOverload(t types.T, opStr string) overload {
	return overload{
		OpStr:  opStr,
		LTyp:   t,
		RTyp:   t,
		RetTyp: t,
	}
}

func makePredOverload(t types.T, opStr string) overload {
	return overload{
		OpStr:  opStr,
		LTyp:   t,
		RTyp:   t,
		RetTyp: types.Bool,
	}
}

// TODO: Autogenerate this collection.
var opMap = map[op][]overload{
	plusOp: {
		makeOverload(types.Int8, "+"),
		makeOverload(types.Int16, "+"),
		makeOverload(types.Int32, "+"),
		makeOverload(types.Int64, "+"),
		makeOverload(types.Float32, "+"),
		makeOverload(types.Float64, "+"),
	},
	minusOp: {
		makeOverload(types.Int8, "-"),
		makeOverload(types.Int16, "-"),
		makeOverload(types.Int32, "-"),
		makeOverload(types.Int64, "-"),
		makeOverload(types.Float32, "-"),
		makeOverload(types.Float64, "-"),
	},
	mulOp: {
		makeOverload(types.Int8, "*"),
		makeOverload(types.Int16, "*"),
		makeOverload(types.Int32, "*"),
		makeOverload(types.Int64, "*"),
		makeOverload(types.Float32, "*"),
		makeOverload(types.Float64, "*"),
	},
	divOp: {
		makeOverload(types.Int8, "/"),
		makeOverload(types.Int16, "/"),
		makeOverload(types.Int32, "/"),
		makeOverload(types.Int64, "/"),
		makeOverload(types.Float32, "/"),
		makeOverload(types.Float64, "/"),
	},
	eqOp: {
		makePredOverload(types.Bool, "=="),
		makePredOverload(types.Int8, "=="),
		makePredOverload(types.Int16, "=="),
		makePredOverload(types.Int32, "=="),
		makePredOverload(types.Int64, "=="),
		makePredOverload(types.Float32, "=="),
		makePredOverload(types.Float64, "=="),
	},
	neOp: {
		makePredOverload(types.Bool, "!="),
		makePredOverload(types.Int8, "!="),
		makePredOverload(types.Int16, "!="),
		makePredOverload(types.Int32, "!="),
		makePredOverload(types.Int64, "!="),
		makePredOverload(types.Float32, "!="),
		makePredOverload(types.Float64, "!="),
	},
	ltOp: {
		makePredOverload(types.Int8, "<"),
		makePredOverload(types.Int16, "<"),
		makePredOverload(types.Int32, "<"),
		makePredOverload(types.Int64, "<"),
		makePredOverload(types.Float32, "<"),
		makePredOverload(types.Float64, "<"),
	},
	lteOp: {
		makePredOverload(types.Int8, "<="),
		makePredOverload(types.Int16, "<="),
		makePredOverload(types.Int32, "<="),
		makePredOverload(types.Int64, "<="),
		makePredOverload(types.Float32, "<="),
		makePredOverload(types.Float64, "<="),
	},
	gtOp: {
		makePredOverload(types.Int8, ">"),
		makePredOverload(types.Int16, ">"),
		makePredOverload(types.Int32, ">"),
		makePredOverload(types.Int64, ">"),
		makePredOverload(types.Float32, ">"),
		makePredOverload(types.Float64, ">"),
	},
	gteOp: {
		makePredOverload(types.Int8, ">="),
		makePredOverload(types.Int16, ">="),
		makePredOverload(types.Int32, ">="),
		makePredOverload(types.Int64, ">="),
		makePredOverload(types.Float32, ">="),
		makePredOverload(types.Float64, ">="),
	},
}

func init() {
	for i := range opMap {
		for j := range opMap[i] {
			opMap[i][j].Name = opNames[i]
		}
	}
}
