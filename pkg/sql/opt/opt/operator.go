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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

//go:generate optgen -out operator.og.go ops ../ops/scalar.opt ../ops/relational.opt ../ops/enforcer.opt

// Operator describes the type of operation that a memo expression performs.
// Some operators are relational (join, select, project) and others are scalar
// (and, or, plus, variable).
type Operator uint16

// String returns the name of the operator as a string.
func (i Operator) String() string {
	if i >= Operator(len(opNames)-1) {
		return fmt.Sprintf("Operator(%d)", i)
	}

	return opNames[opIndexes[i]:opIndexes[i+1]]
}

// ComparisonOpReverseMap maps from an optimizer operator type to a semantic
// tree comparison operator type.
var ComparisonOpReverseMap = [...]tree.ComparisonOperator{
	EqOp:           tree.EQ,
	LtOp:           tree.LT,
	GtOp:           tree.GT,
	LeOp:           tree.LE,
	GeOp:           tree.GE,
	NeOp:           tree.NE,
	InOp:           tree.In,
	NotInOp:        tree.NotIn,
	LikeOp:         tree.Like,
	NotLikeOp:      tree.NotLike,
	ILikeOp:        tree.ILike,
	NotILikeOp:     tree.NotILike,
	SimilarToOp:    tree.SimilarTo,
	NotSimilarToOp: tree.NotSimilarTo,
	RegMatchOp:     tree.RegMatch,
	NotRegMatchOp:  tree.NotRegMatch,
	RegIMatchOp:    tree.RegIMatch,
	NotRegIMatchOp: tree.NotRegIMatch,
	IsOp:           tree.IsNotDistinctFrom,
	IsNotOp:        tree.IsDistinctFrom,
	ContainsOp:     tree.Contains,
}

// BinaryOpReverseMap maps from an optimizer operator type to a semantic tree
// binary operator type.
var BinaryOpReverseMap = [...]tree.BinaryOperator{
	BitandOp:        tree.Bitand,
	BitorOp:         tree.Bitor,
	BitxorOp:        tree.Bitxor,
	PlusOp:          tree.Plus,
	MinusOp:         tree.Minus,
	MultOp:          tree.Mult,
	DivOp:           tree.Div,
	FloorDivOp:      tree.FloorDiv,
	ModOp:           tree.Mod,
	PowOp:           tree.Pow,
	ConcatOp:        tree.Concat,
	LShiftOp:        tree.LShift,
	RShiftOp:        tree.RShift,
	FetchValOp:      tree.JSONFetchVal,
	FetchTextOp:     tree.JSONFetchText,
	FetchValPathOp:  tree.JSONFetchValPath,
	FetchTextPathOp: tree.JSONFetchTextPath,
}

// UnaryOpReverseMap maps from an optimizer operator type to a semantic tree
// unary operator type.
var UnaryOpReverseMap = [...]tree.UnaryOperator{
	UnaryPlusOp:       tree.UnaryPlus,
	UnaryMinusOp:      tree.UnaryMinus,
	UnaryComplementOp: tree.UnaryComplement,
}

// DisambiguateFunction takes a function's name and return type and returns a
// name that is guaranteed to be "non-ambiguous". A non-ambiguous name means
// that the name plus the argument types is enough to infer the return type.
// This is important for simplifying the type inference rules in typing.go, and
// making it easy to match and construct Function operators in transformation
// patterns.
func DisambiguateFunction(name string, returnType types.T) (disambiguatedName string) {
	_, ok := ambiguousFunctions[name]
	if !ok {
		return name
	}
	return fmt.Sprintf("%s:::%s", name, returnType.String())
}

// RecoverAmbiguousFunction returns the function's original ambiguous name and
// return type, given a disambiguated name returned by a previous call to
// DisambiguateFunction.
func RecoverAmbiguousFunction(disambiguatedName string) (name string, returnType types.T) {
	existing, ok := disambiguatedFunctions[disambiguatedName]
	if !ok {
		panic(fmt.Sprintf("%s is not an ambiguous function name", disambiguatedName))
	}
	return existing.name, existing.returnType
}

var ambiguousFunctions = map[string]bool{
	"now":                   true,
	"clock_timestamp":       true,
	"transaction_timestamp": true,
	"current_timestamp":     true,
	"statement_timestamp":   true,
}

type disambiguatedInfo struct {
	name       string
	returnType types.T
}

var disambiguatedFunctions = map[string]disambiguatedInfo{
	"now:::timestamp": {
		name:       "now",
		returnType: types.Timestamp,
	},
	"now:::timestamptz": {
		name:       "now",
		returnType: types.TimestampTZ,
	},
	"clock_timestamp:::timestamp": {
		name:       "clock_timestamp",
		returnType: types.Timestamp,
	},
	"clock_timestamp:::timestamptz": {
		name:       "clock_timestamptz",
		returnType: types.TimestampTZ,
	},
	"transaction_timestamp:::timestamp": {
		name:       "transaction_timestamp",
		returnType: types.Timestamp,
	},
	"transaction_timestamp:::timestamptz": {
		name:       "transaction_timestamp",
		returnType: types.TimestampTZ,
	},
	"current_timestamp:::timestamp": {
		name:       "current_timestamp",
		returnType: types.Timestamp,
	},
	"current_timestamp:::timestamptz": {
		name:       "current_timestamp",
		returnType: types.TimestampTZ,
	},
	"statement_timestamp:::timestamp": {
		name:       "statement_timestamp",
		returnType: types.Timestamp,
	},
	"statement_timestamp:::timestamptz": {
		name:       "statement_timestamp",
		returnType: types.TimestampTZ,
	},
}
