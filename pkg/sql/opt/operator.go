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
)

//go:generate optgen -out operator.og.go ops ops/*.opt

// Operator describes the type of operation that a memo expression performs.
// Some operators are relational (join, select, project) and others are scalar
// (and, or, plus, variable).
type Operator uint16

// MaxOperands is the maximum number of operands that an operator can have.
// Increasing this limit can have a large memory impact, as every memo
// expression uses memory for the max number of operands, even if it does not
// have that many.
const MaxOperands = 3

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
	EqOp:             tree.EQ,
	LtOp:             tree.LT,
	GtOp:             tree.GT,
	LeOp:             tree.LE,
	GeOp:             tree.GE,
	NeOp:             tree.NE,
	InOp:             tree.In,
	NotInOp:          tree.NotIn,
	LikeOp:           tree.Like,
	NotLikeOp:        tree.NotLike,
	ILikeOp:          tree.ILike,
	NotILikeOp:       tree.NotILike,
	SimilarToOp:      tree.SimilarTo,
	NotSimilarToOp:   tree.NotSimilarTo,
	RegMatchOp:       tree.RegMatch,
	NotRegMatchOp:    tree.NotRegMatch,
	RegIMatchOp:      tree.RegIMatch,
	NotRegIMatchOp:   tree.NotRegIMatch,
	IsOp:             tree.IsNotDistinctFrom,
	IsNotOp:          tree.IsDistinctFrom,
	ContainsOp:       tree.Contains,
	JsonExistsOp:     tree.JSONExists,
	JsonSomeExistsOp: tree.JSONSomeExists,
	JsonAllExistsOp:  tree.JSONAllExists,
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
	UnaryMinusOp:      tree.UnaryMinus,
	UnaryComplementOp: tree.UnaryComplement,
}
