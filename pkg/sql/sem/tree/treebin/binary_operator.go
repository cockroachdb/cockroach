// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package treebin

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// BinaryOperator represents a unary operator used in a BinaryExpr.
type BinaryOperator struct {
	Symbol BinaryOperatorSymbol
	// IsExplicitOperator is true if OPERATOR(symbol) is used.
	IsExplicitOperator bool
}

// MakeBinaryOperator creates a BinaryOperator given a symbol.
func MakeBinaryOperator(symbol BinaryOperatorSymbol) BinaryOperator {
	return BinaryOperator{Symbol: symbol}
}

func (o BinaryOperator) String() string {
	if o.IsExplicitOperator {
		return fmt.Sprintf("OPERATOR(%s)", o.Symbol.String())
	}
	return o.Symbol.String()
}

// Operator implements tree.Operator.
func (BinaryOperator) Operator() {}

// BinaryOperatorSymbol is a symbol for a binary operator.
type BinaryOperatorSymbol uint8

// BinaryExpr.Operator
const (
	Bitand BinaryOperatorSymbol = iota
	Bitor
	Bitxor
	Plus
	Minus
	Mult
	Div
	FloorDiv
	Mod
	Pow
	Concat
	LShift
	RShift
	JSONFetchVal
	JSONFetchText
	JSONFetchValPath
	JSONFetchTextPath

	NumBinaryOperatorSymbols
)

var _ = NumBinaryOperatorSymbols

var binaryOpName = [...]string{
	Bitand:            "&",
	Bitor:             "|",
	Bitxor:            "#",
	Plus:              "+",
	Minus:             "-",
	Mult:              "*",
	Div:               "/",
	FloorDiv:          "//",
	Mod:               "%",
	Pow:               "^",
	Concat:            "||",
	LShift:            "<<",
	RShift:            ">>",
	JSONFetchVal:      "->",
	JSONFetchText:     "->>",
	JSONFetchValPath:  "#>",
	JSONFetchTextPath: "#>>",
}

// IsPadded returns whether the binary operator needs to be padded.
func (i BinaryOperatorSymbol) IsPadded() bool {
	return !(i == JSONFetchVal || i == JSONFetchText || i == JSONFetchValPath || i == JSONFetchTextPath)
}

func (i BinaryOperatorSymbol) String() string {
	if i > BinaryOperatorSymbol(len(binaryOpName)-1) {
		return fmt.Sprintf("BinaryOp(%d)", i)
	}
	return binaryOpName[i]
}

// BinaryOpName returns the name of op.
func BinaryOpName(op BinaryOperatorSymbol) string {
	if int(op) >= len(binaryOpName) || binaryOpName[op] == "" {
		panic(errors.AssertionFailedf("missing name for operator %q", op.String()))
	}
	return binaryOpName[op]
}
