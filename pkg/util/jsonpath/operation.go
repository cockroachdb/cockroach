// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"strings"

	"github.com/cockroachdb/errors"
)

type OperationType int

const (
	OpInvalid OperationType = iota
	OpCompEqual
	OpCompNotEqual
	OpCompLess
	OpCompLessEqual
	OpCompGreater
	OpCompGreaterEqual
	OpLogicalAnd
	OpLogicalOr
	OpLogicalNot
	OpAdd
	OpSub
	OpMult
	OpDiv
	OpMod
	OpLikeRegex
	OpPlus
	OpMinus
	OpExists
	OpIsUnknown
	OpStartsWith
)

var OperationTypeStrings = [...]string{
	OpCompEqual:        "==",
	OpCompNotEqual:     "!=",
	OpCompLess:         "<",
	OpCompLessEqual:    "<=",
	OpCompGreater:      ">",
	OpCompGreaterEqual: ">=",
	OpLogicalAnd:       "&&",
	OpLogicalOr:        "||",
	OpLogicalNot:       "!",
	OpAdd:              "+",
	OpSub:              "-",
	OpMult:             "*",
	OpDiv:              "/",
	OpMod:              "%",
	OpLikeRegex:        "like_regex",
	OpPlus:             "+",
	OpMinus:            "-",
	OpExists:           "exists",
	OpIsUnknown:        "is unknown",
	OpStartsWith:       "starts with",
}

type Operation struct {
	Type  OperationType
	Left  Path
	Right Path
}

var _ Path = Operation{}

func (o Operation) ToString(sb *strings.Builder, _, printBrackets bool) {
	switch o.Type {
	case OpCompEqual, OpCompNotEqual, OpCompLess, OpCompLessEqual,
		OpCompGreater, OpCompGreaterEqual, OpLogicalAnd, OpLogicalOr, OpAdd,
		OpSub, OpMult, OpDiv, OpMod, OpStartsWith, OpLikeRegex:
		if printBrackets {
			sb.WriteString("(")
			defer sb.WriteString(")")
		}
		o.Left.ToString(sb, false /* inKey */, opPriority(o.Left) <= opPriority(o))
		sb.WriteString(" ")
		sb.WriteString(OperationTypeStrings[o.Type])
		sb.WriteString(" ")
		o.Right.ToString(sb, false /* inKey */, opPriority(o.Right) <= opPriority(o))
		return
	case OpLogicalNot:
		sb.WriteString("!(")
		o.Left.ToString(sb, false /* inKey */, false /* printBrackets */)
		sb.WriteString(")")
		return
	case OpPlus, OpMinus:
		if printBrackets {
			sb.WriteString("(")
			defer sb.WriteString(")")
		}
		sb.WriteString(OperationTypeStrings[o.Type])
		o.Left.ToString(sb, false /* inKey */, opPriority(o.Left) <= opPriority(o))
		return
	case OpExists:
		sb.WriteString("exists (")
		o.Left.ToString(sb, false /* inKey */, false /* printBrackets */)
		sb.WriteString(")")
		return
	case OpIsUnknown:
		sb.WriteString("(")
		o.Left.ToString(sb, false /* inKey */, false /* printBrackets */)
		sb.WriteString(") is unknown")
		return
	default:
		panic(errors.AssertionFailedf("unhandled operation type: %d", o.Type))
	}
}

func (o Operation) Validate(nestingLevel int, insideArraySubscript bool) error {
	if err := o.Left.Validate(nestingLevel, insideArraySubscript); err != nil {
		return err
	}
	if o.Right != nil {
		if err := o.Right.Validate(nestingLevel, insideArraySubscript); err != nil {
			return err
		}
	}
	return nil
}

func opPriority(path Path) int {
	switch path := path.(type) {
	case Operation:
		switch path.Type {
		case OpLogicalOr:
			return 0
		case OpLogicalAnd:
			return 1
		case OpCompEqual, OpCompNotEqual, OpCompLess, OpCompLessEqual,
			OpCompGreater, OpCompGreaterEqual, OpStartsWith:
			return 2
		case OpAdd, OpSub:
			return 3
		case OpMult, OpDiv, OpMod:
			return 4
		case OpPlus, OpMinus:
			return 5
		default:
			return 6
		}
	default:
		return 6
	}
}
