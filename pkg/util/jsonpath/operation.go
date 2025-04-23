// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "fmt"

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

func (o Operation) String() string {
	if int(o.Type) < 0 || int(o.Type) >= len(OperationTypeStrings) || o.Type == OpInvalid {
		panic(fmt.Sprintf("invalid operation type: %d", o.Type))
	}
	// TODO(normanchenn): Fix recursive brackets. When there is a operation like
	// 1 == 1 && 1 != 1, postgres will output (1 == 1 && 1 != 1), but we output
	// ((1 == 1) && (1 != 1)).
	if o.Type == OpLogicalNot {
		return fmt.Sprintf("%s(%s)", OperationTypeStrings[o.Type], o.Left)
	}
	if o.Type == OpPlus || o.Type == OpMinus {
		return fmt.Sprintf("(%s%s)", OperationTypeStrings[o.Type], o.Left)
	}
	if o.Type == OpExists {
		return fmt.Sprintf("%s (%s)", OperationTypeStrings[o.Type], o.Left)
	}
	if o.Type == OpIsUnknown {
		return fmt.Sprintf("(%s) %s", o.Left, OperationTypeStrings[o.Type])
	}
	return fmt.Sprintf("(%s %s %s)", o.Left, OperationTypeStrings[o.Type], o.Right)
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
