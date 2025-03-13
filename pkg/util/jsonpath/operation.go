// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "fmt"

type OperationType int

const (
	OpCompEqual OperationType = iota
	OpCompNotEqual
	OpCompLess
	OpCompLessEqual
	OpCompGreater
	OpCompGreaterEqual
)

var operationTypeStrings = map[OperationType]string{
	OpCompEqual:        "==",
	OpCompNotEqual:     "!=",
	OpCompLess:         "<",
	OpCompLessEqual:    "<=",
	OpCompGreater:      ">",
	OpCompGreaterEqual: ">=",
}

type Operation struct {
	Type  OperationType
	Left  Path
	Right Path
}

var _ Path = Operation{}

func (o Operation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.Left, operationTypeStrings[o.Type], o.Right)
}
