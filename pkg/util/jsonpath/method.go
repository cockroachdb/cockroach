// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "fmt"

type MethodType int

const (
	SizeMethod MethodType = iota
	TypeMethod
)

var MethodTypeStrings = [...]string{
	SizeMethod: "size",
	TypeMethod: "type",
}

type Method struct {
	Type MethodType
}

var _ Path = Method{}

func (m Method) String() string {
	if int(m.Type) < 0 || int(m.Type) >= len(MethodTypeStrings) {
		panic(fmt.Sprintf("invalid method type: %d", m.Type))
	}
	return fmt.Sprintf(".%s()", MethodTypeStrings[m.Type])
}

func (m Method) Validate(
	vars map[string]struct{}, nestingLevel int, insideArraySubscript bool,
) error {
	return nil
}
