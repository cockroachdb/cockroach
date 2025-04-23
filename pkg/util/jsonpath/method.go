// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import "fmt"

type MethodType int

const (
	InvalidMethod MethodType = iota
	SizeMethod
	TypeMethod
)

var methodTypeStrings = [...]string{
	SizeMethod: "size",
	TypeMethod: "type",
}

type Method struct {
	Type MethodType
}

var _ Path = Method{}

func (m Method) String() string {
	if int(m.Type) < 0 || int(m.Type) >= len(methodTypeStrings) || m.Type == InvalidMethod {
		panic(fmt.Sprintf("invalid method type: %d", m.Type))
	}
	return fmt.Sprintf(".%s()", methodTypeStrings[m.Type])
}

func (m Method) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}
