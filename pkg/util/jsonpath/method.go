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

var MethodTypeStrings = map[MethodType]string{
	SizeMethod: "size",
	TypeMethod: "type",
}

type Method struct {
	Type MethodType
}

var _ Path = Method{}

func (m Method) String() string {
	return fmt.Sprintf(".%s()", MethodTypeStrings[m.Type])
}
