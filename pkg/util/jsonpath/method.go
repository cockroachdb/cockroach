// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

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

func (m Method) ToString(sb *strings.Builder, _, _ bool) {
	switch m.Type {
	case SizeMethod, TypeMethod:
		sb.WriteString(fmt.Sprintf(".%s()", methodTypeStrings[m.Type]))
	default:
		panic(errors.AssertionFailedf("unhandled method type: %d", m.Type))
	}
}

func (m Method) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}
