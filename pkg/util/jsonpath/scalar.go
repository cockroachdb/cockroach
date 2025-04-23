// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type ScalarType int

const (
	ScalarInt ScalarType = iota
	ScalarFloat
	ScalarString
	ScalarBool
	ScalarNull
	ScalarVariable
)

type Scalar struct {
	Type     ScalarType
	Value    json.JSON
	Variable string
}

var _ Path = Scalar{}

func (s Scalar) ToString(sb *strings.Builder, _, _ bool) {
	switch s.Type {
	case ScalarInt, ScalarFloat, ScalarString, ScalarBool, ScalarNull:
		sb.WriteString(s.Value.String())
		return
	case ScalarVariable:
		sb.WriteString(fmt.Sprintf("$%q", s.Variable))
		return
	default:
		panic(errors.AssertionFailedf("unhandled scalar type: %d", s.Type))
	}
}

func (s Scalar) Validate(nestingLevel int, insideArraySubscript bool) error {
	return nil
}
