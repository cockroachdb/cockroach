// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/json"
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

func (s Scalar) String() string {
	if s.Type == ScalarVariable {
		return fmt.Sprintf("$%q", s.Variable)
	}
	return s.Value.String()
}

func (s Scalar) Validate(vars map[string]int, nestingLevel int, insideArraySubscript bool) error {
	if s.Type == ScalarVariable {
		// If the variable is not in the map, add it with a strictly increasing
		// index, so we are able to sort the variables by their first appearance.
		if _, ok := vars[s.Variable]; !ok {
			vars[s.Variable] = len(vars)
		}
	}
	return nil
}
