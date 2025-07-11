// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingutil

import (
	"fmt"

	"github.com/gogo/protobuf/types"
)

// testStructuredImpl is a testing implementation of Structured event.
type TestStructuredImpl struct {
	*types.StringValue
}

func (t *TestStructuredImpl) String() string {
	return fmt.Sprintf("structured=%s", t.Value)
}

func NewTestStructured(s string) *TestStructuredImpl {
	return &TestStructuredImpl{
		&types.StringValue{Value: s},
	}
}
