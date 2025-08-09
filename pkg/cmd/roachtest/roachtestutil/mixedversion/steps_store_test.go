// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStepProtocolTypes tests that all registered `stepProtocolTypes`
// can provide their type name without error.
func TestStepProtocolTypes(t *testing.T) {
	types := stepProtocolTypes{}
	tValue := reflect.ValueOf(types)
	for i := 0; i < tValue.NumField(); i++ {
		field := tValue.Field(i)
		step := field.Interface().(singleStepProtocol)
		_, err := step.getTypeName(&types)
		require.NoError(t, err)
	}
}
