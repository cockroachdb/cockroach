// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// encodeAndDecode encodes a TestPlan to YAML and then decodes it back to a TestPlan.
// It returns the decoded TestPlan or any error encountered during encoding or decoding.
func encodeAndDecode(plan *TestPlan) (*TestPlan, error) {
	buf, err := yaml.Marshal(plan)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(buf))
	decoder.KnownFields(true)

	var decodedPlan TestPlan
	err = decoder.Decode(&decodedPlan)
	if err != nil {
		return nil, err
	}
	return &decodedPlan, nil
}

// TestSerializedPlanEquality tests that a TestPlan can be serialized to YAML
// and deserialized back to an equivalent TestPlan. It ensures that the original
// and deserialized plans are equal.
func TestSerializedPlanEquality(t *testing.T) {
	defer setDefaultVersions()
	mvt := newTest()
	mvt.InMixedVersion("mixed-version 1", dummyHook)
	mvt.InMixedVersion("mixed-version 2", dummyHook)
	plan, err := mvt.plan()
	require.NoError(t, err)
	np, err := encodeAndDecode(plan)
	require.NoError(t, err)
	require.Equal(t, plan, np)
}

// TestStepTypes ensures that all steps correctly implement the dynamicStep
// interface and can retrieve their type name from the `stepTypes` struct
// without errors.
func TestStepTypes(t *testing.T) {
	types := stepTypes{}
	tValue := reflect.ValueOf(types)
	for i := 0; i < tValue.NumField(); i++ {
		field := tValue.Field(i)
		step := field.Interface().(dynamicStep)
		_, err := step.getTypeName(&types)
		require.NoError(t, err)
	}
}
