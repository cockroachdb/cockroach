// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type UnknownOption string

// apply is a no-op for UnknownOption, as it is used to test serialization
// failure of an unknown option.
func (o UnknownOption) apply(_ *ClusterSettings) {}

// getTypeName is a botched implementation for UnknownOption (which is also not
// present in the type register struct), which is used to test serialization
// failure. It should return an error when trying to get the type name of the
// option.
func (o UnknownOption) getTypeName(t *ClusterSettingOptionTypes) (string, error) {
	// This call is structured incorrectly to trigger an error in serialization.
	// It should be `t.getTypeName(o, t.UnknownOption)`, but since `UnknownOption`
	// has not been added to the `ClusterSettingOptionTypes` struct, it will not
	// be found. Any new Dynamic Type should be added to the
	// ClusterSettingOptionTypes.
	return t.getTypeName(o, &o)
}

// TestClusterSettingOptionListSerde tests that a ClusterSettingOptionList can
// be serialized and deserialized correctly.
func TestClusterSettingOptionListSerde(t *testing.T) {
	o1 := ClusterSettingOptionList{TagOption("test"), SecureOption(true), EnvOption{"a", "b"}}
	data, err := yaml.Marshal(o1)
	require.NoError(t, err)

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	var o2 ClusterSettingOptionList
	err = dec.Decode(&o2)
	require.NoError(t, err)
	require.Equal(t, o1, o2)
}

// TestSerializeUnknownOptionError tests that an error is returned when trying
// to serialize a ClusterSettingOptionList that contains an unknown option
// (i.e., an option type that has not been added to the
// `clusterSettingOptionTypes` list).
func TestSerializeUnknownOptionError(t *testing.T) {
	o1 := ClusterSettingOptionList{UnknownOption("bad")}
	_, err := yaml.Marshal(o1)
	require.Error(t, err)
}

// TestClusterSettingTypes tests that all registered ClusterSettingOptionTypes
// can provide their type name without error.
func TestClusterSettingTypes(t *testing.T) {
	types := ClusterSettingOptionTypes{}
	tValue := reflect.ValueOf(types)
	for i := 0; i < tValue.NumField(); i++ {
		field := tValue.Field(i)
		opt := field.Interface().(ClusterSettingOption)
		_, err := opt.getTypeName(&types)
		require.NoError(t, err)
	}
}
