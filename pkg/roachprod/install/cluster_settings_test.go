// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClusterSettingOptionListCodec(t *testing.T) {
	opts := ClusterSettingOptionList{
		NumRacksOption(1),
		DebugDirOption("foo"),
	}
	data, err := yaml.Marshal(opts)
	require.NoError(t, err)

	var decOpts ClusterSettingOptionList
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	err = dec.Decode(&decOpts)
	require.NoError(t, err)

	require.Equal(t, opts, decOpts)
	require.Equal(t, MakeClusterSettings(opts...), MakeClusterSettings(decOpts...))
}
