// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bytes"
	"testing"

	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
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

func TestComplexSecureOptionE2EProjects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		project  string
		insecure bool
	}{
		{name: "production", project: gce.DefaultProjectID, insecure: true},
		{name: "staging", project: gce.StagingProjectID, insecure: true},
		{name: "custom", project: "custom-project", insecure: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &SyncedCluster{
				Cluster: cloudcluster.Cluster{
					Name: "test-cluster",
					VMs: vm.List{{
						Provider: gce.ProviderName,
						Project:  tc.project,
					}},
				},
				ClusterSettings: ClusterSettings{Secure: true},
			}

			err := (ComplexSecureOption{DefaultSecure: true}).overrideBasedOnClusterSettings(c)
			require.NoError(t, err)
			require.Equal(t, !tc.insecure, c.Secure)
		})
	}
}
