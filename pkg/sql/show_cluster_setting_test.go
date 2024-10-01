// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestCheckClusterSettingValuesAreEquivalent exercises the logic used to
// decide whether `SHOW CLUSTER SETTING version` can return the value it read.
func TestCheckClusterSettingValuesAreEquivalent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	encode := func(t *testing.T, s string) []byte {
		v, err := roachpb.ParseVersion(s)
		require.NoError(t, err)
		cv := clusterversion.ClusterVersion{Version: v}
		data, err := protoutil.Marshal(&cv)
		require.NoError(t, err)
		return data
	}
	for _, tc := range []struct {
		local []byte
		kv    []byte
		exp   string
	}{
		{ // 0
			local: encode(t, "22.2-upgrading-to-23.1-step-010"),
			kv:    encode(t, "22.2-upgrading-to-23.1-step-010"),
		},
		{ // 1
			local: encode(t, "22.2-upgrading-to-23.1-step-012"),
			kv:    encode(t, "22.2-upgrading-to-23.1-step-011"),
			exp:   "value differs between local setting (22.2-upgrading-to-23.1-step-012) and KV (22.2-upgrading-to-23.1-step-011)",
		},
		{ // 2
			local: encode(t, "22.2-upgrading-to-23.1-step-011"),
			kv:    encode(t, "22.2-upgrading-to-23.1-step-010"),
		},
		{ // 3
			local: encode(t, "22.2-upgrading-to-23.1-step-011"),
			kv:    []byte("abc"),
			exp:   "value differs between local setting (22.2-upgrading-to-23.1-step-011) and KV ([97 98 99])",
		},
		{ // 4
			kv:  encode(t, "22.2-upgrading-to-23.1-step-011"),
			exp: "value differs between local setting ([]) and KV (22.2-upgrading-to-23.1-step-011)",
		},
		{ // 5
			// NB: On release branches, clusterversion.Latest will have a fence
			// version that has -1 for the internal version.
			local: encode(t, clusterversion.Latest.Version().FenceVersion().String()),
			kv:    encode(t, (clusterversion.Latest - 1).Version().String()),
		},
		{ // 6
			local: encode(t, clusterversion.Latest.Version().String()),
			kv:    encode(t, (clusterversion.Latest - 1).Version().String()),
			exp: fmt.Sprintf(
				"value differs between local setting (%s) and KV (%s)",
				clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
				clusterversion.ClusterVersion{Version: (clusterversion.Latest - 1).Version()},
			),
		},
	} {
		t.Run("", func(t *testing.T) {
			err := checkClusterSettingValuesAreEquivalent(tc.local, tc.kv)
			if tc.exp == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.exp)
			}
		})
	}
}
