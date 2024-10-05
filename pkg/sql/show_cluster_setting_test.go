// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
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
			local: encode(t, "22.2-10"),
			kv:    encode(t, "22.2-10"),
		},
		{ // 1
			local: encode(t, "22.2-12"),
			kv:    encode(t, "22.2-11"),
			exp:   "value differs between local setting (22.2-12) and KV (22.2-11)",
		},
		{ // 2
			local: encode(t, "22.2-11"),
			kv:    encode(t, "22.2-10"),
		},
		{ // 3
			local: encode(t, "22.2-11"),
			kv:    []byte("abc"),
			exp:   "value differs between local setting (22.2-11) and KV ([97 98 99])",
		},
		{ // 4
			kv:  encode(t, "22.2-11"),
			exp: "value differs between local setting ([]) and KV (22.2-11)",
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
