// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
