// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIsTypeSupportedInVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		v clusterversion.Key
		t *T

		ok bool
	}{
		{clusterversion.TODOPreV21_2, RegRole, true},
		{clusterversion.TODOPreV21_2, MakeArray(RegRole), true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s:%s", tc.v, tc.t.SQLString()), func(t *testing.T) {
			ok := IsTypeSupportedInVersion(
				clusterversion.ClusterVersion{Version: clusterversion.ByKey(tc.v)},
				tc.t,
			)
			require.Equal(t, tc.ok, ok)
		})
	}
}
