// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVirtualClusterLabel(t *testing.T) {
	testCases := []struct {
		name               string
		virtualClusterName string
		sqlInstance        int
		expectedLabel      string
	}{
		{
			name:               "empty tenant name",
			virtualClusterName: "",
			expectedLabel:      "cockroach-system",
		},
		{
			name:               "system tenant name",
			virtualClusterName: "system",
			expectedLabel:      "cockroach-system",
		},
		{
			name:               "simple app tenant name",
			virtualClusterName: "a",
			sqlInstance:        1,
			expectedLabel:      "cockroach-a_1",
		},
		{
			name:               "tenant name with hyphens",
			virtualClusterName: "tenant-a-1",
			sqlInstance:        1,
			expectedLabel:      "cockroach-tenant-a-1_1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			label := VirtualClusterLabel(tc.virtualClusterName, tc.sqlInstance)
			require.Equal(t, tc.expectedLabel, label)

			nameFromLabel, instanceFromLabel, err := VirtualClusterInfoFromLabel(label)
			require.NoError(t, err)

			expectedTenantName := tc.virtualClusterName
			if tc.virtualClusterName == "" {
				expectedTenantName = "system"
			}
			require.Equal(t, expectedTenantName, nameFromLabel)

			require.Equal(t, tc.sqlInstance, instanceFromLabel)
		})
	}
}
