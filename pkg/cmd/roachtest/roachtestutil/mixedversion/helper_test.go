// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mixedversion

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func TestLowestBinaryVersion(t *testing.T) {
	currentVersion := version.MustParse("v48.2.3")

	testCases := []struct {
		name        string
		testContext *Context
		crdbNodes   option.NodeListOption
		expected    *version.Version
	}{
		{
			name: "all nodes in the 'from' version, predecessor", // before an upgrade or downgrade
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2, 3},
				FromVersion:      "22.2.8",
				ToVersionNodes:   option.NodeListOption{},
				ToVersion:        clusterupgrade.MainVersion,
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.8"),
		},
		{
			name: "all nodes in the 'from' version, current", // before an upgrade or downgrade
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2, 3},
				FromVersion:      clusterupgrade.MainVersion,
				ToVersionNodes:   option.NodeListOption{},
				ToVersion:        "22.2.8",
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  currentVersion,
		},
		{
			name: "all nodes in the 'to' version, predecessor", // after an upgrade or downgrade
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{},
				FromVersion:      clusterupgrade.MainVersion,
				ToVersionNodes:   option.NodeListOption{1, 2, 3},
				ToVersion:        "22.2.8",
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.8"),
		},
		{
			name: "all nodes in the 'to' version, current", // after an upgrade or downgrade
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{},
				FromVersion:      "22.2.8",
				ToVersionNodes:   option.NodeListOption{1, 2, 3},
				ToVersion:        clusterupgrade.MainVersion,
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  currentVersion,
		},
		{
			name: "mixed-binary state, 'from' version is lowest, both non-current",
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2},
				FromVersion:      "22.2.8",
				ToVersionNodes:   option.NodeListOption{3},
				ToVersion:        "23.1.8",
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.8"),
		},
		{
			name: "mixed-binary state, 'from' version is lowest, 'to' is current",
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2},
				FromVersion:      "22.2.8",
				ToVersionNodes:   option.NodeListOption{3},
				ToVersion:        clusterupgrade.MainVersion,
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.8"),
		},
		{
			name: "mixed-binary state, 'to' version is lowest, both non-current",
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2},
				FromVersion:      "23.1.8",
				ToVersionNodes:   option.NodeListOption{3},
				ToVersion:        "22.2.0",
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.0"),
		},
		{
			name: "mixed-binary state, 'to' version is lowest, 'from' is current",
			testContext: &Context{
				FromVersionNodes: option.NodeListOption{1, 2},
				FromVersion:      clusterupgrade.MainVersion,
				ToVersionNodes:   option.NodeListOption{3},
				ToVersion:        "22.2.0",
			},
			crdbNodes: option.NodeListOption{1, 2, 3},
			expected:  version.MustParse("v22.2.0"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runner := testTestRunner()
			runner.crdbNodes = tc.crdbNodes
			runner.currentVersion = currentVersion

			h := runner.newHelper(ctx, nilLogger)
			h.testContext = tc.testContext
			require.Equal(t, tc.expected, h.LowestBinaryVersion())
		})
	}
}
