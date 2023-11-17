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
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/stretchr/testify/require"
)

func TestContext_startUpgrade(t *testing.T) {
	initialVersion := clusterupgrade.MustParseVersion("v22.2.10")
	upgradeVersion := clusterupgrade.MustParseVersion("v23.1.2")

	c := newInitialContext(initialVersion, option.NodeListOption{1, 2, 3})
	c.startUpgrade(upgradeVersion)

	require.Equal(t, &Context{
		CockroachNodes: option.NodeListOption{1, 2, 3},
		FromVersion:    initialVersion,
		ToVersion:      upgradeVersion,
		nodesByVersion: map[clusterupgrade.Version]*intsets.Fast{
			*initialVersion: intSetP(1, 2, 3),
		},
	}, c)
}

// TestContextOperations runs a series of operations on a context and
// checks that functions called on that context (that could be called
// in user-provided hooks) behave as expected.
func TestContextOperations(t *testing.T) {
	initialVersion := clusterupgrade.MustParseVersion("v22.2.10")
	upgradeVersion := clusterupgrade.MustParseVersion("v23.1.2")

	c := newInitialContext(initialVersion, option.NodeListOption{1, 2, 3})
	c.startUpgrade(upgradeVersion)

	ops := []struct {
		name                           string
		changeVersionNode              int
		changeVersion                  *clusterupgrade.Version
		expectedNodesInPreviousVersion option.NodeListOption
		expectedNodesInNextVersion     option.NodeListOption
		expectedMixedBinary            bool
	}{
		{
			name:                           "upgrade n1",
			changeVersionNode:              1,
			changeVersion:                  upgradeVersion,
			expectedNodesInPreviousVersion: option.NodeListOption{2, 3},
			expectedNodesInNextVersion:     option.NodeListOption{1},
			expectedMixedBinary:            true,
		},
		{
			name:                           "upgrade n3",
			changeVersionNode:              3,
			changeVersion:                  upgradeVersion,
			expectedNodesInPreviousVersion: option.NodeListOption{2},
			expectedNodesInNextVersion:     option.NodeListOption{1, 3},
			expectedMixedBinary:            true,
		},
		{
			name:                           "upgrade n2",
			changeVersionNode:              2,
			changeVersion:                  upgradeVersion,
			expectedNodesInPreviousVersion: nil,
			expectedNodesInNextVersion:     option.NodeListOption{1, 2, 3},
			expectedMixedBinary:            false,
		},
		{
			name:                           "downgrade n3",
			changeVersionNode:              3,
			changeVersion:                  initialVersion,
			expectedNodesInPreviousVersion: option.NodeListOption{3},
			expectedNodesInNextVersion:     option.NodeListOption{1, 2},
			expectedMixedBinary:            true,
		},
		{
			name:                           "downgrade n1",
			changeVersionNode:              1,
			changeVersion:                  initialVersion,
			expectedNodesInPreviousVersion: option.NodeListOption{1, 3},
			expectedNodesInNextVersion:     option.NodeListOption{2},
			expectedMixedBinary:            true,
		},
		{
			name:                           "downgrade n2",
			changeVersionNode:              2,
			changeVersion:                  initialVersion,
			expectedNodesInPreviousVersion: option.NodeListOption{1, 2, 3},
			expectedNodesInNextVersion:     nil,
			expectedMixedBinary:            false,
		},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			c.changeVersion(op.changeVersionNode, op.changeVersion)
			require.Equal(t, op.expectedNodesInPreviousVersion, c.NodesInPreviousVersion())
			require.Equal(t, op.expectedNodesInNextVersion, c.NodesInNextVersion())
			require.Equal(t, op.expectedMixedBinary, c.MixedBinary())
			require.Equal(t, op.changeVersion, c.NodeVersion(op.changeVersionNode))
		})
	}
}
