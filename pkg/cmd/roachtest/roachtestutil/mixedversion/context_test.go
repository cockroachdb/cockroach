// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/stretchr/testify/require"
)

func TestServiceContext_startUpgrade(t *testing.T) {
	initialVersion := clusterupgrade.MustParseVersion("v22.2.10")
	upgradeVersion := clusterupgrade.MustParseVersion("v23.1.2")

	sc := newInitialContext(initialVersion, option.NodeListOption{1, 2, 3}, nil).System
	sc.startUpgrade(upgradeVersion)

	require.Equal(t, &ServiceContext{
		Descriptor:  &ServiceDescriptor{Name: install.SystemInterfaceName, Nodes: option.NodeListOption{1, 2, 3}},
		FromVersion: initialVersion,
		ToVersion:   upgradeVersion,
		nodesByVersion: map[clusterupgrade.Version]*intsets.Fast{
			*initialVersion: intSetP(1, 2, 3),
		},
	}, sc)
}

// TestServiceContextOperations runs a series of operations on a
// service context and checks that functions called on that context
// (that could be called in user-provided hooks) behave as expected.
func TestContextOperations(t *testing.T) {
	initialVersion := clusterupgrade.MustParseVersion("v22.2.10")
	upgradeVersion := clusterupgrade.MustParseVersion("v23.1.2")

	sc := newInitialContext(initialVersion, option.NodeListOption{1, 2, 3}, nil).System
	sc.startUpgrade(upgradeVersion)

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
			require.NoError(t, sc.changeVersion(op.changeVersionNode, op.changeVersion))
			require.Equal(t, op.expectedNodesInPreviousVersion, sc.NodesInPreviousVersion())
			require.Equal(t, op.expectedNodesInNextVersion, sc.NodesInNextVersion())
			require.Equal(t, op.expectedMixedBinary, sc.MixedBinary())

			nodeV, err := sc.NodeVersion(op.changeVersionNode)
			require.NoError(t, err)
			require.Equal(t, op.changeVersion, nodeV)
		})
	}
}
