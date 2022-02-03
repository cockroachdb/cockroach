// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// TestEnsureEngineVersion_SingleNode verifies that the migration of a single
// node waits for the	node's engine version to be at least at a minimum
// version, blocking until it occurs.
func TestEnsureEngineVersion_SingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						// Start at early version of the binary.
						clusterversion.Start22_1,
					),
				},
				Store: &kvserver.StoreTestingKnobs{
					StorageKnobs: storage.TestingKnobs{
						// Start at early engine version.
						FormatMajorVersion: pebble.FormatMostCompatible,
					},
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)
	ts := tc.Server(0)

	getFormatVersion := func() (pebble.FormatMajorVersion, error) {
		e := ts.Engines()[0]
		// Wait for the current version to stabilize.
		currentVers := ts.ClusterSettings().Version.ActiveVersion(ctx)
		if err := e.WaitForCompatibleEngineVersion(ctx, currentVers.Version); err != nil {
			return 0, err
		}
		return e.FormatMajorVersion(), nil
	}

	// We start at Pebble major format version 4 (SetWithDelete). Note that the
	// server was started with an earlier engine version (v0, "most compatible"),
	// on start it will ratchet up to a compatible version.
	v, err := getFormatVersion()
	require.NoError(t, err)
	require.Equal(t, v, pebble.FormatSetWithDelete)

	// Bump the cluster version to include block properties.
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.PebbleFormatVersionBlockProperties).String(),
	)

	// The store version has been ratcheted to version 5 (Block property
	// collectors).
	v, err = getFormatVersion()
	require.NoError(t, err)
	require.Equal(t, v, pebble.FormatBlockPropertyCollector)
}

// TestEnsureEngineVersion_MultiNode is the same as the above, except that it
// runs on a cluster of three nodes.
func TestEnsureEngineVersion_MultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const nNodes = 3
	tc := testcluster.StartTestCluster(t, nNodes, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: 1,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.Start22_1,
					),
				},
				Store: &kvserver.StoreTestingKnobs{
					StorageKnobs: storage.TestingKnobs{
						FormatMajorVersion: pebble.FormatMostCompatible,
					},
				},
			},
		},
	})

	defer tc.Stopper().Stop(ctx)

	getFormatVersions := func(tc *testcluster.TestCluster) (
		[]pebble.FormatMajorVersion, error,
	) {
		var versions [nNodes]pebble.FormatMajorVersion
		for i := 0; i < nNodes; i++ {
			ts := tc.Server(i)
			e := ts.Engines()[0]
			// Wait for the current version to stabilize.
			currentVers := ts.ClusterSettings().Version.ActiveVersion(ctx)
			if err := e.WaitForCompatibleEngineVersion(ctx, currentVers.Version); err != nil {
				return nil, err
			}
			versions[i] = e.FormatMajorVersion()
		}
		return versions[:], nil
	}

	all := func(v pebble.FormatMajorVersion) []pebble.FormatMajorVersion {
		var vs [nNodes]pebble.FormatMajorVersion
		for i := 0; i < nNodes; i++ {
			vs[i] = v
		}
		return vs[:]
	}

	// All nodes start at Pebble major format version 4 (SetWithDelete).
	vs, err := getFormatVersions(tc)
	require.NoError(t, err)
	require.Equal(t, all(pebble.FormatSetWithDelete), vs)

	// Bump the cluster version to include block properties.
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.ExecSucceedsSoon(t,
		"SET CLUSTER SETTING version = $1",
		clusterversion.ByKey(clusterversion.PebbleFormatVersionBlockProperties).String(),
	)

	// The store versions have been ratcheted to version 5 (Block property
	// collectors).
	vs, err = getFormatVersions(tc)
	require.NoError(t, err)
	require.Equal(t, all(pebble.FormatBlockPropertyCollector), vs)
}
