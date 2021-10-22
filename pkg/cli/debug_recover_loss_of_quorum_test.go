// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

/*
Tests in this file try to verify high level basic workflow for loss of
quorum recovery. Only basic sanity checks for handling of flags and
passing data to actual recovery functions are done.
*/

// TestCollectInfo performs basic sanity checks on replica info collection.
// This is done by running three node cluster with disk backed storage,
// stopping it and verifying content of collected replica info file.
// This check verifies that:
//   we successfully iterate requested stores,
//   data is written in expected location,
//   data contains info only about stores requested.
func TestCollectInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
			1: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-2"}}},
			2: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-3"}}},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	// Wait up-replication.
	require.NoError(t, tc.WaitForFullReplication())
	// Shutdown.
	tc.Stopper().Stop(ctx)

	replicaInfoFileName := dir + "/node-1.json"

	c.RunWithArgs([]string{"debug", "recover", "collect-info", "--store=" + dir + "/store-1", "--store=" + dir + "/store-2", replicaInfoFileName})

	replicas, err := readReplicaInfoData([]string{replicaInfoFileName})
	require.NoError(t, err, "failed to read generated replica info")
	stores := map[roachpb.StoreID]interface{}{}
	for _, r := range replicas[0].Replicas {
		stores[r.StoreID] = struct{}{}
	}
	require.Equal(t, 2, len(stores), "collected replicas from stores")
}

// TestGeneratePlan performs sanity check that plan could be generated
// from collected replica info.
// This is done by running a three node cluster, collecting info form one
// of nodes and creating a plan in non-interactive mode.
func TestGeneratePlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
			1: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-2"}}},
			2: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-3"}}},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	// Wait up-replication.
	require.NoError(t, tc.WaitForFullReplication())
	node1ID := tc.Servers[0].NodeID()
	// Shutdown.
	tc.Stopper().Stop(ctx)

	replicaInfoFileName := dir + "/node-1.json"

	c.RunWithArgs([]string{"debug", "recover", "collect-info", "--store=" + dir + "/store-1", replicaInfoFileName})

	planFile := dir + "/recovery-plan.json"
	out, err := c.RunWithCaptureArgs([]string{"debug", "recover", "make-plan", "--confirm=y", "--plan=" + planFile, replicaInfoFileName})
	require.NoError(t, err, "failed to run make-plan")
	require.FileExists(t, planFile, "generated plan file")
	require.Contains(t, out, fmt.Sprintf("Run update on node n%d", node1ID), "planner didn't provide correct apply instructions")
}

// TestApplyPlan performs a sanity check on apply-plan recovery command
// invocation.
// This is done by first collecting and generating a plan off single store
// out of three and checking that application of command produces output
// that is consistent with plan application execution.
func TestApplyPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		NoServer: true,
	})
	defer c.Cleanup()

	tc := testcluster.NewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-1"}}},
			1: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-2"}}},
			2: {StoreSpecs: []base.StoreSpec{{Path: dir + "/store-3"}}},
		},
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	// Wait up-replication.
	require.NoError(t, tc.WaitForFullReplication())
	node1ID := tc.Servers[0].NodeID()
	// Shutdown.
	tc.Stopper().Stop(ctx)

	server1StoreDir := dir + "/store-1"
	replicaInfoFileName := dir + "/node-1.json"
	c.RunWithArgs([]string{"debug", "recover", "collect-info", "--store=" + server1StoreDir, replicaInfoFileName})
	planFile := dir + "/recovery-plan.json"
	c.RunWithArgs([]string{"debug", "recover", "make-plan", "--confirm=y", "--plan=" + planFile, replicaInfoFileName})

	out, err := c.RunWithCaptureArgs([]string{"debug", "recover", "apply-plan", "--confirm=y", "--store=" + server1StoreDir, planFile})
	require.NoError(t, err, "failed to run apply plan")
	// Check that there were at least one mention of replica being promoted.
	require.Contains(t, out, "will be promoted", "no replica updated were recorded")
	require.Contains(t, out, fmt.Sprintf("Updated store(s): s%d", node1ID), "apply plan was not executed on requested node")
}
