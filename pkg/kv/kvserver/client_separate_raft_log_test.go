// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSeparateRaftLog is a very basic test for the separate raft log.
// It sets up a three node cluster with two stores each (each store with
// a separate raft log), and waits for rebalancing. This requires both
// raft handling, distributed txns, and snapshot application, so it's
// a reasonably good sanity check.
func TestSeparateRaftLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	skip.IgnoreLint(t, "TODO(this PR): unskip in last commit")

	// Six stores, each of which with a separate log engine, makes twelve pebble
	// instances - likely too heavyweight for stressing.
	skip.UnderStress(t, "not a useful test to stress")

	const numNodes = 3

	testutils.RunTrueAndFalse(t, "mem", func(t *testing.T, inMem bool) {
		if !inMem {
			// The test is very slow with real disks, at least on OSX.
			// Six disks + rebalancing = lots of fsyncs. But the code
			// in CreateEngines for the disk path is very different
			// so it makes sense to be able to run it manually at least.
			skip.IgnoreLint(t, "only for manual testing")
		}
		var args base.TestClusterArgs
		// Two in-mem stores per node, with sep raft log enabled.
		args.ServerArgsPerNode = map[int]base.TestServerArgs{}
		for i := 0; i < numNodes; i++ {
			var serverArgs base.TestServerArgs
			for j := 0; j < 2; j++ {
				spec := base.DefaultTestStoreSpec
				spec.InMemory = inMem
				spec.ExperimentalSeparateRaftLogPath = "mem"
				if !inMem {
					spec.Path = t.TempDir()
					spec.ExperimentalSeparateRaftLogPath = t.TempDir()
				}
				serverArgs.StoreSpecs = append(serverArgs.StoreSpecs, spec)
			}
			args.ServerArgsPerNode[i] = serverArgs
		}
		tc := testcluster.StartTestCluster(t, numNodes, args)
		defer tc.Stopper().Stop(ctx)

		for _, srv := range tc.Servers {
			require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
				require.True(t, s.StateEngine() != s.LogEngine())
				return nil
			}))
		}
	})
}
