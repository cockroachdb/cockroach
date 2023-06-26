// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStartupInjectedFailureSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const failProb = 0.1

	ctx := context.Background()

	rng, seed := randutil.NewLockedTestRand()
	t.Log("TestStartupInjectedFailure random seed", seed)
	lReg := testutils.NewListenerRegistry()
	defer lReg.Close()
	reg := server.NewStickyInMemEnginesRegistry()
	defer reg.CloseAllStickyInMemEngines()

	var enableFaults atomic.Bool
	args := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: "1",
				},
			},
			Listener: lReg.GetOrFail(t, 0),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: reg,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					// Ensure that scratch range has proper zone config, otherwise it is
					// anybody's guess and if we chose it test can fail.
					ConfigureScratchRange: true,
				},
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(ctx context.Context, br roachpb.BatchRequest,
					) *roachpb.Error {
						if enableFaults.Load() {
							if rng.Float32() < failProb {
								t.Log("injecting fault into range ", br.RangeID)
								return roachpb.NewError(roachpb.NewReplicaUnavailableError(errors.New("injected error"),
									&roachpb.RangeDescriptor{RangeID: br.RangeID}, roachpb.ReplicaDescriptor{
										NodeID:  roachpb.NodeID(1),
										StoreID: roachpb.StoreID(1),
									}))
							}
						}
						return nil
					},
				},
			},
		},
		ReusableListeners: true,
	}
	tc := testcluster.NewTestCluster(t, 1, args)
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)
	tc.StopServer(0)
	enableFaults.Store(true)
	lReg.ReopenOrFail(t, 0)
	require.NoError(t, tc.RestartServer(0), "failed to restart server")

	// Disable faults to make it easier for cluster to stop.
	enableFaults.Store(false)
}
