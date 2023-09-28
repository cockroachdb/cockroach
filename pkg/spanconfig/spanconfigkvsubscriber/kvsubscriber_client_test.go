// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBlockedKVSubscriberDisablesQueues ensures that the queue that need span
// configs are disabled until the KVSubscriber has some snapshot. It
// additionally verifies that the queues don't attempt to load span configs more
// than once per run.
func TestBlockedKVSubscriberDisablesQueues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	for _, queueName := range []string{
		"merge",
		"mvccGC",
		"replicate",
		"split",
	} {
		t.Run(fmt.Sprintf("queue=%s", queueName), func(t *testing.T) {
			// Once this is closed, allow subscriber updates.
			allowSubscriberCh := make(chan struct{})
			// Once this is closed, startup is complete, so error any future span lookups.
			startupCompleteCh := make(chan struct{})

			nodes, replicationMode := 1, base.ReplicationAuto
			if queueName == "replicate" {
				// To enqueue replicas into the replicate queue without erroring
				// out, we need enough nodes, lest we run into:
				//
				//   ERR: live stores are able to take a new replica for the
				//   range (1 already has a voter, 0 already have a non-voter);
				//   likely not enough nodes in cluster
				nodes = 3
				// This test ends up disabling the replicate queue by blocking
				// span config subscription. Use manual replication to bypass
				// any automatic WaitForFullReplication by the testcluster
				// harness which calls into the replicate queue.
				replicationMode = base.ReplicationManual
			}
			expectedErr := errors.New("span configs blocked")
			tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{
				ReplicationMode: replicationMode,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{ConfReaderInterceptor: func() error {
							select {
							case <-allowSubscriberCh:
								return nil
							default:
							}
							select {
							case <-startupCompleteCh:
								return expectedErr
							default:
							}
							return nil
						},
						},
					},
				},
			})

			defer tc.Stopper().Stop(ctx)

			store := tc.GetFirstStoreFromServer(t, 0)
			scratchKey := tc.ScratchRange(t)
			tc.SplitRangeOrFatal(t, scratchKey.Next())

			var repl *kvserver.Replica
			testutils.SucceedsSoon(t, func() error {
				repl = store.LookupReplica(roachpb.RKey(scratchKey))
				if repl == nil {
					return errors.New(`could not find replica`)
				}
				return nil
			})

			close(startupCompleteCh)
			{
				_, processErr, err := store.Enqueue(
					ctx, queueName, repl, true /* skipShouldQueue */, false, /* async */
				)
				require.NoError(t, processErr)
				require.Error(t, err)
				require.True(t, errors.Is(err, expectedErr))
			}

			close(allowSubscriberCh)
			{
				trace, processErr, err := store.Enqueue(
					ctx, queueName, repl, true /* skipShouldQueue */, false, /* async */
				)
				require.NoError(t, err)
				require.NoError(t, processErr)
				require.Error(t, testutils.MatchInOrder(trace.String(), `unable to retrieve conf reader`))
				require.Error(t, testutils.MatchInOrder(trace.String(), `queue has been disabled`))
			}
		})
	}
}
