// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriber_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBlockedKVSubscriberDisablesQueues ensures that the queue that need span
// configs are disabled until the KVSubscriber has some snapshot.
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
			blockSubscriberCh := make(chan struct{})
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
			tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{
				ReplicationMode: replicationMode,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						SpanConfig: &spanconfig.TestingKnobs{
							KVSubscriberRangeFeedKnobs: &rangefeedcache.TestingKnobs{
								PostRangeFeedStart: func() { <-blockSubscriberCh },
							},
						},
					},
				},
			})

			defer tc.Stopper().Stop(ctx)
			ts := tc.Server(0)
			scKVSubscriber := ts.SpanConfigKVSubscriber().(spanconfig.KVSubscriber)
			require.True(t, scKVSubscriber.LastUpdated().IsEmpty())

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

			{
				processErr, err := store.Enqueue(
					ctx, queueName, repl, true /* skipShouldQueue */, false, /* async */
				)
				require.NoError(t, processErr)
				require.Error(t, err)
				require.True(t, testutils.IsError(err, `unable to retrieve conf reader`))
			}

			close(blockSubscriberCh)
			testutils.SucceedsSoon(t, func() error {
				if scKVSubscriber.LastUpdated().IsEmpty() {
					return errors.New("expected non-empty update ts")
				}
				return nil
			})

			{
				traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
				processErr, err := store.Enqueue(
					traceCtx, queueName, repl, true /* skipShouldQueue */, false, /* async */
				)
				require.NoError(t, err)
				require.NoError(t, processErr)
				trace := rec()
				require.Error(t, testutils.MatchInOrder(trace.String(), `unable to retrieve conf reader`))
				require.Error(t, testutils.MatchInOrder(trace.String(), `queue has been disabled`))
			}
		})
	}
}
