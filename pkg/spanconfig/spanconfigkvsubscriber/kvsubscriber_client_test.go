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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestBlockedKVSubscriberDisablesMerges ensures that the merge queue is
// disabled until the KVSubscriber has some snapshot.
func TestBlockedKVSubscriberDisablesMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	blockSubscriberCh := make(chan struct{})
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
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
		trace, processErr, err := store.Enqueue(
			ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		require.NoError(t, testutils.MatchInOrder(trace.String(), `skipping merge: queue has been disabled`))
	}

	close(blockSubscriberCh)
	testutils.SucceedsSoon(t, func() error {
		if scKVSubscriber.LastUpdated().IsEmpty() {
			return errors.New("expected non-empty update ts")
		}
		return nil
	})

	{
		trace, processErr, err := store.Enqueue(
			ctx, "merge", repl, true /* skipShouldQueue */, false, /* async */
		)
		require.NoError(t, err)
		require.NoError(t, processErr)
		require.Error(t, testutils.MatchInOrder(trace.String(), `skipping merge: queue has been disabled`))
	}
}
