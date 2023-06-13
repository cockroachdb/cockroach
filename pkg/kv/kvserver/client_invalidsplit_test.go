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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var badKey = append([]byte{'\xfe'}, '\xfd')

func TestInvalidSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	_, _, err := keys.DecodeTenantPrefix(badKey)
	t.Log(err)
	require.NoError(t, err) // used to error before changing DecodeTenantPrefix

	ctx := context.Background()
	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	start := func(t *testing.T) *testcluster.TestCluster {
		serverArgs := base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{InMemory: true, StickyInMemoryEngineID: "1"},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		}

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: serverArgs,
		})
		return tc
	}

	func() {
		tc := start(t)
		defer tc.Stopper().Stop(ctx)

		_, _, err = tc.SplitRange(badKey)
		t.Log(err)
	}()

	func() {
		tc := start(t)
		defer tc.Stopper().Stop(ctx)
		mergeKey := roachpb.Key(badKey).Prevish(5)
		desc := tc.LookupRangeOrFatal(t, mergeKey)
		require.EqualValues(t, badKey, desc.EndKey)
		_, err = tc.Servers[0].MergeRanges(mergeKey)
		require.NoError(t, err)
	}()
}
