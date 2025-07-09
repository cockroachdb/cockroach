// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDeleteRangeTombstoneSetsGCHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Disable tenant testing here to simplify the creation of the scratch range
		// below.
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()
	key, err := srv.ScratchRange()
	require.NoError(t, err, "failed to create scratch range")

	store, err := srv.StorageLayer().GetStores().(*kvserver.Stores).GetStore(srv.StorageLayer().GetFirstStoreID())
	require.NoError(t, err)
	repl := store.LookupReplica(roachpb.RKey(key))

	gcHint := repl.GetGCHint()
	require.True(t, gcHint.LatestRangeDeleteTimestamp.IsEmpty(), "gc hint should be empty by default")

	content := []byte("test")
	pArgs := &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(content),
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSenderI().(kv.Sender), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	r, err := srv.LookupRange(key)
	require.NoError(t, err, "failed to lookup range")

	drArgs := &kvpb.DeleteRangeRequest{
		UpdateRangeDeleteGCHint: true,
		UseRangeTombstone:       true,
		RequestHeader: kvpb.RequestHeader{
			Key:    r.StartKey.AsRawKey(),
			EndKey: r.EndKey.AsRawKey(),
		},
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSenderI().(kv.Sender), drArgs); pErr != nil {
		t.Fatal(pErr)
	}

	gcHint = repl.GetGCHint()
	require.True(t, !gcHint.LatestRangeDeleteTimestamp.IsEmpty(), "gc hint was not set by delete range")
}
