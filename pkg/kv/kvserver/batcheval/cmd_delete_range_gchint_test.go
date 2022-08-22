// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestDeleteRangeTombstoneSetsGCHint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key := roachpb.Key("b")
	content := []byte("test")

	repl := store.LookupReplica(roachpb.RKey(key))
	gcHint := repl.GetGCHint()
	require.True(t, gcHint.LatestRangeDeleteTimestamp.IsEmpty(), "gc hint should be empty by default")

	pArgs := &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(content),
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSender(), pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	r, err := s.LookupRange(key)
	require.NoError(t, err, "failed to lookup range")

	drArgs := &roachpb.DeleteRangeRequest{
		UpdateRangeDeleteGCHint: true,
		UseRangeTombstone:       true,
		RequestHeader: roachpb.RequestHeader{
			Key:    r.StartKey.AsRawKey(),
			EndKey: r.EndKey.AsRawKey(),
		},
	}
	if _, pErr := kv.SendWrapped(ctx, s.DistSender(), drArgs); pErr != nil {
		t.Fatal(pErr)
	}

	gcHint = repl.GetGCHint()
	require.True(t, !gcHint.LatestRangeDeleteTimestamp.IsEmpty(), "gc hint was not set by delete range")
}
