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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIsSpanEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var sentIsSpanEmptyRequests int64
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
						if _, exists := request.GetArg(kvpb.IsSpanEmpty); exists {
							atomic.AddInt64(&sentIsSpanEmptyRequests, 1)
						}
						return nil
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	kvDB := tc.Server(0).DB()
	scratchKey := tc.ScratchRange(t)
	mkKey := func(suffix string) roachpb.Key {
		return append(scratchKey[:len(scratchKey):len(scratchKey)], suffix...)
	}

	checkIsEmpty := func(t *testing.T, exp bool, from, to roachpb.Key) {
		var ba kv.Batch
		ba.Header.MaxSpanRequestKeys = 1
		ba.AddRawRequest(&kvpb.IsSpanEmptyRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: from, EndKey: to,
			},
		})
		require.NoError(t, kvDB.Run(ctx, &ba))
		require.Equal(t, exp, ba.RawResponse().Responses[0].GetIsSpanEmpty().IsEmpty())
	}

	requireEmpty := func(t *testing.T, from, to roachpb.Key) {
		checkIsEmpty(t, true, from, to)
	}
	requireNotEmpty := func(t *testing.T, from, to roachpb.Key) {
		checkIsEmpty(t, false, from, to)
	}

	requireEmpty(t, mkKey(""), mkKey("").PrefixEnd())

	tc.SplitRangeOrFatal(t, mkKey("c"))
	requireEmpty(t, mkKey(""), mkKey("").PrefixEnd())

	require.NoError(t, kvDB.Put(ctx, mkKey("x"), "foo"))
	requireEmpty(t, mkKey(""), mkKey("x"))
	requireNotEmpty(t, mkKey(""), mkKey("").PrefixEnd())

	_, err := kvDB.Del(ctx, mkKey("x"))
	require.NoError(t, err)
	requireEmpty(t, mkKey(""), mkKey("x"))
	requireNotEmpty(t, mkKey(""), mkKey("").PrefixEnd())

	// We want to make sure that the DistSender stops iterating ranges once
	// the first range with any keys is found.
	checkIsCalled := func(t *testing.T, expEmpty bool, delta int64, from, to roachpb.Key) {
		before := atomic.LoadInt64(&sentIsSpanEmptyRequests)
		checkIsEmpty(t, expEmpty, from, to)
		require.Equal(t, delta, atomic.LoadInt64(&sentIsSpanEmptyRequests)-before)
	}
	checkIsCalled(t, false, 2, mkKey(""), mkKey("").PrefixEnd())
	tc.SplitRangeOrFatal(t, mkKey("a"))
	tc.SplitRangeOrFatal(t, mkKey("b"))
	checkIsCalled(t, false, 4, mkKey(""), mkKey("").PrefixEnd())
}
