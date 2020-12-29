// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestDBClientScan tests that the logic in Scan on the dbAdapter is sane.
// The rangefeed logic is a literal passthrough so it's not getting a lot of
// testing directly.
func TestDBClientScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	beforeAny := db.Clock().Now()
	scratchKey := tc.ScratchRange(t)
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}
	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	require.NoError(t, db.Put(ctx, mkKey("b"), 2))
	afterB := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("c"), 3))

	dba, err := rangefeed.NewDBAdapter(db)
	require.NoError(t, err)
	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}

	// Ensure that the timestamps are properly respected by not observing any
	// values at the timestamp preceding writes.
	{
		var responses []roachpb.KeyValue
		require.NoError(t, dba.Scan(ctx, sp, beforeAny, func(value roachpb.KeyValue) {
			responses = append(responses, value)
		}))
		require.Len(t, responses, 0)
	}

	// Ensure that expected values are seen at the intermediate timestamp.
	{
		var responses []roachpb.KeyValue
		require.NoError(t, dba.Scan(ctx, sp, afterB, func(value roachpb.KeyValue) {
			responses = append(responses, value)
		}))
		require.Len(t, responses, 2)
		require.Equal(t, mkKey("a"), responses[0].Key)
		va, err := responses[0].Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), va)
	}

	// Ensure that pagination doesn't break anything.
	dba.SetTargetScanBytes(1)
	{
		var responses []roachpb.KeyValue
		require.NoError(t, dba.Scan(ctx, sp, db.Clock().Now(), func(value roachpb.KeyValue) {
			responses = append(responses, value)
		}))
		require.Len(t, responses, 3)
	}

}
