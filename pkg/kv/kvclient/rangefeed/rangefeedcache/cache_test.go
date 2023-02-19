// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedcache_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestCache is a basic test of the Cache.
func TestCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	scratch, err := s.ScratchRange()
	require.NoError(t, err)
	scratchSpan := roachpb.Span{
		Key:    scratch,
		EndKey: scratch.PrefixEnd(),
	}
	mkKey := func(suffix string) roachpb.Key {
		return encoding.EncodeStringAscending(
			scratch[:len(scratch):len(scratch)],
			suffix)
	}

	c := rangefeedcache.NewCache(
		"test",
		kvDB.Clock(),
		s.RangeFeedFactory().(*rangefeed.Factory),
		[]roachpb.Span{scratchSpan},
	)
	require.NoError(t, c.Start(ctx, s.Stopper()))
	readRowsAt := func(t *testing.T, ts hlc.Timestamp) []roachpb.KeyValue {
		txn := kvDB.NewTxn(ctx, "test")
		require.NoError(t, txn.SetFixedTimestamp(ctx, ts))
		ba := &kvpb.BatchRequest{}
		ba.Add(&kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    scratch,
				EndKey: scratchSpan.EndKey,
			},
			ScanFormat: kvpb.KEY_VALUES,
		})
		br, pErr := txn.Send(ctx, ba)
		require.NoError(t, pErr.GoError())
		return br.Responses[0].GetScan().Rows
	}
	writeAndCheck := func(t *testing.T, f func(t *testing.T, txn *kv.Txn)) {
		var copied *kv.Txn
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			copied = txn
			f(t, txn)
			return nil
		}))
		testutils.SucceedsSoon(t, func() error {
			_, ts, ok := c.GetSnapshot()
			if !ok || ts.Less(copied.CommitTimestamp()) {
				return errors.Errorf("cache not yet up to date")
			}
			return nil
		})
		resp := readRowsAt(t, s.Clock().Now())
		got, _, _ := c.GetSnapshot()
		require.Equalf(t, resp, got, "%v", copied.CommitTimestamp())
	}

	// Initialize an empty cache.
	// TODO(ajwerner): We should not need to do this, this indicates
	// that there's something going wrong with the timestamp on the catchup
	// scan.
	writeAndCheck(t, func(t *testing.T, txn *kv.Txn) {
		require.NoError(t, txn.Put(ctx, mkKey("a"), 1))
		require.NoError(t, txn.Put(ctx, mkKey("b"), 1))
		require.NoError(t, txn.Put(ctx, mkKey("c"), 1))
		require.NoError(t, txn.Put(ctx, mkKey("d"), 1))
	})
	writeAndCheck(t, func(t *testing.T, txn *kv.Txn) {
		_, err := txn.Del(ctx, mkKey("a"))
		require.NoError(t, err)
	})
	writeAndCheck(t, func(t *testing.T, txn *kv.Txn) {
		_, err := txn.DelRange(ctx, mkKey("a"), mkKey("c"), false)
		require.NoError(t, err)
	})
}
