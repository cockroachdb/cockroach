// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func makeMockTxnFeedReadTracker() (txnFeedReadTracker, *mockLockedSender) {
	mockSender := &mockLockedSender{}
	st := cluster.MakeTestingClusterSettings()
	return txnFeedReadTracker{
		wrapped: mockSender,
		st:      st,
	}, mockSender
}

func TestTxnFeedReadTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("disabled", func(t *testing.T) {
		// When txnfeed is disabled, no read spans should be attached.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, false)
		txn := makeTxnProto()

		// Perform a read.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Send a committing EndTxn. No read spans should be attached.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			et := ba.Requests[0].GetEndTxn()
			require.Empty(t, et.ReadSpans, "read spans should not be set when txnfeed is disabled")
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			resp.Txn.Status = roachpb.COMMITTED
			return resp, nil
		})

		br, pErr = tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})

	t.Run("enabled", func(t *testing.T) {
		// When txnfeed is enabled, read spans should be tracked and attached.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		txn := makeTxnProto()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, true)

		// Perform two reads.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("c")}})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			return resp, nil
		})

		br, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Send a committing EndTxn. Read spans should be attached.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			et := ba.Requests[0].GetEndTxn()
			require.Len(t, et.ReadSpans, 2)
			require.Equal(t, roachpb.Key("a"), et.ReadSpans[0].Key)
			require.Equal(t, roachpb.Key("c"), et.ReadSpans[1].Key)
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			resp.Txn.Status = roachpb.COMMITTED
			return resp, nil
		})

		br, pErr = tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})

	t.Run("abort", func(t *testing.T) {
		// On abort, read spans should not be attached.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		txn := makeTxnProto()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, true)

		// Perform a read.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Send an aborting EndTxn. No read spans should be attached.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: false})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			et := ba.Requests[0].GetEndTxn()
			require.Empty(t, et.ReadSpans, "read spans should not be set on abort")
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			resp.Txn.Status = roachpb.ABORTED
			return resp, nil
		})

		br, pErr = tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})

	t.Run("epoch-bump-clears", func(t *testing.T) {
		// Epoch bump should clear the read footprint.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		txn := makeTxnProto()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, true)

		// Perform a read.
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")}})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		_, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.False(t, tracker.readFootprint.empty())

		// Bump the epoch.
		tracker.epochBumpedLocked()
		require.True(t, tracker.readFootprint.empty())

		// Commit. No read spans from the prior epoch should be attached.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			et := ba.Requests[0].GetEndTxn()
			require.Empty(t, et.ReadSpans)
			br := ba.CreateReply()
			br.Txn = ba.Txn
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		})

		_, pErr = tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
	})

	t.Run("passthrough", func(t *testing.T) {
		// Non-EndTxn batches without reads should pass through unchanged.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		txn := makeTxnProto()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, true)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a")},
			Value:         roachpb.MakeValueFromString("val"),
		})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
		// Writes don't produce read spans.
		require.True(t, tracker.readFootprint.empty())
	})

	t.Run("scan", func(t *testing.T) {
		// Range scans should be tracked as read spans.
		tracker, mockSender := makeMockTxnFeedReadTracker()
		txn := makeTxnProto()
		kvserverbase.TxnFeedEnabled.Override(ctx, &tracker.st.SV, true)

		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.ScanRequest{
			RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
		})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			br := ba.CreateReply()
			br.Txn = ba.Txn
			return br, nil
		})

		br, pErr := tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)

		// Commit and verify the scan span is attached.
		ba = &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn.Clone()}
		ba.Add(&kvpb.EndTxnRequest{Commit: true})

		mockSender.MockSend(func(ba *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
			et := ba.Requests[0].GetEndTxn()
			require.Len(t, et.ReadSpans, 1)
			require.Equal(t, roachpb.Key("a"), et.ReadSpans[0].Key)
			require.Equal(t, roachpb.Key("z"), et.ReadSpans[0].EndKey)
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			resp.Txn.Status = roachpb.COMMITTED
			return resp, nil
		})

		br, pErr = tracker.SendLocked(ctx, ba)
		require.Nil(t, pErr)
		require.NotNil(t, br)
	})
}
