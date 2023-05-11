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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type interceptedReq struct {
	ba *kvpb.BatchRequest

	fromNodeID  roachpb.NodeID
	toNodeID    roachpb.NodeID
	toReplicaID roachpb.ReplicaID
	toRangeID   roachpb.RangeID
	txnName     string
	txnMeta     *enginepb.TxnMeta
	prefix      string // for logging
}

func (ir interceptedReq) String() string {
	return fmt.Sprintf("(%s) n%d->n%d:r%d/%d",
		ir.txnName, ir.fromNodeID, ir.toNodeID, ir.toRangeID, ir.toReplicaID,
	)
}

type interceptedResp struct {
	br  *kvpb.BatchResponse
	err error
}

type interceptingTransport struct {
	kvcoord.Transport
	nID roachpb.NodeID

	beforeSend func(context.Context, interceptedReq) (bool, *kvpb.BatchResponse, error)
	afterSend  func(context.Context, interceptedReq, interceptedResp) (bool, *kvpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	txnName := "_"
	var txnMeta *enginepb.TxnMeta
	if ba.Txn != nil {
		txnName = ba.Txn.Name
		txnMeta = &ba.Txn.TxnMeta
	}

	req := interceptedReq{
		ba:          ba,
		fromNodeID:  t.nID,
		toNodeID:    t.NextReplica().NodeID,
		toReplicaID: t.NextReplica().ReplicaID,
		toRangeID:   ba.RangeID,
		txnName:     txnName,
		txnMeta:     txnMeta,
	}

	if t.beforeSend != nil {
		override, oResp, oErr := t.beforeSend(ctx, req)
		if override {
			return oResp, oErr
		}
	}

	resp := interceptedResp{}
	resp.br, resp.err = t.Transport.SendNext(ctx, ba)

	if t.afterSend != nil {
		override, oResp, oErr := t.afterSend(ctx, req, resp)
		if override {
			return oResp, oErr
		}
	}

	return resp.br, resp.err
}

// TestTransactionUnexpectedlyCommitted validates the handling of the case where
// a parallel commit transaction with an ambiguous error on a write races with
// a contending transaction's recovery attempt. In the case that the recovery
// succeeds prior to the original transaction's retries, an ambiguous error
// should be raised.
//
// NB: This case encounters a known issue described in #103817 and seen in #67765,
// where it currently is surfaced as an assertion failure that will result in a
// node crash.
//
// TODO(sarkesian): Validate the ambiguous result error once the initial fix as
// outlined in #103817 has been resolved.
func TestTransactionUnexpectedlyCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test depends on an intricate sequencing of events, so should not be
	// run under race/deadlock.
	skip.UnderShort(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	// Key constants.
	tablePrefix := bootstrap.TestingUserTableDataMin()
	keyA := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 1))
	keyB := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 2))

	// Checkpoints in test.
	txn1Ready := make(chan struct{})
	txn2Ready := make(chan struct{})
	leaseMoveReady := make(chan struct{})
	leaseMoveComplete := make(chan struct{})
	receivedETRetry := make(chan struct{})
	recoverComplete := make(chan struct{})
	txn1Done := make(chan struct{})

	// Final result.
	txn1ResultCh := make(chan error, 1)

	// Test synchronization helpers.
	// Handle all synchronization of KV operations at the transport level.
	// This allows us to schedule the operations such that they look like:
	//  txn1: read a (n1), b (n2)
	//  txn1: write a, endtxn (n1), write b (n2) encounters network failure
	//  txn2: read a, b; contends on locks held by n1 and issues push
	//     _: the push kicks off recovery of txn1
	//  <transfer b's lease to n1>
	//  txn1: reattempt failed write b (n1) and attempt to finalize transaction
	type CheckPoint int
	const (
		BeforeSending CheckPoint = iota
		AfterSending
	)
	tMu := struct {
		syncutil.RWMutex
		filter    func(req interceptedReq) bool
		willPause func(req interceptedReq) bool
		maybeWait func(cp CheckPoint, req interceptedReq, resp interceptedResp) (override error)
	}{}
	getInterceptingTransportFactory := func(nID roachpb.NodeID) kvcoord.TransportFactory {
		opID := 0
		return func(options kvcoord.SendOptions, dialer *nodedialer.Dialer, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
			transport, tErr := kvcoord.GRPCTransportFactory(options, dialer, slice)
			interceptor := &interceptingTransport{
				Transport: transport,
				nID:       nID,
				beforeSend: func(ctx context.Context, req interceptedReq) (bool, *kvpb.BatchResponse, error) {
					tMu.RLock()
					defer tMu.RUnlock()

					if tMu.filter != nil && tMu.filter(req) {
						opID++
						if tMu.willPause != nil && tMu.willPause(req) {
							req.prefix = fmt.Sprintf("[paused %d] ", opID)
						}
						t.Logf("%s%s batchReq={%s}, meta={%s}", req.prefix, req, req.ba, req.txnMeta)

						if tMu.maybeWait != nil {
							err := tMu.maybeWait(BeforeSending, req, interceptedResp{})
							if err != nil {
								return true, nil, err
							}
						}
					}

					return false, nil, nil
				},
				afterSend: func(ctx context.Context, req interceptedReq, resp interceptedResp) (bool, *kvpb.BatchResponse, error) {
					tMu.RLock()
					defer tMu.RUnlock()

					if tMu.filter != nil && tMu.filter(req) && tMu.maybeWait != nil {
						err := tMu.maybeWait(AfterSending, req, resp)
						if err != nil {
							return true, nil, err
						}
					}

					return false, nil, nil
				},
			}
			return interceptor, tErr
		}
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Disable closed timestamps for control over when transaction gets bumped.
	closedts.TargetDuration.Override(ctx, &st.SV, 1*time.Hour)

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Insecure: true,
		},
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {
				Settings: st,
				Insecure: true,
				Knobs: base.TestingKnobs{
					KVClient: &kvcoord.ClientTestingKnobs{
						TransportFactory: getInterceptingTransportFactory(roachpb.NodeID(1)),
					},
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							// NB: This setting is critical to the test, as it ensures that
							// a push will kick off recovery.
							RecoverIndeterminateCommitsOnFailedPushes: true,
						},
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	requireRangeLease := func(desc roachpb.RangeDescriptor, serverIdx int) {
		t.Helper()
		testutils.SucceedsSoon(t, func() error {
			hint := tc.Target(serverIdx)
			li, _, err := tc.FindRangeLeaseEx(ctx, desc, &hint)
			if err != nil {
				return errors.Wrapf(err, "could not find lease for %s", desc)
			}
			curLease := li.Current()
			if curLease.Empty() {
				return errors.Errorf("could not find lease for %s", desc)
			}
			expStoreID := tc.Target(serverIdx).StoreID
			if !curLease.OwnedBy(expStoreID) {
				return errors.Errorf("expected s%d to own the lease for %s\n"+
					"actual lease info: %s",
					expStoreID, desc, curLease)
			}
			if curLease.Speculative() {
				return errors.Errorf("only had speculative lease for %s", desc)
			}
			if !kvserver.ExpirationLeasesOnly.Get(&tc.Server(0).ClusterSettings().SV) &&
				curLease.Type() != roachpb.LeaseEpoch {
				return errors.Errorf("awaiting upgrade to epoch-based lease for %s", desc)
			}
			t.Logf("valid lease info for r%d: %v", desc.RangeID, curLease)
			return nil
		})
	}

	getInBatch := func(ctx context.Context, txn *kv.Txn, keys ...roachpb.Key) []int64 {
		batch := txn.NewBatch()
		for _, key := range keys {
			batch.GetForUpdate(key)
		}
		assert.NoError(t, txn.Run(ctx, batch))
		assert.Len(t, batch.Results, len(keys))
		vals := make([]int64, len(keys))
		for i, result := range batch.Results {
			assert.Len(t, result.Rows, 1)
			vals[i] = result.Rows[0].ValueInt()
		}
		return vals
	}

	db := tc.Server(0).DB()

	// Dump KVs at end of test for debugging purposes.
	defer func() {
		scannedKVs, err := db.Scan(ctx, tablePrefix, tablePrefix.PrefixEnd(), 0)
		require.NoError(t, err)
		for _, kv := range scannedKVs {
			mvccValue, err := storage.DecodeMVCCValue(kv.Value.RawBytes)
			require.NoError(t, err)
			t.Logf("key: %s, value: %s", kv.Key, mvccValue)
		}
	}()

	// Split so we have multiple ranges.
	// Corresponds to:
	_ = `
		CREATE TABLE bank (id INT PRIMARY KEY, balance INT);
		INSERT INTO bank VALUES (1, 50), (2, 50);
		ALTER TABLE bank SPLIT AT VALUES (2);`
	require.NoError(t, db.Put(ctx, keyA, 50))
	require.NoError(t, db.Put(ctx, keyB, 50))

	tc.SplitRangeOrFatal(t, keyA)
	firstRange, secondRange := tc.SplitRangeOrFatal(t, keyB)
	t.Logf("first range: %s", firstRange)
	t.Logf("second range: %s", secondRange)

	// Separate the leases for each range so they are not on the same node.
	tc.TransferRangeLeaseOrFatal(t, firstRange, tc.Target(0))
	requireRangeLease(firstRange, 0)
	tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(1))
	requireRangeLease(secondRange, 1)

	// Filtering and logging annotations for the request interceptor.
	tMu.Lock()
	tMu.filter = func(req interceptedReq) bool {
		// Log all requests on txn1 or txn2, except for heartbeats.
		if (req.txnName == "txn1" || req.txnName == "txn2") && !req.ba.IsSingleHeartbeatTxnRequest() {
			return true
		}

		// Log recovery on txn1/txn2's txn record key.
		if req.ba.IsSingleRecoverTxnRequest() && keyA.Equal(req.ba.Requests[0].GetRecoverTxn().Txn.Key) {
			return true
		}

		// Log pushes to txn1/txn2's txn record key.
		if req.ba.IsSinglePushTxnRequest() && keyA.Equal(req.ba.Requests[0].GetPushTxn().PusheeTxn.Key) {
			return true
		}

		return false
	}
	tMu.willPause = func(req interceptedReq) bool {
		_, hasPut := req.ba.GetArg(kvpb.Put)

		// txn1's writes to n2 will be paused.
		if req.txnName == "txn1" && hasPut && req.toNodeID == tc.Server(1).NodeID() {
			return true
		}

		// txn1's retried EndTxn will be paused.
		if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() {
			return true
		}

		// The recovery operation on txn1 needs to be sequenced specifically.
		if req.ba.IsSingleRecoverTxnRequest() {
			return true
		}

		return false
	}
	tMu.Unlock()

	// Execute implicit transaction to move $10 from account 1 to account 2.
	// Corresponds to:
	_ = `
		UPDATE bank SET balance =
			CASE id
				WHEN $1 THEN balance - $3
				WHEN $2 THEN balance + $3
			END
			WHERE id IN ($1, $2)`
	const xferAmount = 10

	// Concurrent transactions.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		// Wait until txn1 is ready to start.
		select {
		case <-txn1Ready:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("txn1 quiescing...")
		}
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, "txn1")
		vals := getInBatch(tCtx, txn, keyA, keyB)

		batch := txn.NewBatch()
		batch.Put(keyA, vals[0]-xferAmount)
		batch.Put(keyB, vals[1]+xferAmount)
		txn1ResultCh <- txn.CommitInBatch(tCtx, batch)
	}()
	go func() {
		defer wg.Done()
		// Wait until txn2 is ready to start.
		select {
		case <-txn2Ready:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("txn2 quiescing...")
		}
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, "txn2")
		vals := getInBatch(tCtx, txn, keyA, keyB)

		batch := txn.NewBatch()
		batch.Put(keyA, vals[0]-xferAmount)
		batch.Put(keyB, vals[1]+xferAmount)
		assert.NoError(t, txn.CommitInBatch(tCtx, batch))
	}()
	go func() {
		defer wg.Done()
		// Wait until lease move is ready.
		select {
		case <-leaseMoveReady:
		case <-tc.Stopper().ShouldQuiesce():
			t.Logf("lease mover quiescing...")
		}

		t.Logf("Transferring r%d lease to n%d", secondRange.RangeID, tc.Target(0).NodeID)
		assert.NoError(t, tc.TransferRangeLease(secondRange, tc.Target(0)))

		close(leaseMoveComplete)
	}()

	// KV Request sequencing.
	tMu.Lock()
	tMu.maybeWait = func(cp CheckPoint, req interceptedReq, resp interceptedResp) (override error) {
		_, hasPut := req.ba.GetArg(kvpb.Put)

		// These conditions are checked in order of expected operations of the test.

		// 1. txn1->n1: Get(a)
		// 2. txn1->n2: Get(b)
		// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
		// 4. txn1->n2: Put(b) -- Send the request, but pause before returning the
		// response so we can inject network failure.
		if req.txnName == "txn1" && hasPut && req.toNodeID == tc.Server(1).NodeID() && cp == AfterSending {
			// Once we have seen the write on txn1 to n2 that we will fail, txn2 can start.
			close(txn2Ready)
		}

		// 5. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
		// 6. txn2->n2: Get(b)
		// 7. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts recovery.
		// 8. _->n1: RecoverTxn(txn1) -- Before sending, pause the request so we
		// can ensure it gets evaluated after txn1 retries (and refreshes), but before
		// its final EndTxn.
		if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
			// Once the RecoverTxn request is issued, as part of txn2's PushTxn
			// request, the lease can be moved.
			close(leaseMoveReady)
		}

		// <transfer b's lease to n1>
		// <inject a network failure and finally allow (4) txn1->n2: Put(b) to return with error>
		if req.txnName == "txn1" && hasPut && req.toNodeID == tc.Server(1).NodeID() && cp == AfterSending {
			// Hold the operation open until we are ready to retry on the new replica,
			// after which we will return the injected failure.
			<-leaseMoveComplete
			t.Logf("%s%s Put op unpaused (injected RPC error)", req.prefix, req)
			return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
		}

		// -- NB: If ambiguous errors were propagated, txn1 would end here.
		// -- NB: When ambiguous errors are not propagated, txn1 continues with:
		//
		// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
		// timestamp, and attempts to evaluate it as an idempotent replay, but at a
		// higher timestamp, which breaks idempotency due to being on commit.
		// 10. txn1->n1: Refresh(a)
		// 11. txn1->n1: Refresh(b)
		// Note that if these refreshes fail, the transaction coordinator
		// would return a retriable error, although the transaction could be actually
		// committed during recovery - this is highly problematic.

		// 12. txn1->n1: EndTxn(commit) -- Before sending, pause the request so that
		// we can allow (8) RecoverTxn(txn1) to proceed, simulating a race in which
		// the recovery wins.
		if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
			close(receivedETRetry)
		}

		// <allow (8) RecoverTxn(txn1) to proceed and finish> -- because txn1
		// is in STAGING and has all of its writes, it is implicitly committed,
		// so the recovery will succeed in marking it explicitly committed.
		if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
			// The RecoverTxn operation must be evaluated after txn1's Refreshes,
			// or after txn1 completes with error.
			select {
			case <-receivedETRetry:
			case <-txn1Done:
			}
			t.Logf("%s%s RecoverTxn op unpaused", req.prefix, req)
		}
		if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
			t.Logf("%s RecoverTxn op complete, resp={%s}", req, resp.br.Responses[0].GetRecoverTxn())
			close(recoverComplete)
		}

		// <allow (12) EndTxn(commit) to proceed and execute> -- Results in
		// "transaction unexpectedly committed" due to the recovery completing first.
		if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
			<-recoverComplete
			t.Logf("%s%s EndTxn op unpaused", req.prefix, req)
		}

		return nil
	}
	tMu.Unlock()

	// Start test, await concurrent operations and validate results.
	close(txn1Ready)
	err := <-txn1ResultCh
	t.Logf("txn1 completed with err: %+v", err)
	close(txn1Done)
	wg.Wait()

	// TODO(sarkesian): While we expect an AmbiguousResultError once the immediate
	//  changes outlined in #103817 are implemented, right now this is essentially
	//  validating the existence of the bug. This needs to be fixed, and we should
	//  not see an assertion failure from the transaction coordinator once fixed.
	tErr := (*kvpb.TransactionStatusError)(nil)
	require.ErrorAsf(t, err, &tErr,
		"expected TransactionStatusError due to being already committed")
	require.Equalf(t, kvpb.TransactionStatusError_REASON_TXN_COMMITTED, tErr.Reason,
		"expected TransactionStatusError due to being already committed")
	require.Truef(t, errors.HasAssertionFailure(err),
		"expected AssertionFailedError due to sanity check on transaction already committed")
}
