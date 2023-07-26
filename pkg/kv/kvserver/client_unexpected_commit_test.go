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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

type interceptingTransport struct {
	kvcoord.Transport
	intercept func(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	return t.intercept(ctx, ba)
}

func getJamKey(
	txnName string, fromNodeID, toNodeID roachpb.NodeID, rangeID roachpb.RangeID,
) string {
	return fmt.Sprintf("%s:n%d->n%d:r%d", txnName, fromNodeID, toNodeID, rangeID)
}

// TestTransactionUnexpectedlyCommitted validates the handling of the case where
// a parallel commit transaction with an ambiguous error on a write races with
// a contending transaction's recovery attempt. In the case that the recovery
// succeeds prior to the original transaction's retries, an ambiguous error
// should be raised.
//
// NB: This case deals with a known issue described in #103817 and seen in #67765.
func TestTransactionUnexpectedlyCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is slow and depends on an intricate sequencing of events, so
	// should not be run under race/deadlock.
	skip.UnderShort(t)
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	// Key constants.
	tablePrefix := bootstrap.TestingUserTableDataMin()
	key1 := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 1))
	key2 := roachpb.Key(encoding.EncodeUvarintAscending(tablePrefix.Clone(), 2))

	// Checkpoints in test.
	var jammedOps sync.Map // map[string]chan struct{}
	txn1Ready := make(chan struct{})
	txn2Ready := make(chan struct{})
	leaseMoveReady := make(chan struct{})
	txn1Done := make(chan struct{})
	receivedETRetry := make(chan struct{})
	recoverComplete := make(chan struct{})
	networkManipReady := make(chan struct{})
	networkManipComplete := make(chan struct{})

	// Final result.
	txn1ResultCh := make(chan error, 1)

	// Test helpers.
	dbg := func(format string, args ...any) {
		t.Helper()
		t.Logf("[dbg] "+format, args...)
	}

	dumpKVs := func(scanResults []kv.KeyValue) {
		t.Helper()
		for _, kv := range scanResults {
			mvccValue, err := storage.DecodeMVCCValue(kv.Value.RawBytes)
			require.NoError(t, err)
			t.Logf("key: %s, value: %s", kv.Key, mvccValue)
		}
	}

	// Network jamming helper functions.
	jamOperation := func(jamKey string) {
		dbg("Jamming %s", jamKey)
		jamChan := make(chan struct{})
		jammedOps.Store(jamKey, jamChan)
	}

	unjamOperation := func(jamKey string) {
		var jamChan chan struct{}
		if jamVal, ok := jammedOps.Load(jamKey); ok {
			jamChan = jamVal.(chan struct{})
		} else {
			t.Fatalf("couldn't find operation to unjam: %s", jamKey)
		}

		close(jamChan)
	}

	// Handle all synchronization of KV operations at the transport level.
	// This allows us to schedule the operations such that they look like:
	//  txn1: read a (n1), b (n2)
	//  txn1: write a, endtxn (n1), write b (n2) encounters network failure
	//  txn2: read a, b; contends on locks held by n1 and issues push
	//     _: the push kicks of recovery of txn1
	//  <transfer b's lease to n1>
	//  txn1: reattempt failed write b (n1) and attempt to finalize transaction
	//
	// We currently see these operations:
	//  txn1->n1: Get(a)
	//  txn1->n2: Get(b)
	//  txn1->n1: Put(a), EndTxn(parallel commit)
	//  txn1->n2: Put(b) -- network failure!
	//  txn2->n1: Get(a)
	//  	 _->n1: PushTxn(txn2->txn1)
	//  <transfer b's lease to n1>
	//  -- NB: When ambiguous errors get propagated, txn1 will end here.
	//         When ambiguous errors are not propagated, txn1 continues with:
	//  txn1->n1: Put(b) -- retry sees new lease start timestamp
	//  txn1->n1: Refresh(a)
	//  txn1->n1: Refresh(b)
	//     _->n1: RecoverTxn(txn1) -- due to the PushTxn seeing txn1 in staging
	//  txn1->n1: EndTxn(commit) -- results in transaction unexpectedly committed!
	getInterceptingTransportFactory := func(nID roachpb.NodeID) kvcoord.TransportFactory {
		opID := 0
		return func(options kvcoord.SendOptions, dialer *nodedialer.Dialer, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
			transport, tErr := kvcoord.GRPCTransportFactory(options, dialer, slice)
			interceptor := &interceptingTransport{
				Transport: transport,
				intercept: func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
					fromNodeID := nID
					toNodeID := transport.NextReplica().NodeID
					toRangeID := ba.RangeID
					toReplicaID := transport.NextReplica().ReplicaID
					txnName := "_"
					var txnMeta *enginepb.TxnMeta
					if ba.Txn != nil {
						txnName = ba.Txn.Name
						txnMeta = &ba.Txn.TxnMeta
					}
					jamKey := getJamKey(txnName, fromNodeID, toNodeID, toRangeID)
					var jammed bool
					var jamChan chan struct{}
					var tags strings.Builder

					if jamVal, ok := jammedOps.Load(jamKey); ok {
						jamChan = jamVal.(chan struct{})
					}

					if ((txnName == "txn1" || txnName == "txn2") && !ba.IsSingleHeartbeatTxnRequest()) ||
						(ba.IsSingleRecoverTxnRequest() && key1.Equal(ba.Requests[0].GetRecoverTxn().Txn.Key)) ||
						(ba.IsSinglePushTxnRequest() && key1.Equal(ba.Requests[0].GetPushTxn().PusheeTxn.Key)) {
						opID++
						if jamChan != nil {
							jammed = true
							fmt.Fprintf(&tags, "[jammed %d] ", opID)
						} else if (txnName == "txn1" && ba.IsSingleEndTxnRequest()) || ba.IsSingleRecoverTxnRequest() {
							fmt.Fprintf(&tags, "[paused %d] ", opID)
						}

						fmt.Fprintf(&tags, "(%s) n%d->n%d:r%d/%d ",
							txnName, fromNodeID, toNodeID, toRangeID, toReplicaID,
						)

						t.Logf("%sbatchReq={%s}, meta={%s}", tags.String(), ba, txnMeta)
					}

					// Ensure that txn1's post-refresh EndTxn occurs after recovery.
					if txnName == "txn1" && ba.IsSingleEndTxnRequest() {
						close(receivedETRetry)
						<-recoverComplete
						t.Logf("%sEndTxn op unpaused", tags.String())
					}

					// Block transaction recovery until ready.
					if ba.IsSingleRecoverTxnRequest() && key1.Equal(ba.Requests[0].GetRecoverTxn().Txn.Key) {
						// Once the RecoverTxn request is issued, as part of txn2's PushTxn
						// request, the lease can be moved.
						close(leaseMoveReady)

						// The RecoverTxn operation must be evaluated after txn1's Refreshes,
						// or after txn1 completes with error.
						select {
						case <-receivedETRetry:
						case <-txn1Done:
						}
						t.Logf("%sRecoverTxn op unpaused", tags.String())
					}

					br, err := transport.SendNext(ctx, ba)

					// Once recovery is completed, signal to allow txn1's EndTxn to be retried.
					if ba.IsSingleRecoverTxnRequest() && key1.Equal(ba.Requests[0].GetRecoverTxn().Txn.Key) {
						t.Logf("RECOVERY: op complete, batchResp={%s}", br)
						close(recoverComplete)
					}

					if jammed {
						// Allow txn2 to start while txn1's EndTxn is jammed.
						dbg("txn2 ready")
						close(txn2Ready)

						if br.Error != nil || err != nil {
							// The requests should not encounter errors, as this can interfere
							// with the test's expectations.
							t.Errorf("%sop encountered error, br.Error = %v, err = %v", tags.String(), br.Error, err)
						} else {
							<-jamChan
							t.Logf("%sop released", tags.String())
							return nil, grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", fromNodeID, toNodeID)
						}
					}

					return br, err
				},
			}
			return interceptor, tErr
		}
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// Disable closed timestamps for greater control over when transaction gets bumped.
	closedts.TargetDuration.Override(ctx, &st.SV, 1*time.Hour)
	ts.TimeseriesStorageEnabled.Override(ctx, &st.SV, false)

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
						DisableCommitSanityCheck: true,
						TransportFactory:         getInterceptingTransportFactory(roachpb.NodeID(1)),
					},
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
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
		dumpKVs(scannedKVs)
	}()

	// Split so we have multiple ranges.
	// Corresponds to:
	_ = `
		CREATE TABLE bank (id INT PRIMARY KEY, balance INT);
		INSERT INTO bank VALUES (1, 50), (2, 50);
		ALTER TABLE bank SPLIT AT VALUES (2);`
	dbg("Putting initial keys")
	require.NoError(t, db.Put(ctx, key1, 50))
	require.NoError(t, db.Put(ctx, key2, 50))

	dbg("Splitting ranges")
	tc.SplitRangeOrFatal(t, key1)
	firstRange, secondRange := tc.SplitRangeOrFatal(t, key2)
	t.Logf("first range: %s", firstRange)
	t.Logf("second range: %s", secondRange)

	// Separate the leases for each range so they are not on the same node.
	dbg("Moving leases")
	tc.TransferRangeLeaseOrFatal(t, firstRange, tc.Target(0))
	requireRangeLease(firstRange, 0)
	tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(1))
	requireRangeLease(secondRange, 1)

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
	jamTxn1Key := getJamKey("txn1", tc.Server(0).NodeID(), tc.Server(1).NodeID(), secondRange.RangeID)

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
		vals := getInBatch(tCtx, txn, key1, key2)

		// Signal for network jamming to occur.
		close(networkManipReady)
		<-networkManipComplete

		batch := txn.NewBatch()
		batch.Put(key1, vals[0]-xferAmount)
		batch.Put(key2, vals[1]+xferAmount)
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
		vals := getInBatch(tCtx, txn, key1, key2)

		batch := txn.NewBatch()
		batch.Put(key1, vals[0]-xferAmount)
		batch.Put(key2, vals[1]+xferAmount)
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

		dbg("Transferring r%d lease to n%d", secondRange.RangeID, tc.Target(0).NodeID)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(0))

		unjamOperation(jamTxn1Key)
	}()

	// Test synchronization.
	close(txn1Ready)

	// Jam network connections when ready.
	<-networkManipReady
	dbg("Manipulating network links")
	jamOperation(jamTxn1Key)
	close(networkManipComplete)

	// Await concurrent operations and validate results.
	err := <-txn1ResultCh
	t.Logf("txn1 completed with err: %s", err)
	close(txn1Done)
	wg.Wait()

	// NB: While ideally we would hope to see a successful commit
	// without error, with the near-term solution outlined in #103817 we expect
	// an AmbiguousResultError in this case.
	aErr := (*kvpb.AmbiguousResultError)(nil)
	tErr := (*kvpb.TransactionStatusError)(nil)
	require.ErrorAsf(t, err, &aErr,
		"expected ambiguous result error due to RPC error")
	require.Falsef(t, errors.As(err, &tErr),
		"did not expect TransactionStatusError due to being already committed")
	require.Falsef(t, errors.HasAssertionFailure(err),
		"expected no AssertionFailedError due to sanity check on transaction already committed")
}
