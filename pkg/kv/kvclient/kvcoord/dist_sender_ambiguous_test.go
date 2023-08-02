// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/cockroachdb/cockroach/pkg/util"
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

func (req *interceptedReq) String() string {
	return fmt.Sprintf("%s(%s) n%d->n%d:r%d/%d",
		req.prefix, req.txnName, req.fromNodeID, req.toNodeID, req.toRangeID, req.toReplicaID,
	)
}

// pauseUntil blocks on untilCh, logging before and after.
func (req *interceptedReq) pauseUntil(t *testing.T, untilCh chan struct{}, cp InterceptPoint) {
	t.Logf("%s ‹%s› paused %s", req, req.ba.Summary(), cp)
	<-untilCh
	t.Logf("%s ‹%s› unpaused", req, req.ba.Summary())
}

// pauseUntil blocks until the first of c1/2 is ready, logging before and after.
func (req *interceptedReq) pauseUntilFirst(t *testing.T, c1, c2 chan struct{}, cp InterceptPoint) {
	t.Logf("%s ‹%s› paused %s", req, req.ba.Summary(), cp)
	select {
	case <-c1:
	case <-c2:
	}
	t.Logf("%s ‹%s› unpaused", req, req.ba.Summary())
}

type interceptedResp struct {
	br  *kvpb.BatchResponse
	err error
}

func (resp *interceptedResp) String() string {
	if resp.err != nil || resp.br == nil {
		return fmt.Sprintf("err: %s", resp.err)
	}

	if resp.br.Error != nil {
		return fmt.Sprintf("br.Error: %s", resp.br.Error)
	}

	var output strings.Builder
	for i, response := range resp.br.Responses {
		if i > 0 {
			fmt.Fprintf(&output, ",")
		}
		fmt.Fprintf(&output, "%s", response.GetInner())
	}
	return output.String()
}

// interceptingTransport is a gRPC transport wrapper than can be returned from a
// kvcoord.TransportFactory that is passed in to the KVClient testing knobs in
// order to block requests/responses and inject RPC failures. It is used as
// the transport for kvpb.BatchRequest in the kvcoord.DistSender.
type interceptingTransport struct {
	kvcoord.Transport
	nID roachpb.NodeID

	// beforeSend defines an optional function called before sending a
	// kvpb.BatchRequest on the underlying transport. If a non-nil response
	// is returned, it will be returned to the caller without sending the request.
	beforeSend func(context.Context, *interceptedReq) (overrideResp *interceptedResp)

	// afterSend defines an optional function called after sending a
	// kvpb.BatchRequest on the underlying tranport, passing in both the request
	// and response (*kvpb.BatchResponse, error). If a non-nil response is
	// returned, it will be returned to the caller instead of the real response.
	afterSend func(context.Context, *interceptedReq, *interceptedResp) (overrideResp *interceptedResp)
}

// SendNext implements the kvcoord.Transport interface.
func (t *interceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	txnName := "_"
	var txnMeta *enginepb.TxnMeta
	if ba.Txn != nil {
		txnName = ba.Txn.Name
		txnMeta = &ba.Txn.TxnMeta
	}

	req := &interceptedReq{
		ba:          ba,
		fromNodeID:  t.nID,
		toNodeID:    t.NextReplica().NodeID,
		toReplicaID: t.NextReplica().ReplicaID,
		toRangeID:   ba.RangeID,
		txnName:     txnName,
		txnMeta:     txnMeta,
	}

	if t.beforeSend != nil {
		overrideResp := t.beforeSend(ctx, req)
		if overrideResp != nil {
			return overrideResp.br, overrideResp.err
		}
	}

	resp := &interceptedResp{}
	resp.br, resp.err = t.Transport.SendNext(ctx, ba)

	if t.afterSend != nil {
		overrideResp := t.afterSend(ctx, req, resp)
		if overrideResp != nil {
			return overrideResp.br, overrideResp.err
		}
	}

	return resp.br, resp.err
}

type InterceptPoint int

const (
	BeforeSending InterceptPoint = iota
	AfterSending
)

func (cp InterceptPoint) String() string {
	switch cp {
	case BeforeSending:
		return "before send"
	case AfterSending:
		return "after send"
	default:
		panic(fmt.Sprintf("unknown InterceptPoint: %d", cp))
	}
}

// interceptorHelperMutex represents a convenience structure so that tests or
// subtests using the interceptingTransport can organize the interception and
// request/response sequencing logic alongside the logic of the test itself.
// The override functions are locked with a RWMutex since the transport is used
// for the entire lifetime of the test cluster, but the logic of the overridden
// functions is particular to the test/subtest; this way the logic can be
// modified after the test cluster has started.
type interceptorHelperMutex struct {
	syncutil.RWMutex
	interceptorTestConfig
}

// interceptorTestConfig is the inner shared state of interceptorHelperMutex.
type interceptorTestConfig struct {
	*testing.T

	// lastInterceptedOpID is a counter that is to be incremented by each
	// request r for which filter(r) returns true, for the purposes of debugging.
	// Incremented atomically by concurrent operations holding the read lock.
	lastInterceptedOpID int64

	// filter defines a function that should return true for requests the test
	// cares about - this includes logging, blocking, or overriding responses. All
	// requests that return true should be logged.
	filter func(req *interceptedReq) (isObservedReq bool)

	// maybeWait defines a function within which the actual sequencing of requests
	// and responses can occur, by blocking until conditions are met. If a non-nil
	// error is returned, it will be returned to the DistSender, otherwise the
	// BatchResponse/error from the underlying transport will be returned.
	maybeWait func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error)
}

// configureSubTest is a utility for the interceptorHelperMutex so that all
// test logging and assertions performed by the interceptor can be tied to the
// particular active subtest. Returns a function to restore the original
// configuration on teardown.
func (tMu *interceptorHelperMutex) configureSubTest(t *testing.T) (restore func()) {
	tMu.Lock()
	defer tMu.Unlock()

	origConfig := tMu.interceptorTestConfig
	restore = func() {
		tMu.Lock()
		defer tMu.Unlock()
		tMu.interceptorTestConfig = origConfig
	}

	tMu.T = t
	tMu.lastInterceptedOpID = 0
	return restore
}

// TestTransactionUnexpectedlyCommitted validates the handling of the case where
// a parallel commit transaction with an ambiguous error on a write races with
// a contending transaction's recovery attempt. In the case that the recovery
// succeeds prior to the original transaction's retries, an ambiguous error
// should be raised.
//
// NB: This case encounters a known issue described in #103817 and seen in
// #67765, where it currently is surfaced as an assertion failure that will
// result in a node crash.
//
// TODO(sarkesian): Validate the ambiguous result error once the initial fix as
// outlined in #103817 has been resolved.
func TestTransactionUnexpectedlyCommitted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test depends on an intricate sequencing of events that can take
	// several seconds, and requires maintaining expected leases.
	skip.UnderShort(t)
	skip.UnderStressRace(t)

	succeedsSoonDuration := testutils.DefaultSucceedsSoonDuration
	if util.RaceEnabled {
		succeedsSoonDuration = testutils.RaceSucceedsSoonDuration
	}

	// Key constants.
	tablePrefix := bootstrap.TestingUserTableDataMin()
	tableSpan := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}
	keyA := roachpb.Key(encoding.EncodeBytesAscending(tablePrefix.Clone(), []byte("a")))
	keyB := roachpb.Key(encoding.EncodeBytesAscending(tablePrefix.Clone(), []byte("b")))

	// Test synchronization helpers.
	// Handles all synchronization of KV operations at the transport level.
	tMu := interceptorHelperMutex{
		interceptorTestConfig: interceptorTestConfig{
			T: t,
		},
	}
	getInterceptingTransportFactory := func(nID roachpb.NodeID) kvcoord.TransportFactory {
		return func(options kvcoord.SendOptions, dialer *nodedialer.Dialer, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
			transport, tErr := kvcoord.GRPCTransportFactory(options, dialer, slice)
			interceptor := &interceptingTransport{
				Transport: transport,
				nID:       nID,
				beforeSend: func(ctx context.Context, req *interceptedReq) (overrideResp *interceptedResp) {
					tMu.RLock()
					defer tMu.RUnlock()

					if tMu.filter != nil && tMu.filter(req) {
						opID := atomic.AddInt64(&tMu.lastInterceptedOpID, 1)
						req.prefix = fmt.Sprintf("[op %d] ", opID)
						tMu.Logf("%s batchReq={%s}, meta={%s}", req, req.ba, req.txnMeta)

						if tMu.maybeWait != nil {
							err := tMu.maybeWait(BeforeSending, req, nil)
							if err != nil {
								return &interceptedResp{err: err}
							}
						}
					}

					return nil
				},
				afterSend: func(ctx context.Context, req *interceptedReq, resp *interceptedResp) (overrideResp *interceptedResp) {
					tMu.RLock()
					defer tMu.RUnlock()

					if tMu.filter != nil && tMu.filter(req) && tMu.maybeWait != nil {
						err := tMu.maybeWait(AfterSending, req, resp)
						if err != nil {
							return &interceptedResp{err: err}
						}
					}

					return nil
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

	requireRangeLease := func(t *testing.T, desc roachpb.RangeDescriptor, serverIdx int) {
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

	// Perform initial range split.
	{
		tc.SplitRangeOrFatal(t, keyA)
		firstRange, secondRange := tc.SplitRangeOrFatal(t, keyB)
		t.Logf("first range: %s", firstRange)
		t.Logf("second range: %s", secondRange)
	}

	initSubTest := func(t *testing.T) (finishSubTest func()) {
		restoreAfterSubTest := tMu.configureSubTest(t)

		// Write initial values and separate leases.
		require.NoError(t, db.Put(ctx, keyA, 50))
		require.NoError(t, db.Put(ctx, keyB, 50))

		firstRange := tc.LookupRangeOrFatal(t, keyA)
		secondRange := tc.LookupRangeOrFatal(t, keyB)
		tc.TransferRangeLeaseOrFatal(t, firstRange, tc.Target(0))
		requireRangeLease(t, firstRange, 0)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(1))
		requireRangeLease(t, secondRange, 1)

		return func() {
			defer restoreAfterSubTest()

			// Dump KVs at end of test for debugging purposes on failure.
			if t.Failed() {
				scannedKVs, err := db.Scan(ctx, tablePrefix, tablePrefix.PrefixEnd(), 0)
				require.NoError(t, err)
				for _, scannedKV := range scannedKVs {
					mvccValue, err := storage.DecodeMVCCValue(scannedKV.Value.RawBytes)
					require.NoError(t, err)
					t.Logf("key: %s, value: %s", scannedKV.Key, mvccValue)
				}
			}
		}
	}

	// Filtering and logging annotations for the request interceptor.
	tMu.Lock()
	tMu.filter = func(req *interceptedReq) bool {
		// Log all requests on txn1 or txn2, except for heartbeats.
		if (req.txnName == "txn1" || req.txnName == "txn2") && !req.ba.IsSingleHeartbeatTxnRequest() {
			return true
		}

		// Log recovery on txn1's txn record key.
		if req.ba.IsSingleRecoverTxnRequest() && keyA.Equal(req.ba.Requests[0].GetRecoverTxn().Txn.Key) {
			return true
		}

		// Log pushes to txn1's txn record key.
		if req.ba.IsSinglePushTxnRequest() && keyA.Equal(req.ba.Requests[0].GetPushTxn().PusheeTxn.Key) {
			return true
		}

		// Log intent resolution on the key span used in the test.
		if riReq, ok := req.ba.GetArg(kvpb.ResolveIntent); ok && tableSpan.ContainsKey(riReq.Header().Key) {
			return true
		}

		return false
	}
	tMu.Unlock()

	// Test contending implicit transactions with distributed writes and parallel
	// commits with injected RPC failures. As this test was modeled after a
	// real-world failure seen in the "bank" workload, this corresponds to the
	// following SQL operation:
	_ = `
	UPDATE bank SET balance =
		CASE id
			WHEN $1 THEN balance - $3
			WHEN $2 THEN balance + $3
		END
		WHERE id IN ($1, $2)`
	const xferAmount = 10

	execWorkloadTxn := func(t *testing.T, name string) error {
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, name)
		vals := getInBatch(tCtx, txn, keyA, keyB)

		batch := txn.NewBatch()
		batch.Put(keyA, vals[0]-xferAmount)
		batch.Put(keyB, vals[1]+xferAmount)
		return txn.CommitInBatch(tCtx, batch)
	}

	waitUntilReady := func(t *testing.T, name string, readyCh chan struct{}) (finishedWithoutTimeout bool) {
		select {
		case <-readyCh:
		case <-time.After(succeedsSoonDuration):
			t.Logf("%s timed out before start", name)
			return false
		}

		return true
	}

	runTxn := func(t *testing.T, name string, wg *sync.WaitGroup, readyCh, doneCh chan struct{}, resultCh chan error) {
		defer wg.Done()
		if doneCh != nil {
			defer close(doneCh)
		}
		if !waitUntilReady(t, name, readyCh) {
			return
		}
		err := execWorkloadTxn(t, name)
		if resultCh != nil {
			resultCh <- err
		} else {
			assert.NoError(t, err)
		}
	}

	runLeaseMover := func(t *testing.T, wg *sync.WaitGroup, readyCh chan struct{}, doneCh chan struct{}) {
		defer wg.Done()
		if !waitUntilReady(t, "lease mover", readyCh) {
			return
		}

		desc, err := tc.LookupRange(keyB)
		assert.NoError(t, err)
		t.Logf("Transferring r%d lease to n%d", desc.RangeID, tc.Target(0).NodeID)
		assert.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))

		close(doneCh)
	}

	t.Run("workload conflict", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		leaseMoveReady := make(chan struct{})
		leaseMoveComplete := make(chan struct{})
		receivedFinalET := make(chan struct{})
		recoverComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(3)
		go runTxn(t, "txn1", &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runTxn(t, "txn2", &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runLeaseMover(t, &wg, leaseMoveReady, leaseMoveComplete)

		// KV Request sequencing.
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			_, hasPut := req.ba.GetArg(kvpb.Put)

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n2: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n2: Put(b) -- Send the request, but pause before returning
			// the response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && req.toNodeID == tc.Server(1).NodeID() && cp == AfterSending {
				// Once we have seen the write on txn1 to n2 that we will fail, txn2
				// can start.
				close(txn2Ready)
			}

			// 5. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
			// 6. txn2->n2: Get(b)
			// 7. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 8. _->n1: RecoverTxn(txn1) -- Before sending, pause the request so we
			// can ensure it gets evaluated after txn1 retries (and refreshes), but
			// before its final EndTxn.
			if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
				// Once the RecoverTxn request is issued, as part of txn2's PushTxn
				// request, the lease can be moved.
				close(leaseMoveReady)
			}

			// <transfer b's lease to n1>
			// <inject a network failure and finally allow (4) txn1->n2: Put(b) to
			// return with error>
			if req.txnName == "txn1" && hasPut && req.toNodeID == tc.Server(1).NodeID() && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, leaseMoveComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// -- NB: If ambiguous errors were propagated, txn1 would end here.
			// -- NB: When ambiguous errors are not propagated, txn1 continues with:
			//
			// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.
			// 10. txn1->n1: Refresh(a)
			// 11. txn1->n1: Refresh(b)
			// 12. txn1->n1: EndTxn(commit) -- Before sending, pause the request so
			// that we can allow (8) RecoverTxn(txn1) to proceed, simulating a race
			// in which the recovery wins.
			if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				close(receivedFinalET)
			}

			// <allow (8) RecoverTxn(txn1) to proceed and finish> -- because txn1
			// is in STAGING and has all of its writes, it is implicitly committed,
			// so the recovery will succeed in marking it explicitly committed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
				// The RecoverTxn operation is evaluated after txn1's Refreshes,
				// or after txn1 completes with error.
				req.pauseUntilFirst(t, receivedFinalET, txn1Done, cp)
			}
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(recoverComplete)
			}

			// <allow (12) EndTxn(commit) to proceed and execute> -- Results in
			// "transaction unexpectedly committed" due to the recovery completing
			// first.
			if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				req.pauseUntil(t, recoverComplete, cp)
			}

			// <allow txn2's Puts to execute>
			if req.txnName == "txn2" && hasPut && cp == BeforeSending {
				// While txn2's Puts can only occur after txn1 is marked as explicitly
				// committed, if the Recovery and the subsequent txn2 Put(b) operations
				// happen before txn1 retries its Put(b) on n1, it will encounter txn2's
				// intent and get a WriteTooOld error instead of potentially being an
				// idempotent replay.
				<-txn1Done
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %+v", err)
		wg.Wait()

		// TODO(sarkesian): While we expect an AmbiguousResultError once the
		// immediate changes outlined in #103817 are implemented, right now this is
		// essentially validating the existence of the bug. This needs to be fixed,
		// and we should not see an assertion failure from the transaction
		// coordinator once fixed.
		tErr := (*kvpb.TransactionStatusError)(nil)
		require.ErrorAsf(t, err, &tErr,
			"expected TransactionStatusError due to being already committed")
		require.Equalf(t, kvpb.TransactionStatusError_REASON_TXN_COMMITTED, tErr.Reason,
			"expected TransactionStatusError due to being already committed")
		require.Truef(t, errors.HasAssertionFailure(err),
			"expected AssertionFailedError due to sanity check on transaction already committed")
	})
}
