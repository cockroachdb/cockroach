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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	// modifyReq defines a function that can modify the intercepted BatchRequest.
	modifyReq func(req *interceptedReq) (modified bool)

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

// validateTxnCommitAmbiguousError checks that an error on txn commit is
// ambiguous, rather than an assertion failure or a retryable error.
func validateTxnCommitAmbiguousError(t *testing.T, err error, reason string) {
	aErr := (*kvpb.AmbiguousResultError)(nil)
	rErr := (*kvpb.TransactionRetryWithProtoRefreshError)(nil)
	tErr := (*kvpb.TransactionStatusError)(nil)
	require.Errorf(t, err, "expected an AmbiguousResultError")
	require.ErrorAsf(t, err, &aErr,
		"expected AmbiguousResultError due to %s", reason)
	require.Falsef(t, errors.As(err, &tErr),
		"did not expect TransactionStatusError due to being already committed")
	require.Falsef(t, errors.As(err, &rErr),
		"did not expect incorrect TransactionRetryWithProtoRefreshError due to failed refresh")
	require.Falsef(t, errors.HasAssertionFailure(err),
		"expected no AssertionFailedError due to sanity check on transaction already committed")
}

// TestTransactionUnexpectedlyCommitted validates the handling of the case where
// a parallel commit transaction with an ambiguous error on a write races with
// a contending transaction's recovery attempt. In the case that the recovery
// succeeds prior to the original transaction's retries, an ambiguous error
// should be raised.
//
// NB: These tests deal with a known issue described in #103817 and seen in
// #67765.
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
	tablePrefix := bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec)
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
	getInterceptingTransportFactory := func(nID roachpb.NodeID) func(kvcoord.TransportFactory) kvcoord.TransportFactory {
		return func(factory kvcoord.TransportFactory) kvcoord.TransportFactory {
			return func(options kvcoord.SendOptions, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
				transport, tErr := factory(options, slice)
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

						if tMu.modifyReq != nil && tMu.modifyReq(req) {
							tMu.Logf("%s modified", req)
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
						TransportFactory: getInterceptingTransportFactory(1),
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

	getInBatch := func(t *testing.T, ctx context.Context, txn *kv.Txn, keys ...roachpb.Key) []int64 {
		batch := txn.NewBatch()
		for _, key := range keys {
			batch.GetForUpdate(key, kvpb.BestEffort)
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
		// Log all requests on txn1/txn2/txn3, except for heartbeats.
		if (req.txnName == "txn1" || req.txnName == "txn2" || req.txnName == "txn3") && !req.ba.IsSingleHeartbeatTxnRequest() {
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

	type opFn func(t *testing.T, name string) error

	execWorkloadTxn := func(t *testing.T, name string) error {
		tCtx := context.Background()
		txn := db.NewTxn(tCtx, name)
		vals := getInBatch(t, tCtx, txn, keyA, keyB)

		batch := txn.NewBatch()
		batch.Put(keyA, vals[0]-xferAmount)
		batch.Put(keyB, vals[1]+xferAmount)
		return txn.CommitInBatch(tCtx, batch)
	}

	execLeaseMover := func(t *testing.T, name string) error {
		desc, err := tc.LookupRange(keyB)
		assert.NoError(t, err)
		t.Logf("Transferring r%d lease to n%d", desc.RangeID, 1)
		assert.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))
		return nil
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

	runConcurrentOp := func(t *testing.T, name string, execOp opFn, wg *sync.WaitGroup, readyCh, doneCh chan struct{}, resultCh chan error) {
		defer wg.Done()
		if doneCh != nil {
			defer close(doneCh)
		}
		if !waitUntilReady(t, name, readyCh) {
			return
		}
		err := execOp(t, name)
		if resultCh != nil {
			resultCh <- err
		} else {
			assert.NoError(t, err)
		}
	}

	// The "basic" test case, allowing for multiple variants of txn2.
	runBasicTestCase := func(t *testing.T, txn2Ops opFn, txn2PutsAfterTxn1 bool) {
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
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", txn2Ops, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

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
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Once we have seen the write on txn1 to n2 that we will fail, txn2
				// can start.
				close(txn2Ready)
			}

			// 5. txn2->n1: Get(a) OR Put(a) -- Discovers txn1's locks, issues push request.
			// 6. txn2->n2: Get(b) OR Put(b)
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
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, leaseMoveComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.

			// -- NB: With ambiguous replay protection, txn1 should end here.
			// -- NB: Without ambiguous replay protection, txn1 would continue with:

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
			if txn2PutsAfterTxn1 && req.txnName == "txn2" && hasPut && cp == BeforeSending {
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
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: While ideally we would hope to see a successful commit without
		// error, without querying txn record/intents from the txn coordinator we
		// expect an AmbiguousResultError in this case (as outlined in #103817).
		validateTxnCommitAmbiguousError(t, err, "replay protection" /* reason */)
	}

	// The set of test cases that use the same request scheduling.
	basicVariants := []struct {
		name              string
		txn2Ops           opFn
		txn2PutsAfterTxn1 bool
	}{
		{
			name: "writer reader conflict",
			txn2Ops: func(t *testing.T, name string) error {
				tCtx := context.Background()

				// txn2 just performs a simple Get on a conflicting key, causing
				// it to issue a PushTxn for txn1 which will kick off recovery.
				txn := db.NewTxn(tCtx, name)
				_, err := txn.Get(tCtx, keyA)
				assert.NoError(t, err)
				assert.NoError(t, txn.Commit(ctx))
				return nil
			},
		},
		{
			name: "writer writer conflict",
			txn2Ops: func(t *testing.T, name string) error {
				tCtx := context.Background()

				// txn2 performs simple Puts on conflicting keys, causing it to issue a
				// PushTxn for txn1 which will kick off recovery.
				txn := db.NewTxn(tCtx, name)
				batch := txn.NewBatch()
				batch.Put(keyA, 0)
				batch.Put(keyB, 0)
				assert.NoError(t, txn.CommitInBatch(ctx, batch))
				t.Logf("txn2 finished here")
				return nil
			},
		},
		{
			name:              "workload conflict",
			txn2Ops:           execWorkloadTxn,
			txn2PutsAfterTxn1: true,
		},
	}

	for _, variant := range basicVariants {
		t.Run(variant.name, func(t *testing.T) {
			runBasicTestCase(t, variant.txn2Ops, variant.txn2PutsAfterTxn1)
		})
	}

	// Test cases with custom request scheduling.

	// NB: This test can be removed for versions >=24.1.
	t.Run("mixed version", func(t *testing.T) {
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
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execWorkloadTxn, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

		// KV Request sequencing.
		tMu.Lock()
		tMu.modifyReq = func(req *interceptedReq) (modified bool) {
			// In order to simulate a "mixed version" scenario, let's simply drop the
			// AmbiguousReplayProtection flag from the batch request, as if the
			// coordinator never sent it.
			if req.ba.AmbiguousReplayProtection {
				req.ba.AmbiguousReplayProtection = false
				return true
			}
			return false
		}
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			_, hasPut := req.ba.GetArg(kvpb.Put)

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n2: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n2: Put(b) -- Send the request, but pause before returning
			// the response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Once we have seen the write on txn1 to n2 that we will fail, txn2
				// can start.
				close(txn2Ready)
			}

			// 5. txn2->n1: Get(a) OR Put(a) -- Discovers txn1's locks, issues push request.
			// 6. txn2->n2: Get(b) OR Put(b)
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
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, leaseMoveComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.

			// -- NB: With ambiguous replay protection, txn1 should end here.
			// -- NB: Without ambiguous replay protection, txn1 would continue with:

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
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// While we expect an AmbiguousResultError if the AmbiguousReplayProtection
		// flag is sent/received, in the interests of ensuring that the client not
		// sending the flag does not produce unexpected behavior, we expect the
		// previous, incorrect TransactionStatusError with REASON_TXN_COMMITTED
		// to be returned.
		tErr := (*kvpb.TransactionStatusError)(nil)
		require.ErrorAsf(t, err, &tErr,
			"expected TransactionStatusError due to being already committed")
		require.Equalf(t, kvpb.TransactionStatusError_REASON_TXN_COMMITTED, tErr.Reason,
			"expected TransactionStatusError due to being already committed")
		require.Truef(t, errors.HasAssertionFailure(err),
			"expected AssertionFailedError due to sanity check on transaction already committed")
	})

	// The txn coordinator shouldn't respond with an incorrect retryable failure
	// based on a refresh as the transaction may have already been committed.
	t.Run("recovery before refresh fails", func(t *testing.T) {
		keyAPrime := roachpb.Key(encoding.EncodeBytesAscending(tablePrefix.Clone(), []byte("a'")))
		defer func() {
			_, err := db.Del(ctx, keyAPrime)
			require.NoError(t, err)
		}()

		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		leaseMoveReady := make(chan struct{})
		leaseMoveComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Operation functions.
		execTxn1 := func(t *testing.T, name string) error {
			tCtx := context.Background()
			txn := db.NewTxn(tCtx, name)
			vals := getInBatch(t, tCtx, txn, keyA, keyB, keyAPrime)

			batch := txn.NewBatch()
			batch.Put(keyA, vals[0]-xferAmount)
			batch.Put(keyB, vals[1]+xferAmount)
			return txn.CommitInBatch(tCtx, batch)
		}
		execTxn2 := func(t *testing.T, name string) error {
			tCtx := context.Background()
			txn := db.NewTxn(tCtx, name)

			// The intent from txn2 on a' will cause txn1's read refresh to fail.
			assert.NoError(t, txn.Put(tCtx, keyAPrime, 100))
			_ = getInBatch(t, tCtx, txn, keyA)
			assert.NoError(t, txn.Commit(tCtx))
			return nil
		}

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(3)
		go runConcurrentOp(t, "txn1", execTxn1, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execTxn2, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

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
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Once we have seen the write on txn1 to n2 that we will fail, we can
				// move the lease.
				close(txn2Ready)
			}

			// 5. txn2->n1: Put(a')
			// 6. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
			// 7. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 8. _->n1: RecoverTxn(txn1) -- Allow to proceed and finish. Since txn1
			// is in STAGING and has all of its writes, it is implicitly committed,
			// so the recovery will succeed in marking it explicitly committed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(leaseMoveReady)
			}

			// <transfer b's lease to n1>
			// <inject a network failure and finally allow (4) txn1->n2: Put(b) to
			// return with error>
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, leaseMoveComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.

			// -- NB: With ambiguous replay protection, txn1 should end here.
			// -- NB: Without ambiguous replay protection, txn1 would continue with:

			// 10. txn1->n1: Refresh(a)
			// 11. txn1->n1: Refresh(b,c) -- This fails due to txn2's intent on c.
			// Causes the transaction coordinator to return a retryable error,
			// although the transaction has been actually committed during recovery;
			// a highly problematic bug.

			// <Only allow txn2's successful push to return once txn1 has finished>
			// This way we can prevent txn1's intents from being resolved too early.
			if _, ok := req.ba.GetArg(kvpb.PushTxn); ok && cp == AfterSending &&
				resp != nil && resp.err == nil {
				req.pauseUntil(t, txn1Done, cp)
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: While ideally we would hope to see a successful commit without
		// error, without querying txn record/intents from the txn coordinator we
		// expect an AmbiguousResultError in this case (as outlined in #103817).
		validateTxnCommitAmbiguousError(t, err, "replay protection" /* reason */)
	})

	// The txn coordinator shouldn't respond with an incorrect retryable failure
	// based on a refresh as the transaction may yet be recovered/committed.
	t.Run("recovery after refresh fails", func(t *testing.T) {
		keyC := roachpb.Key(encoding.EncodeBytesAscending(tablePrefix.Clone(), []byte("c")))
		defer func() {
			_, err := db.Del(ctx, keyC)
			require.NoError(t, err)
		}()

		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		otherTxnsReady := make(chan struct{})
		leaseMoveReady := make(chan struct{})
		leaseMoveComplete := make(chan struct{})
		recoverComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		execTxn1 := func(t *testing.T, name string) error {
			tCtx := context.Background()
			txn := db.NewTxn(tCtx, name)
			vals := getInBatch(t, tCtx, txn, keyA, keyB, keyC)

			batch := txn.NewBatch()
			batch.Put(keyA, vals[0]-xferAmount)
			batch.Put(keyB, vals[1]+xferAmount)
			return txn.CommitInBatch(tCtx, batch)
		}
		execOtherTxns := func(t *testing.T, name string) error {
			tCtx := context.Background()

			// The intent from txn3 will cause txn1's read refresh to fail.
			txn3 := db.NewTxn(tCtx, "txn3")
			batch := txn3.NewBatch()
			batch.Put(keyC, 100)
			assert.NoError(t, txn3.CommitInBatch(tCtx, batch))

			txn2 := db.NewTxn(tCtx, "txn2")
			vals := getInBatch(t, tCtx, txn2, keyA, keyB)

			batch = txn2.NewBatch()
			batch.Put(keyA, vals[0]-xferAmount)
			batch.Put(keyB, vals[1]+xferAmount)
			assert.NoError(t, txn2.CommitInBatch(tCtx, batch))
			return nil
		}

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(3)
		go runConcurrentOp(t, "txn1", execTxn1, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2/txn3", execOtherTxns, &wg, otherTxnsReady, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

		// KV Request sequencing.
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			_, hasPut := req.ba.GetArg(kvpb.Put)

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n2: Get(b), Get(c)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n2: Put(b) -- Send the request, but pause before returning the
			// response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Once we have seen the write on txn1 to n2 that we will fail, txn2/txn3 can start.
				close(otherTxnsReady)
			}

			// 5. txn3->n2: Put(c), EndTxn(parallel commit) -- Hits 1PC fast path.
			// 6. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
			// 7. txn2->n2: Get(b)
			// 8. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 9. _->n1: RecoverTxn(txn1) -- Before sending, pause the request so we
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
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, leaseMoveComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 10. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.

			// -- NB: With ambiguous replay protection, txn1 should end here.
			// -- NB: Without ambiguous replay protection, txn1 would continue with:

			// 11. txn1->n1: Refresh(a)
			// 12. txn1->n1: Refresh(b,c) -- This fails due to txn3's write on c.
			// Causes the transaction coordinator to return a retryable error,
			// although the transaction could be actually committed during recovery;
			// a highly problematic bug.
			// <allow (9) RecoverTxn(txn1) to proceed and finish> -- because txn1
			// is in STAGING and has all of its writes, it is implicitly committed,
			// so the recovery will succeed in marking it explicitly committed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
				// The RecoverTxn operation should be evaluated after txn1 completes,
				// in this case with a problematic retryable error.
				req.pauseUntil(t, txn1Done, cp)
			}
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(recoverComplete)
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
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: While ideally we would hope to see a successful commit without
		// error, without querying txn record/intents from the txn coordinator we
		// expect an AmbiguousResultError in this case (as outlined in #103817).
		validateTxnCommitAmbiguousError(t, err, "replay protection" /* reason */)
	})

	// This test is primarily included for completeness, in order to ensure the
	// same behavior regardless of if the recovery occurs before or after the
	// lease transfer.
	t.Run("recovery after transfer lease", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		leaseMoveReady := make(chan struct{})
		leaseMoveComplete := make(chan struct{})
		recoverComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(3)
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execWorkloadTxn, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

		// KV Request sequencing.
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			_, hasPut := req.ba.GetArg(kvpb.Put)

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n2: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n2: Put(b) -- Send the request, but pause before returning the
			// response so we can inject network failure.
			// <transfer b's lease to n1>
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				close(leaseMoveReady)
				req.pauseUntil(t, leaseMoveComplete, cp)
				close(txn2Ready)
			}

			// 5. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
			// 6. txn2->n1: Get(b)
			// 7. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 8. _->n1: RecoverTxn(txn1) -- Recovery should mark txn1 committed, but
			// intent resolution on txn1 needs to be paused until after txn1 finishes.
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				close(recoverComplete)
			}

			// <inject a network failure and finally allow (4) txn1->n2: Put(b) to
			// return with error>
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, recoverComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry on new leaseholder sees new lease start
			// timestamp, and attempts to evaluate it as an idempotent replay, but at
			// a higher timestamp, which breaks idempotency due to being on commit.

			// -- NB: With ambiguous replay protection, txn1 should end here.
			// -- NB: Without ambiguous replay protection, txn1 would continue with:

			// 10. txn1->n1: Refresh(a)
			// 11. txn1->n1: Refresh(b)
			// 12. txn1->n1: EndTxn(commit) -- Recovery has already completed, so this
			// request fails with "transaction unexpectedly committed".

			// <allow intent resolution after (8) recovery so txn2 can continue>
			// If the intent on (b) were resolved and txn2 could grab the lock prior
			// to txn1's retry of the Put(b), the retry will cause a PushTxn to txn2.
			// Given that the recovery at (8) has already completed, a PushTxn
			// request where the pusher is a committed transaction results in an
			// "already committed" TransactionStatusError from the txnwait queue.
			// While this still results in AmbiguousResultError from the DistSender,
			// the reason will be distinct; as such we pause the intent resolution.
			if riReq, ok := req.ba.GetArg(kvpb.ResolveIntent); ok && riReq.Header().Key.Equal(keyB) && cp == BeforeSending {
				req.pauseUntil(t, txn1Done, cp)
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: While ideally we would hope to see a successful commit without
		// error, without querying txn record/intents from the txn coordinator we
		// expect an AmbiguousResultError in this case (as outlined in #103817).
		validateTxnCommitAmbiguousError(t, err, "replay protection" /* reason */)
	})

	// When a retried write happens after another txn's intent already exists on
	// the key, we expect to see a WriteTooOld error propagate an ambiguous error.
	t.Run("retry sees other intent", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		leaseMoveReady := make(chan struct{})
		leaseMoveComplete := make(chan struct{})
		txn2ETReady := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(3)
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execWorkloadTxn, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)
		go runConcurrentOp(t, "lease mover", execLeaseMover, &wg, leaseMoveReady, leaseMoveComplete, nil /* resultCh */)

		// KV Request sequencing.
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			_, hasPut := req.ba.GetArg(kvpb.Put)

			// These conditions are checked in order of expected operations of the test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n2: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n2: Put(b) -- Send the request, but pause before returning the
			// response so we can inject network failure.
			// <transfer b's lease to n1>
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				close(leaseMoveReady)
				req.pauseUntil(t, leaseMoveComplete, cp)
				close(txn2Ready)
			}

			// 5. txn2->n1: Get(a) -- Discovers txn1's locks, issues push request.
			// 6. txn2->n1: Get(b)
			// 7. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 8. _->n1: RecoverTxn(txn1) -- Recovery should mark txn1 committed.
			// 9. txn2->n1: Put(a), EndTxn(parallel commit) -- Writes intent.
			// 10. txn2->n1: Put(b)
			// 11. txn2->n1: EndTxn(commit) -- Happens asynchronously, so txn2's
			// coordinator has already returned success, but we want recovery of txn1
			// and txn1's retry to complete before txn2's intents have been resolved.
			if req.txnName == "txn2" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				close(txn2ETReady)
			}

			// <inject a network failure and finally allow (4) txn1->n2: Put(b) to
			// return with error>
			if req.txnName == "txn1" && hasPut && req.toNodeID == 2 && cp == AfterSending {
				// Hold the operation open until we are ready to retry on the new
				// replica, after which we will return the injected failure.
				req.pauseUntil(t, txn2ETReady, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry sees intent, so returns a WriteTooOld
			// error. Since the transaction had an earlier ambiguous failure on a
			// batch with a commit, it should propagate the ambiguous error.

			// -- NB: Ambiguous replay protection is not required, and should not
			// come into play here.

			// <allow txn2 to complete>
			if req.txnName == "txn2" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				req.pauseUntil(t, txn1Done, cp)
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: It is likely not possible to eliminate ambiguity in this case, as
		// the original intents were already cleaned up.
		validateTxnCommitAmbiguousError(t, err, "WriteTooOld" /* reason */)
	})

	// When a retried write happens after our txn's intent has already been
	// resolved post-recovery, even at the same timestamp we expect to see a
	// WriteTooOld error propagate an ambiguous error.
	t.Run("recovery before retry at same timestamp", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		recoverComplete := make(chan struct{})
		resolveIntentComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Place second range on n1 (same as first).
		secondRange := tc.LookupRangeOrFatal(t, keyB)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(0))
		requireRangeLease(t, secondRange, 0)

		// Operation functions.
		execTxn2 := func(t *testing.T, name string) error {
			tCtx := context.Background()

			// txn2 just performs a simple GetForUpdate on a conflicting key, causing
			// it to issue a PushTxn for txn1 which will kick off recovery.
			txn := db.NewTxn(tCtx, name)
			_ = getInBatch(t, tCtx, txn, keyB)
			assert.NoError(t, txn.Commit(ctx))
			return nil
		}

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(2)
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execTxn2, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)

		// KV Request sequencing.
		var txn1KeyBWriteCount int64
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			putReq, hasPut := req.ba.GetArg(kvpb.Put)
			var keyBWriteCount int64 = -1

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n1: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n1: Put(b) -- Send the request, but pause before returning
			// the response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && putReq.Header().Key.Equal(keyB) && cp == AfterSending {
				keyBWriteCount = atomic.AddInt64(&txn1KeyBWriteCount, 1)
				if keyBWriteCount == 1 {
					close(txn2Ready)
				}
			}

			// 5. txn2->n1: Get(b) -- Discovers txn1's locks, issues push request.
			// 6. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 7. _->n1: RecoverTxn(txn1) -- Allow to proceed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(recoverComplete)
			}

			// 8. _->n1: ResolveIntent(txn1, b)
			if riReq, ok := req.ba.GetArg(kvpb.ResolveIntent); ok && riReq.Header().Key.Equal(keyB) && cp == AfterSending {
				t.Logf("%s - complete", req.prefix)
				close(resolveIntentComplete)
			}

			// <inject a network failure and finally allow (4) txn1->n1: Put(b) to
			// return with error>
			if req.txnName == "txn1" && keyBWriteCount == 1 && cp == AfterSending {
				// Hold the operation open until we are ready to retry, after which we
				// will return the injected failure.
				req.pauseUntil(t, resolveIntentComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 9. txn1->n1: Put(b) -- Retry gets WriteTooOld and cannot perform a
			// serverside refresh as reads were already returned. Fails, and should
			// propagate the ambiguous error.

			// -- NB: Ambiguous replay protection is not required, and should not
			// come into play here.

			// <allow txn2's Puts to execute>
			if req.txnName == "txn2" && hasPut && cp == BeforeSending {
				<-txn1Done
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: It is likely not possible to eliminate ambiguity in this case, as
		// the original intents were already cleaned up.
		validateTxnCommitAmbiguousError(t, err, "WriteTooOld" /* reason */)
	})

	// This test is included for completeness, but tests expected behavior;
	// when a retried write happens successfully at the same timestamp, and no
	// refresh is required, the explicit commit can happen asynchronously and the
	// txn coordinator can return early, reporting a successful commit.
	// When this occurs, even if the recovery happens before the async EndTxn,
	// it is expected behavior for the EndTxn to encounter an already committed
	// txn record, and this is not treated as error.
	t.Run("recovery after retry at same timestamp", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		putRetryReady := make(chan struct{})
		receivedFinalET := make(chan struct{})
		recoverComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Place second range on n1 (same as first).
		secondRange := tc.LookupRangeOrFatal(t, keyB)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(0))
		requireRangeLease(t, secondRange, 0)

		// Operation functions.
		execTxn2 := func(t *testing.T, name string) error {
			tCtx := context.Background()

			// txn2 just performs a simple GetForUpdate on a conflicting key, causing
			// it to issue a PushTxn for txn1 which will kick off recovery.
			txn := db.NewTxn(tCtx, name)
			_ = getInBatch(t, tCtx, txn, keyB)
			assert.NoError(t, txn.Commit(ctx))
			return nil
		}

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(2)
		go runConcurrentOp(t, "txn1", execWorkloadTxn, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execTxn2, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)

		// KV Request sequencing.
		var txn1KeyBWriteCount int64
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			putReq, hasPut := req.ba.GetArg(kvpb.Put)
			var keyBWriteCount int64 = -1

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Get(a)
			// 2. txn1->n1: Get(b)
			// 3. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 4. txn1->n1: Put(b) -- Send the request, but pause before returning
			// the response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && putReq.Header().Key.Equal(keyB) && cp == AfterSending {
				keyBWriteCount = atomic.AddInt64(&txn1KeyBWriteCount, 1)
				if keyBWriteCount == 1 {
					close(txn2Ready)
				}
			}

			// 5. txn2->n1: Get(b) -- Discovers txn1's locks, issues push request.
			// 6. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 7. _->n1: RecoverTxn(txn1) -- Before sending, pause the request so we
			// can ensure it gets evaluated after txn1 retries, but before its final
			// EndTxn.
			if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
				close(putRetryReady)
			}

			// <inject a network failure and finally allow (4) txn1->n1: Put(b) to
			// return with error>
			if req.txnName == "txn1" && keyBWriteCount == 1 && cp == AfterSending {
				// Hold the operation open until we are ready to retry, after which we
				// will return the injected failure.
				req.pauseUntil(t, putRetryReady, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 8. txn1->n1: Put(b) -- Retry gets evaluated as idempotent replay and
			// correctly succeeds. Ambiguous replay protection, though enabled,
			// should not cause the retried write to error.

			// -- NB: In this case, txn1 returns without error here, as there is no
			// need to refresh and the final EndTxn can be split off and run
			// asynchronously.

			// 9. txn1->n1: EndTxn(commit) -- Before sending, pause the request so
			// that we can allow (8) RecoverTxn(txn1) to proceed, simulating a race
			// in which the recovery wins.
			if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				close(receivedFinalET)
			}

			// <allow (7) RecoverTxn(txn1) to proceed and finish> -- because txn1
			// is in STAGING and has all of its writes, it is implicitly committed,
			// so the recovery will succeed in marking it explicitly committed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == BeforeSending {
				req.pauseUntilFirst(t, receivedFinalET, txn1Done, cp)
			}
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(recoverComplete)
			}

			// <allow (9) EndTxn(commit) to proceed and execute> -- even though the
			// recovery won, this is allowed. See makeTxnCommitExplicitLocked(..).
			if req.txnName == "txn1" && req.ba.IsSingleEndTxnRequest() && cp == BeforeSending {
				req.pauseUntil(t, recoverComplete, cp)
			}

			// <allow txn2's Puts to execute>
			if req.txnName == "txn2" && hasPut && cp == BeforeSending {
				<-txn1Done
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		require.NoErrorf(t, err, "expected txn1 to succeed")
	})

	// When a retried write happens after our txn's intent has already been
	// resolved post-recovery, it should not be able to perform a serverside
	// refresh to "handle" the WriteTooOld error as it may already be committed.
	t.Run("recovery before retry with serverside refresh", func(t *testing.T) {
		defer initSubTest(t)()

		// Checkpoints in test.
		txn1Ready := make(chan struct{})
		txn2Ready := make(chan struct{})
		recoverComplete := make(chan struct{})
		resolveIntentComplete := make(chan struct{})
		txn1Done := make(chan struct{})

		// Final result.
		txn1ResultCh := make(chan error, 1)

		// Place second range on n1 (same as first).
		secondRange := tc.LookupRangeOrFatal(t, keyB)
		tc.TransferRangeLeaseOrFatal(t, secondRange, tc.Target(0))
		requireRangeLease(t, secondRange, 0)

		// Operation functions.
		execTxn1 := func(t *testing.T, name string) error {
			tCtx := context.Background()

			// txn2 just performs a simple GetForUpdate on a conflicting key, causing
			// it to issue a PushTxn for txn1 which will kick off recovery.
			txn := db.NewTxn(tCtx, name)
			batch := txn.NewBatch()
			batch.Put(keyA, 100)
			batch.Put(keyB, 100)
			return txn.CommitInBatch(tCtx, batch)
		}
		execTxn2 := func(t *testing.T, name string) error {
			tCtx := context.Background()

			// txn2 just performs a simple GetForUpdate on a conflicting key, causing
			// it to issue a PushTxn for txn1 which will kick off recovery.
			txn := db.NewTxn(tCtx, name)
			_ = getInBatch(t, tCtx, txn, keyB)
			assert.NoError(t, txn.Commit(ctx))
			return nil
		}

		// Concurrent transactions.
		var wg sync.WaitGroup
		wg.Add(2)
		go runConcurrentOp(t, "txn1", execTxn1, &wg, txn1Ready, txn1Done, txn1ResultCh)
		go runConcurrentOp(t, "txn2", execTxn2, &wg, txn2Ready, nil /* doneCh */, nil /* resultCh */)

		// KV Request sequencing.
		var txn1KeyBWriteCount int64
		var txn1KeyBResolveIntentCount int64
		tMu.Lock()
		tMu.maybeWait = func(cp InterceptPoint, req *interceptedReq, resp *interceptedResp) (override error) {
			putReq, hasPut := req.ba.GetArg(kvpb.Put)
			var keyBWriteCount int64 = -1
			var keyBResolveIntentCount int64 = -1

			// These conditions are checked in order of expected operations of the
			// test.

			// 1. txn1->n1: Put(a), EndTxn(parallel commit) -- Puts txn1 in STAGING.
			// 2. txn1->n1: Put(b) -- Send the request, but pause before returning
			// the response so we can inject network failure.
			if req.txnName == "txn1" && hasPut && putReq.Header().Key.Equal(keyB) && cp == AfterSending {
				keyBWriteCount = atomic.AddInt64(&txn1KeyBWriteCount, 1)
				if keyBWriteCount == 1 {
					close(txn2Ready)
				}
			}

			// 3. txn2->n1: Get(b) -- Discovers txn1's locks, issues push request.
			// 4. _->n1: PushTxn(txn2->txn1) -- Discovers txn1 in STAGING and starts
			// recovery.
			// 5. _->n1: RecoverTxn(txn1) -- Allow to proceed.
			if req.ba.IsSingleRecoverTxnRequest() && cp == AfterSending {
				t.Logf("%s - complete, resp={%s}", req.prefix, resp)
				close(recoverComplete)
			}

			// 6. _->n1: ResolveIntent(txn1, b)
			if riReq, ok := req.ba.GetArg(kvpb.ResolveIntent); ok && riReq.Header().Key.Equal(keyB) && cp == AfterSending {
				keyBResolveIntentCount = atomic.AddInt64(&txn1KeyBResolveIntentCount, 1)
				if keyBResolveIntentCount == 1 {
					t.Logf("%s - complete", req.prefix)
					close(resolveIntentComplete)
				}
			}

			// <inject a network failure and finally allow (2) txn1->n1: Put(b) to
			// return with error>
			if req.txnName == "txn1" && keyBWriteCount == 1 && cp == AfterSending {
				// Hold the operation open until we are ready to retry, after which we
				// will return the injected failure.
				req.pauseUntil(t, resolveIntentComplete, cp)
				t.Logf("%s - injected RPC error", req.prefix)
				return grpcstatus.Errorf(codes.Unavailable, "response jammed on n%d<-n%d", req.fromNodeID, req.toNodeID)
			}

			// 7. txn1->n1: Put(b) -- Retry gets WriteTooOld; with ambiguous replay
			// protection enabled, it should not be able to perform a serverside
			// refresh. Fails, and should propagate the ambiguous error.

			// -- NB: Without ambiguous replay protection, txn1 would continue with:

			// 8. txn1->n1: EndTxn(commit) -- Would get "transaction unexpectedly
			// committed" due to the recovery completing first.

			// <allow txn2's Puts to execute>
			if req.txnName == "txn2" && hasPut && cp == BeforeSending {
				<-txn1Done
			}

			return nil
		}
		tMu.Unlock()

		// Start test, await concurrent operations and validate results.
		close(txn1Ready)
		err := <-txn1ResultCh
		t.Logf("txn1 completed with err: %v", err)
		wg.Wait()

		// NB: It is likely not possible to eliminate ambiguity in this case, as
		// the original intents were already cleaned up.
		validateTxnCommitAmbiguousError(t, err, "WriteTooOld" /* reason */)
	})
}
