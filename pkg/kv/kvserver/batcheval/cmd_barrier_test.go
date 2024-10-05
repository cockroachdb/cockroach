// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestBarrierEval tests basic Barrier evaluation.
func TestBarrierEval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	start := roachpb.Key("a")
	end := roachpb.Key("b")

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	ts := clock.Now()
	evalCtx := (&batcheval.MockEvalCtx{Clock: clock}).EvalContext()

	testutils.RunTrueAndFalse(t, "WithLeaseAppliedIndex", func(t *testing.T, withLAI bool) {
		resp := kvpb.BarrierResponse{}
		res, err := batcheval.Barrier(ctx, nil, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Args: &kvpb.BarrierRequest{
				RequestHeader:         kvpb.RequestHeader{Key: start, EndKey: end},
				WithLeaseAppliedIndex: withLAI,
			},
		}, &resp)
		require.NoError(t, err)

		require.Equal(t, result.Result{
			Local: result.LocalResult{
				PopulateBarrierResponse: withLAI,
			},
		}, res)

		// Ignore the logical timestamp component, which is incremented per reading.
		resp.Timestamp.Logical = 0

		require.Equal(t, kvpb.BarrierResponse{
			Timestamp: ts,
		}, resp)
	})
}

// TestBarrier is an integration test for Barrier. It tests that it processes
// the request and response properly, within a single range and across multiple
// ranges.
func TestBarrier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Set up a test server.
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ssrv := srv.StorageLayer()
	tsrv := srv.ApplicationLayer()
	srv = nil // prevent direct access, use system or tenant as appropriate

	store, err := ssrv.GetStores().(*kvserver.Stores).GetStore(ssrv.GetFirstStoreID())
	require.NoError(t, err)
	sender := kvDB.NonTransactionalSender()

	// We'll use /a to /z as our keyspace, and split off a range at /x.
	prefix := tsrv.Codec().TenantPrefix()
	_, _, err = ssrv.SplitRange(append(prefix, []byte("/x")...))
	require.NoError(t, err)

	// Send Barrier request with/without LeaseAppliedIndex, and within a single
	// range or across multiple ranges.
	testutils.RunTrueAndFalse(t, "WithLeaseAppliedIndex", func(t *testing.T, withLAI bool) {
		testutils.RunTrueAndFalse(t, "crossRange", func(t *testing.T, crossRange bool) {
			start := append(prefix, []byte("/a")...)
			end := append(prefix, []byte("/b")...)
			if crossRange {
				end = append(prefix, []byte("/z")...)
			}
			repl := store.LookupReplica(roachpb.RKey(start))

			tsBefore := tsrv.Clock().Now()
			laiBefore := repl.GetLeaseAppliedIndex()
			req := kvpb.BarrierRequest{
				RequestHeader:         kvpb.RequestHeader{Key: start, EndKey: end},
				WithLeaseAppliedIndex: withLAI,
			}
			respI, pErr := kv.SendWrapped(ctx, sender, &req)

			// WithLeaseAppliedIndex should return RangeKeyMismatchError when across
			// multiple ranges.
			if withLAI && crossRange {
				require.Error(t, pErr.GoError())
				require.IsType(t, &kvpb.RangeKeyMismatchError{}, pErr.GoError())
				return
			}

			require.NoError(t, pErr.GoError())
			resp, ok := respI.(*kvpb.BarrierResponse)
			require.True(t, ok)

			// The timestamp must be after the request was sent.
			require.True(t, tsBefore.LessEq(resp.Timestamp))

			// If WithLeaseAppliedIndex is set, it also returns the LAI and range
			// descriptor.
			if withLAI {
				require.GreaterOrEqual(t, resp.LeaseAppliedIndex, laiBefore)
				require.GreaterOrEqual(t, repl.GetLeaseAppliedIndex(), resp.LeaseAppliedIndex)
				require.Equal(t, *repl.Desc(), resp.RangeDesc)
			} else {
				require.Zero(t, resp.LeaseAppliedIndex)
				require.Zero(t, resp.RangeDesc)
			}
		})
	})
}

// TestBarrierLatches tests Barrier latch interactions. Specifically, that it
// waits for in-flight requests to complete, but that it does not block later
// requests.
func TestBarrierLatches(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t) // too slow, times out

	// Use a timeout, to prevent blocking indefinitely if something goes wrong.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// We'll do reads and writes to two separate keys, with a range split in
	// between. These keys will be under the tenant prefix.
	readSuffix := roachpb.Key("/read")
	splitSuffix := roachpb.Key("/split")
	writeSuffix := roachpb.Key("/write")

	// Set up a request evaluation filter which will block Gets to /read and Puts
	// to /write. These will signal that they're blocked via blockedC, and unblock
	// when unblockC is closed.
	//
	// Unfortunately, we can't use a magic context to specify which requests to
	// block, since this does not work with external process tenants which may be
	// randomly enabled. We therefore have to match the actual keys.
	blockedC := make(chan struct{}, 10)
	unblockC := make(chan struct{})

	evalFilter := func(args kvserverbase.FilterArgs) *kvpb.Error {
		var shouldBlock bool
		if key, err := keys.StripTenantPrefix(args.Req.Header().Key); err == nil {
			if args.Req.Method() == kvpb.Get && bytes.Equal(key, readSuffix) {
				shouldBlock = true
			}
			if args.Req.Method() == kvpb.Put && bytes.Equal(key, writeSuffix) {
				shouldBlock = true
			}
		}
		if shouldBlock {
			// Notify callers that we're blocking.
			select {
			case blockedC <- struct{}{}:
				t.Logf("blocked %s", args.Req)
			case <-ctx.Done():
				return kvpb.NewError(ctx.Err())
			}
			// Wait to unblock.
			select {
			case <-unblockC:
				t.Logf("unblocked %s", args.Req)
			case <-ctx.Done():
				return kvpb.NewError(ctx.Err())
			}
		}
		return nil
	}

	// Set up a test server.
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: evalFilter,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	ssrv := srv.StorageLayer()
	tsrv := srv.ApplicationLayer()
	srv = nil // prevent direct access, use system or tenant as appropriate

	db := tsrv.DB()
	store, err := ssrv.GetStores().(*kvserver.Stores).GetStore(ssrv.GetFirstStoreID())
	require.NoError(t, err)
	_ = store

	// Determine the tenant prefix and keys.
	prefix := tsrv.Codec().TenantPrefix()
	readKey := append(prefix, readSuffix...)
	splitKey := append(prefix, splitSuffix...)
	writeKey := append(prefix, writeSuffix...)

	// Set up helpers to run barriers, both sync and async.
	barrier := func(ctx context.Context, start, end roachpb.Key, withLAI bool) (err error) {
		if withLAI {
			_, _, err = db.BarrierWithLAI(ctx, start, end)
		} else {
			_, err = db.Barrier(ctx, start, end)
		}
		return
	}

	barrierAsync := func(ctx context.Context, start, end roachpb.Key, withLAI bool) <-chan error {
		errC := make(chan error, 1)
		go func() {
			errC <- barrier(ctx, start, end, withLAI)
		}()
		return errC
	}

	// Split off a range at /split, to test cross-range barriers.
	_, _, err = ssrv.SplitRange(splitKey)
	require.NoError(t, err)

	// Spawn read and write requests, and wait for them to block.
	go func() {
		_ = db.Put(ctx, writeKey, "value")
	}()
	go func() {
		_, _ = db.Get(ctx, readKey)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-blockedC:
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}

	// Barriers should not conflict outside of these keys.
	require.NoError(t, barrier(ctx, readKey.Next(), splitKey, true /* withLAI */))
	require.NoError(t, barrier(ctx, splitKey, writeKey, true /* withLAI */))
	require.Error(t, barrier(ctx, readKey.Next(), writeKey, true /* withLAI */)) // can't span ranges
	require.NoError(t, barrier(ctx, readKey.Next(), writeKey, false /* withLAI */))

	// Barriers should not conflict with read requests.
	require.NoError(t, barrier(ctx, readKey, readKey.Next(), true /* withLAI */))

	// Barriers should conflict with write requests. We send off two barriers: one
	// WithLAI in a single range, and another across ranges. Neither of these
	// should return in a second.
	withLAIC := barrierAsync(ctx, splitKey, writeKey.Next(), true /* withLAI */)
	withoutLAIC := barrierAsync(ctx, readKey, writeKey.Next(), false /* withLAI */)
	select {
	case err := <-withLAIC:
		t.Fatalf("WithLAI=true barrier returned prematurely: %v", err)
	case err := <-withoutLAIC:
		t.Fatalf("WithLAI=false barrier returned prematurely: %v", err)
	case <-time.After(time.Second):
	}

	// While the barriers are blocked, later overlapping requests should be able
	// to proceed and evaluate below them.
	require.NoError(t, db.Put(ctx, splitKey, "value"))
	_, err = db.Get(ctx, splitKey)
	require.NoError(t, err)

	// Unblock the requests. This should now unblock the barriers as well.
	close(unblockC)

	select {
	case err := <-withLAIC:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("WithLAI=true barrier did not return")
	}

	select {
	case err := <-withoutLAIC:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("WithLAI=false barrier did not return")
	}
}
