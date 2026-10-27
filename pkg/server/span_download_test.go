// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDownloadSpanFailFast covers the contracts of the per-node fan-out in
// systemStatusServer.DownloadSpan:
//
//  1. A node retries its own transient failure perNodeMaxDownloadAttempts times
//     before giving up.
//
//  2. Once a node exhausts its retries and the fan-out has run for at least
//     stopOnErrorMinFanoutRuntime, the coordinator cancels the remaining
//     nodes' work and returns that node's error in the response's Errors with
//     its type intact.
//
//  3. If a node exhausts its retries before stopOnErrorMinFanoutRuntime has
//     elapsed, the other nodes keep working until it does.
//
// The test injects a permanent InsufficientSpaceError on one node (the victim).
// A second node (the sibling) waits for the victim to exhaust its retries and
// then for its own download context to be cancelled, up to siblingWait, and
// records whether it was. The subtests share one cluster; each installs its own
// failFastRun for the knob to use.
func TestDownloadSpanFailFast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var victim, sibling roachpb.NodeID
	var curRun atomic.Pointer[failFastRun]

	knobs := &TestingKnobs{
		DownloadSpanLocalError: func(ctx context.Context, nodeID roachpb.NodeID) error {
			run := curRun.Load()
			if run == nil {
				return nil
			}
			switch nodeID {
			case victim:
				if run.victimAttempts.Add(1) == int64(perNodeMaxDownloadAttempts) {
					close(run.victimExhausted)
				}
				return &kvpb.InsufficientSpaceError{StoreID: 1, Op: "ingest data", Required: 0.05}
			case sibling:
				if run.siblingDone.Load() {
					return nil
				}
				defer run.siblingDone.Store(true)
				select {
				case <-run.victimExhausted:
				case <-ctx.Done():
					t.Errorf("sibling node n%d cancelled before the victim exhausted its retries", nodeID)
					return ctx.Err()
				}
				select {
				case <-ctx.Done():
					run.siblingCanceled.Store(true)
					return ctx.Err()
				case <-time.After(run.siblingWait):
					return nil
				}
			default:
				return nil
			}
		},
	}

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs:             base.TestingKnobs{Server: knobs},
		},
	})
	defer tc.Stopper().Stop(ctx)

	victim = tc.Server(1).NodeID()
	sibling = tc.Server(2).NodeID()

	emptyTable := keys.SystemSQLCodec.TablePrefix(9999)
	span := roachpb.Span{Key: emptyTable, EndKey: emptyTable.PrefixEnd()}

	for _, c := range []struct {
		name       string
		minRuntime time.Duration
		// siblingWait bounds how long the sibling waits for cancellation after
		// the victim gives up. When cancellation is expected it is a generous
		// upper bound that a passing run never reaches. When it is not, every
		// passing run waits it out, so it is short: the coordinator cancels
		// within milliseconds of the victim's error if it is going to.
		siblingWait    time.Duration
		expectCanceled bool
	}{
		{
			name:           "min-runtime-elapsed",
			minRuntime:     0,
			siblingWait:    45 * time.Second,
			expectCanceled: true,
		},
		{
			name:           "min-runtime-pending",
			minRuntime:     time.Hour,
			siblingWait:    time.Second,
			expectCanceled: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func(old time.Duration) { stopOnErrorMinFanoutRuntime = old }(
				stopOnErrorMinFanoutRuntime)
			stopOnErrorMinFanoutRuntime = c.minRuntime

			run := &failFastRun{victimExhausted: make(chan struct{}), siblingWait: c.siblingWait}
			curRun.Store(run)
			defer curRun.Store(nil)

			// An empty NodeID triggers the all-nodes fan-out. The status client
			// authenticates as root, which satisfies DownloadSpan's REPAIRCLUSTER
			// check.
			resp, err := tc.Server(0).GetStatusClient(t).DownloadSpan(ctx,
				&serverpb.DownloadSpanRequest{Spans: []roachpb.Span{span}})

			require.NoError(t, err)
			encodedErr, ok := resp.Errors[victim]
			require.True(t, ok, "expected an error for victim node n%d, got %v", victim, resp.Errors)
			victimErr := errors.DecodeError(ctx, encodedErr)
			require.True(t, errors.HasType(victimErr, &kvpb.InsufficientSpaceError{}),
				"expected InsufficientSpaceError, got %+v", victimErr)

			require.Equal(t, int64(perNodeMaxDownloadAttempts), run.victimAttempts.Load(),
				"victim node n%d should retry %d times", victim, perNodeMaxDownloadAttempts)

			if !c.expectCanceled {
				// The fan-out returned only after every node finished, so the
				// sibling is done and did not see a cancellation.
				require.True(t, run.siblingDone.Load(), "sibling node n%d did not finish", sibling)
				require.False(t, run.siblingCanceled.Load(),
					"sibling node n%d was cancelled before the minimum fan-out runtime", sibling)
				_, ok := resp.Errors[sibling]
				require.False(t, ok, "unexpected error for sibling node n%d: %v", sibling, resp.Errors)
				return
			}
			// The fan-out returns as soon as it cancels, so wait for the sibling
			// to observe the cancellation.
			testutils.SucceedsSoon(t, func() error {
				if !run.siblingDone.Load() {
					return errors.Newf("sibling node n%d still running", sibling)
				}
				return nil
			})
			require.True(t, run.siblingCanceled.Load(),
				"sibling node n%d was not cancelled after the victim exhausted retries", sibling)
		})
	}
}

// failFastRun is the per-subtest state of TestDownloadSpanFailFast, read and
// written by the DownloadSpanLocalError knob.
type failFastRun struct {
	// victimExhausted is closed once the victim makes its last attempt.
	victimExhausted chan struct{}
	siblingWait     time.Duration

	victimAttempts  atomic.Int64
	siblingDone     atomic.Bool
	siblingCanceled atomic.Bool
}
