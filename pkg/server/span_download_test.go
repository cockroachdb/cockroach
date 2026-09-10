// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDownloadSpanFailFast covers two related contracts of the per-node fan-out
// in systemStatusServer.DownloadSpan:
//
//  1. A node retries its own transient failure perNodeMaxDownloadAttempts times
//     before giving up.
//
//  2. Once a node exhausts its retries, the fan-out fails fast: the coordinator
//     cancels the remaining nodes' work and returns the error to the caller rather
//     than letting the other nodes keep running.
//
// The test injects a permanent failure on one node (the victim) and blocks a
// second node (the blocker) until its download context is cancelled. The victim
// must attempt perNodeMaxDownloadAttempts times, DownloadSpan must return a
// non-nil error, and the blocker must observe cancellation — proving sibling node
// work is actually shut down.
func TestDownloadSpanFailFast(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var mu syncutil.Mutex
	attempts := make(map[roachpb.NodeID]int)

	var victim, blocker roachpb.NodeID

	// blockerCanceled is signaled when the blocker node observes cancellation of
	// its download context. Buffered so the injected hook never blocks and safe
	// against a spurious second invocation.
	blockerCanceled := make(chan struct{}, 1)

	knobs := &TestingKnobs{
		DownloadSpanLocalError: func(ctx context.Context, nodeID roachpb.NodeID) error {
			switch nodeID {
			case victim:
				mu.Lock()
				attempts[nodeID]++
				mu.Unlock()
				return errors.Newf("injected transient download error on n%d", nodeID)
			case blocker:
				<-ctx.Done()
				select {
				case blockerCanceled <- struct{}{}:
				default:
				}
				return ctx.Err()
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
	blocker = tc.Server(2).NodeID()

	emptyTable := keys.SystemSQLCodec.TablePrefix(9999)
	span := roachpb.Span{Key: emptyTable, EndKey: emptyTable.PrefixEnd()}

	// An empty NodeID triggers the all-nodes fan-out. The status client
	// authenticates as root, which satisfies DownloadSpan's REPAIRCLUSTER check.
	resp, err := tc.Server(0).GetStatusClient(t).DownloadSpan(ctx, &serverpb.DownloadSpanRequest{
		Spans: []roachpb.Span{span},
	})

	require.Error(t, err)
	require.Nil(t, resp)

	mu.Lock()
	victimAttempts := attempts[victim]
	mu.Unlock()
	require.Equal(t, perNodeMaxDownloadAttempts, victimAttempts,
		"victim node n%d should retry %d times, got %d attempts",
		victim, perNodeMaxDownloadAttempts, victimAttempts)

	select {
	case <-blockerCanceled:
	case <-time.After(45 * time.Second):
		t.Fatalf("blocker node n%d was not cancelled after the victim exhausted retries", blocker)
	}
}
