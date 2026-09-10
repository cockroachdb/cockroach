// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

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

// TestDownloadSpanRetriesFirstNodeFailure is a regression test for a bug in the
// per-node retry loop of systemStatusServer.DownloadSpan. The loop inspected the
// cluster-wide error accumulator (resp.Errors) instead of the node's own response
// (nodeResp.Errors). Because the accumulator is only populated by responseFn
// after a node's nodeFn returns, the first node to fail saw an empty accumulator,
// concluded it had succeeded, and broke out of its retry loop after a single
// attempt — never retrying its own transient failure.
//
// Injecting a download error on exactly one node makes that node's retry loop
// observe a permanently-empty accumulator, so the bug reproduces deterministically:
// the victim node attempts the download once (buggy) rather than
// perNodeMaxDownloadAttempts times (correct).
func TestDownloadSpanRetriesFirstNodeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var mu syncutil.Mutex
	attempts := make(map[roachpb.NodeID]int)

	// victim is set once the cluster is up; only this node's download fails, so it
	// is the sole contributor to the shared error accumulator and thus deterministically
	// the "first" (and only) node to observe an empty accumulator in its retry loop.
	var victim roachpb.NodeID

	knobs := &TestingKnobs{
		DownloadSpanLocalError: func(nodeID roachpb.NodeID) error {
			mu.Lock()
			attempts[nodeID]++
			mu.Unlock()
			if nodeID == victim {
				return errors.Newf("injected transient download error on n%d", nodeID)
			}
			return nil
		},
	}

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// DownloadSpan's fan-out is served by the system status server, so
			// keep the test on the system tenant.
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			Knobs:             base.TestingKnobs{Server: knobs},
		},
	})
	defer tc.Stopper().Stop(ctx)

	victim = tc.Server(1).NodeID()

	// The span is irrelevant: the victim short-circuits before touching storage,
	// and the other nodes download an empty span (a no-op). Use a narrow span over
	// a non-existent table so the healthy nodes have no real work.
	emptyTable := keys.SystemSQLCodec.TablePrefix(9999)
	span := roachpb.Span{Key: emptyTable, EndKey: emptyTable.PrefixEnd()}

	// An empty NodeID triggers the all-nodes fan-out. The status client
	// authenticates as root, which satisfies DownloadSpan's REPAIRCLUSTER check.
	resp, err := tc.Server(0).GetStatusClient(t).DownloadSpan(ctx, &serverpb.DownloadSpanRequest{
		Spans: []roachpb.Span{span},
	})
	require.NoError(t, err)

	// The victim's error must surface to the coordinator.
	require.Contains(t, resp.Errors, victim)

	// The victim should have retried perNodeMaxDownloadAttempts times before giving
	// up. On buggy code it stops after a single attempt.
	mu.Lock()
	victimAttempts := attempts[victim]
	mu.Unlock()
	require.Equal(t, perNodeMaxDownloadAttempts, victimAttempts,
		"victim node n%d should retry %d times, got %d attempts",
		victim, perNodeMaxDownloadAttempts, victimAttempts)
}
