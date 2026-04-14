// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestPostDestroySideloadedTruncation verifies that
// postDestroyClearSideloadedRaftMuLocked handles sideloaded entries correctly
// based on engine separation:
//   - Separated engines: only the unapplied suffix (entries after
//     RaftAppliedIndex) is removed via TruncateFrom; the applied prefix is
//     left for WAGTruncator to clean up later.
//   - Single engine: all entries are cleared.
func TestPostDestroySideloadedTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const term = kvpb.RaftTerm(1)
	allIndexes := []kvpb.RaftIndex{5, 10, 15, 20, 25}

	tests := []struct {
		name              string
		separated         bool
		expectedRemaining []kvpb.RaftIndex
	}{
		{
			separated: true,
			// On separate engines, only sideloaded entries belonging to the unapplied
			// suffix (indexes > RaftAppliedIndex) should be removed upon replica
			// destruction.
			expectedRemaining: []kvpb.RaftIndex{5, 10, 15},
		},
		{
			separated: false,
			// On a single engine, all sideloaded entries should be removed upon
			// replica destruction.
			expectedRemaining: nil,
		},
	}

	for _, tc := range tests {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tCtx := testContext{}
			tCtx.Start(ctx, t, stopper)
			repl := tCtx.repl

			// Configure engine separation for the test case.
			e := storage.NewDefaultInMemForTesting()
			defer e.Close()
			if tc.separated {
				repl.store.internalEngines = kvstorage.MakeSeparatedEnginesForTesting(e, e)
			} else {
				repl.store.internalEngines = kvstorage.MakeEngines(e)
			}

			// Populate sideloaded entries at various indexes.
			ss := repl.logStorage.ls.Sideload
			for _, idx := range allIndexes {
				require.NoError(t, ss.Put(ctx, idx, term, []byte("data")))
			}
			repl.shMu.state.RaftAppliedIndex = 15
			require.NoError(t, repl.postDestroyClearSideloadedRaftMuLocked(ctx))
			var remaining []kvpb.RaftIndex
			for _, idx := range allIndexes {
				if _, err := ss.Get(ctx, idx, term); err == nil {
					remaining = append(remaining, idx)
				}
			}
			require.Equal(t, tc.expectedRemaining, remaining)
		})
	}
}
