// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplicaLeaseStatus tests that Replica.leaseStatus returns a valid lease
// status, both for expiration- and epoch-based leases.
func TestReplicaLeaseStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	const maxOffset = 100 * time.Nanosecond
	clock := hlc.NewClock(hlc.NewHybridManualClock(), maxOffset, maxOffset)

	r1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	ts := []hlc.ClockTimestamp{
		{WallTime: 500, Logical: 0},  // before lease start
		{WallTime: 1000, Logical: 0}, // lease start
		{WallTime: 2000, Logical: 0}, // within lease
		{WallTime: 2950, Logical: 0}, // within stasis
		{WallTime: 3000, Logical: 0}, // lease expiration
		{WallTime: 3500, Logical: 0}, // expired
		{WallTime: 5000, Logical: 0}, // next expiration
	}
	inStasis := ts[3].ToTimestamp()
	expLease := roachpb.Lease{
		Replica:    r1,
		Start:      ts[1],
		Expiration: ts[4].ToTimestamp().Clone(),
		ProposedTS: &hlc.ClockTimestamp{WallTime: ts[1].WallTime - 100},
	}
	epoLease := roachpb.Lease{
		Replica:    expLease.Replica,
		Start:      expLease.Start,
		Epoch:      5,
		ProposedTS: expLease.ProposedTS,
	}

	oldLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 4, Expiration: hlc.LegacyTimestamp{WallTime: ts[1].WallTime},
	}
	curLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 5, Expiration: hlc.LegacyTimestamp{WallTime: ts[4].WallTime},
	}
	expLiveness := livenesspb.Liveness{
		NodeID: 1, Epoch: 6, Expiration: hlc.LegacyTimestamp{WallTime: ts[6].WallTime},
	}

	for _, tc := range []struct {
		lease         roachpb.Lease
		now           hlc.ClockTimestamp
		minProposedTS hlc.ClockTimestamp
		reqTS         hlc.Timestamp
		liveness      livenesspb.Liveness
		want          kvserverpb.LeaseState
		wantErr       string
	}{
		// Expiration-based lease, EXPIRED.
		{lease: expLease, now: ts[5], want: kvserverpb.LeaseState_EXPIRED},
		{lease: expLease, now: ts[4], want: kvserverpb.LeaseState_EXPIRED},
		// Expiration-based lease, PROSCRIBED.
		{lease: expLease, now: ts[3], minProposedTS: ts[1], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[2], want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[3], reqTS: inStasis,
			want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: expLease, now: ts[3], minProposedTS: ts[4], reqTS: inStasis,
			want: kvserverpb.LeaseState_PROSCRIBED},
		// Expiration-based lease, UNUSABLE.
		{lease: expLease, now: ts[3], reqTS: inStasis, want: kvserverpb.LeaseState_UNUSABLE},
		// Expiration-based lease, VALID.
		{lease: expLease, now: ts[2], reqTS: ts[2].ToTimestamp(), want: kvserverpb.LeaseState_VALID},

		// Epoch-based lease, EXPIRED.
		{lease: epoLease, now: ts[5], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLease, now: ts[4], liveness: curLiveness, want: kvserverpb.LeaseState_EXPIRED},
		{lease: epoLease, now: ts[2], liveness: expLiveness, want: kvserverpb.LeaseState_EXPIRED},
		// Epoch-based lease, PROSCRIBED.
		{lease: epoLease, now: ts[3], liveness: curLiveness, minProposedTS: ts[1],
			want: kvserverpb.LeaseState_PROSCRIBED},
		{lease: epoLease, now: ts[2], liveness: curLiveness, minProposedTS: ts[2],
			want: kvserverpb.LeaseState_PROSCRIBED},
		// Epoch-based lease, UNUSABLE.
		{lease: epoLease, now: ts[3], liveness: curLiveness, reqTS: ts[3].ToTimestamp(),
			want: kvserverpb.LeaseState_UNUSABLE},
		// Epoch-based lease, VALID.
		{lease: epoLease, now: ts[2], liveness: curLiveness, want: kvserverpb.LeaseState_VALID},

		// Epoch-based lease, ERROR.
		{lease: epoLease, now: ts[2], want: kvserverpb.LeaseState_ERROR,
			wantErr: "liveness record not found"},
		{lease: epoLease, now: ts[2], liveness: oldLiveness, want: kvserverpb.LeaseState_ERROR,
			wantErr: "node liveness info for n1 is stale"},
	} {
		t.Run("", func(t *testing.T) {
			cache :=
				liveness.NewCache(
					gossip.NewTest(roachpb.NodeID(1), stopper, metric.NewRegistry()),
					clock,
					cluster.MakeTestingClusterSettings(),
					nil,
				)
			l := liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
				Clock: clock,
				Cache: cache,
			})
			r := Replica{store: &Store{
				Ident: &roachpb.StoreIdent{StoreID: 1, NodeID: 1},
				cfg:   StoreConfig{Clock: clock, NodeLiveness: l},
			}}
			var empty livenesspb.Liveness
			if maybeLiveness := tc.liveness; maybeLiveness != empty {
				l.TestingMaybeUpdate(ctx, liveness.Record{Liveness: maybeLiveness})
			}

			got := r.leaseStatus(ctx, tc.lease, tc.now, tc.minProposedTS, hlc.ClockTimestamp{}, tc.reqTS)
			if tc.wantErr != "" {
				require.Contains(t, got.ErrInfo, tc.wantErr)
				got.ErrInfo = ""
			}
			assert.Equal(t, kvserverpb.LeaseStatus{
				Lease: tc.lease, Now: tc.now, RequestTime: tc.reqTS, State: tc.want, Liveness: tc.liveness,
			}, got)
		})
	}
}
