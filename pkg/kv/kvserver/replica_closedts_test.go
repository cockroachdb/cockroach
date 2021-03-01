// Copyright 2021 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// BenchmarkBumpSideTransportClosed measures the latency of a single call to
// (*Replica).BumpSideTransportClosed. The closed timestamp side-transport was
// designed with a performance expectation of this check taking no more than
// 100ns, so that calling the method on 10,000 leaseholders on a node could be
// done in less than 1ms.
//
// TODO(nvanbenschoten,andrei): Currently, the benchmark indicates that a call
// takes about 130ns. This exceeds the latency budget we've allocated to the
// call. However, it looks like there is some low-hanging fruit. 70% of the time
// is spent in leaseStatusForRequestRLocked, within which 24% of the total time
// is spent zeroing and copying memory and 30% of the total time is spent in
// (*NodeLiveness).GetLiveness, grabbing the current node's liveness record. If
// we eliminate some memory copying and pass the node liveness record in to the
// function so that we only have to grab it once instead of on each call to
// BumpSideTransportClosed, we should be able to reach our target latency.
func BenchmarkBumpSideTransportClosed(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	manual := hlc.NewHybridManualClock()
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	key, err := s.ScratchRange()
	require.NoError(b, err)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(b, err)
	rkey := keys.MustAddr(key)
	r := store.LookupReplica(rkey)
	require.NotNil(b, r)

	manual.Pause()
	now := s.Clock().NowAsClockTimestamp()
	var targets [roachpb.MAX_CLOSED_TIMESTAMP_POLICY]hlc.Timestamp

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Advance time and the closed timestamp target.
		now = now.ToTimestamp().Add(1, 0).UnsafeToClockTimestamp()
		targets[roachpb.LAG_BY_CLUSTER_SETTING] = now.ToTimestamp()

		// Perform the call.
		ok, _, _ := r.BumpSideTransportClosed(ctx, now, targets)
		if !ok {
			b.Fatal("BumpSideTransportClosed unexpectedly failed")
		}
	}
}
