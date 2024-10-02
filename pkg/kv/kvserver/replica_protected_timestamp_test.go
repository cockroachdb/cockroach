// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestCheckProtectedTimestampsForGC exercises
// Replica.checkProtectedTimestampsForGC() at a low level. It does so by passing
// a Replica connected to an already shut down store to a variety of test cases.
func TestCheckProtectedTimestampsForGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	makeTTLDuration := func(ttlSec int32) time.Duration {
		return time.Duration(ttlSec) * time.Second
	}
	for _, testCase := range []struct {
		name string
		// Note that the store underneath the passed in Replica has been stopped.
		// This leaves the test to mutate the Replica state as it sees fit.
		test func(t *testing.T, r *Replica, mp *manualPTSReader)
	}{
		{
			name: "lease is too new",
			test: func(t *testing.T, r *Replica, _ *manualPTSReader) {
				r.shMu.state.Lease.Start = r.store.Clock().NowAsClockTimestamp()
				canGC, _, gcTimestamp, _, _, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.False(t, canGC)
				require.Zero(t, gcTimestamp)
			},
		},
		{
			name: "no PTS information is available",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				mp.asOf = hlc.Timestamp{}
				canGC, _, gcTimestamp, _, _, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.False(t, canGC)
				require.Zero(t, gcTimestamp)
			},
		},
		{
			name: "have overlapping but new enough that it's okay",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				ts := r.store.Clock().Now()
				mp.asOf = r.store.Clock().Now().Next()
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{ts},
				})
				// We should allow gc to proceed with the normal new threshold if that
				// threshold is earlier than all of the records.
				canGC, _, gcTimestamp, _, _, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.Equal(t, mp.asOf, gcTimestamp)
			},
		},
		{
			// In this case we have a record which protects some data but we can
			// set the threshold to a later point.
			name: "have overlapping but can still GC some",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				ts := r.store.Clock().Now().Add(-11*time.Second.Nanoseconds(), 0)
				mp.asOf = r.store.Clock().Now().Next()
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{ts},
				})
				// We should allow gc to proceed up to the timestamp which precedes the
				// protected timestamp. This means we expect a GC timestamp 10 seconds
				// after ts.Prev() given the policy.
				canGC, _, gcTimestamp, oldThreshold, newThreshold, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.False(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, ts.Prev().Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			// In this case we have a record which is right up against the GC
			// threshold.
			name: "have overlapping but have already GC'd right up to the threshold",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				r.mu.Lock()
				th := *r.shMu.state.GCThreshold
				r.mu.Unlock()
				mp.asOf = r.store.Clock().Now().Next()
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{th.Next()},
				})
				// We should allow GC even if the threshold is already the
				// predecessor of the earliest valid record. However, the GC
				// queue does not enqueue ranges in such cases, so this is only
				// applicable to manually enqueued ranges.
				canGC, _, gcTimestamp, oldThreshold, newThreshold, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.Equal(t, newThreshold, oldThreshold)
				require.True(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, th.Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			name: "failed record does not prevent GC",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				ts := r.store.Clock().Now()
				thresh := ts.Next()
				r.shMu.state.GCThreshold = &thresh
				mp.asOf = thresh.Next()
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{ts},
				})
				canGC, _, gcTimestamp, _, _, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.Equal(t, mp.asOf, gcTimestamp)
			},
		},
		{
			name: "gc-able data before lease start",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				// Set up the following scenario :
				//      gc threshold < pts record timestamp < lease start < pts record read at
				// AND  pts record timestamp + gcttl < lease start
				//
				// We're simulating a protection record being read within the
				// current lease interval, with a protection timestamp before
				// the current lease start time but larger than the replica's gc
				// threshold (i.e. the pts record applies to the replica).
				// Finally, we want to make sure that the newly computed gc
				// threshold is still less than the current lease start time --
				// all of this serving as a regression test for #82954. In this
				// scenario, we should still be able to GC data.

				ts := r.store.Clock().Now()
				tsMinus60s := ts.Add(-60*time.Second.Nanoseconds(), 0)
				tsMinus30s := ts.Add(-30*time.Second.Nanoseconds(), 0)
				tsPlus30s := ts.Add(5*time.Second.Nanoseconds(), 0)
				const gcTTLSec = 10

				mp.asOf = tsPlus30s
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{tsMinus30s},
				})
				r.raftMu.Lock()
				r.mu.Lock()
				r.shMu.state.GCThreshold = &tsMinus60s
				r.shMu.state.Lease.Start = ts.UnsafeToClockTimestamp()
				r.raftMu.Unlock()
				r.mu.Unlock()

				canGC, readAt, gcTimestamp, oldThreshold, newThreshold, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(gcTTLSec))
				require.NoError(t, err)
				require.True(t, canGC)
				require.Equal(t, tsPlus30s, readAt)
				require.Equal(t, oldThreshold, tsMinus60s)
				require.True(t, oldThreshold.Less(newThreshold))
				require.Equal(t, tsMinus30s.Prev().Add(gcTTLSec*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			name: "earliest timestamp is picked when multiple records exist",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				ts1 := r.store.Clock().Now().Add(-11*time.Second.Nanoseconds(), 0)
				ts2 := r.store.Clock().Now().Add(-20*time.Second.Nanoseconds(), 0)
				ts3 := r.store.Clock().Now().Add(-30*time.Second.Nanoseconds(), 0)
				mp.asOf = r.store.Clock().Now().Next()
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{ts1, ts2, ts3},
				})

				// Shuffle the protection timestamps for good measure.
				mp.shuffleAllProtectionTimestamps()
				// We should allow gc to proceed up to the timestamp which precedes the
				// earliest protected timestamp (t3). This means we expect a GC
				// timestamp 10 seconds after ts3.Prev() given the policy.
				canGC, _, gcTimestamp, oldThreshold, newThreshold, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.False(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, ts3.Prev().Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			// We should be able to move the GC timestamp up if no protection
			// timestamps apply. The timestamp moves up till how fresh our reading of
			// PTS state is.
			name: "no protections apply",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				mp.asOf = r.store.Clock().Now().Next()
				canGC, _, gcTimestamp, _, _, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.Equal(t, mp.asOf, gcTimestamp)
			},
		},
		{
			// Set up such that multiple timestamps are present including timestamps
			// from failed records (i.e below the GCThreshold). We should be able to
			// move the GC timestamp using the earliest protection timestamp that is
			// still above the GCThreshold in such a case.
			name: "multiple timestamps present including failed",
			test: func(t *testing.T, r *Replica, mp *manualPTSReader) {
				mp.asOf = r.store.Clock().Now().Next()
				thresh := r.shMu.state.GCThreshold
				ts1 := thresh.Add(-7*time.Second.Nanoseconds(), 0)
				ts2 := thresh.Add(-4*time.Second.Nanoseconds(), 0)
				ts3 := thresh.Add(14*time.Second.Nanoseconds(), 0)
				ts4 := thresh.Add(20*time.Second.Nanoseconds(), 0)
				mp.protections = append(mp.protections, manualPTSReaderProtection{
					sp:                   roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
					protectionTimestamps: []hlc.Timestamp{ts1, ts2, ts3, ts4},
				})
				mp.shuffleAllProtectionTimestamps()
				// We should allow gc to proceed up to the timestamp which precedes the
				// earliest protected timestamp (t3) that is still valid. This means we
				// expect a GC timestamp 10 seconds after ts3.Prev() given the policy.
				canGC, _, gcTimestamp, oldThreshold, newThreshold, err := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.NoError(t, err)
				require.True(t, canGC)
				require.False(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, ts3.Prev().Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mp := &manualPTSReader{}
			tsc.ProtectedTimestampReader = mp
			stopper := stop.NewStopper()
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)
			stopper.Stop(ctx)
			testCase.test(t, tc.repl, mp)
		})
	}
}

type manualPTSReaderProtection struct {
	sp                   roachpb.Span
	protectionTimestamps []hlc.Timestamp
}

type manualPTSReader struct {
	asOf        hlc.Timestamp
	protections []manualPTSReaderProtection
}

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (mp *manualPTSReader) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	for _, protection := range mp.protections {
		if protection.sp.Overlaps(sp) {
			protectionTimestamps = append(protectionTimestamps, protection.protectionTimestamps...)
		}
	}
	return protectionTimestamps, mp.asOf, nil
}

// shuffleAllProtectionTimestamps shuffles protection timestamps associated with
// all spans.
func (mp *manualPTSReader) shuffleAllProtectionTimestamps() {
	for i := range mp.protections {
		rand.Shuffle(len(mp.protections[i].protectionTimestamps), func(a, b int) {
			mp.protections[i].protectionTimestamps[a], mp.protections[i].protectionTimestamps[b] =
				mp.protections[i].protectionTimestamps[b], mp.protections[i].protectionTimestamps[a]
		})
	}
}

var _ spanconfig.ProtectedTSReader = (*manualPTSReader)(nil)
