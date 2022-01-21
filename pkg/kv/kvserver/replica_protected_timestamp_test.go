// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestCheckProtectedTimestampsForGC exercises
// Replica.checkProtectedTimestampsForGC() at a low level.
// It does so by passing a Replica connected to an already
// shut down store to a variety of test cases.
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
		test func(t *testing.T, r *Replica, mt *manualCache)
	}{
		{
			name: "lease is too new",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				r.mu.state.Lease.Start = r.store.Clock().NowAsClockTimestamp()
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.False(t, canGC)
				require.Zero(t, gcTimestamp)
			},
		},
		{
			name: "have overlapping but new enough that it's okay",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				mt.asOf = r.store.Clock().Now().Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        uuid.MakeV4().GetBytes(),
					Timestamp: ts,
					DeprecatedSpans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				// We should allow gc to proceed with the normal new threshold if that
				// threshold is earlier than all of the records.
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.True(t, canGC)
				require.Equal(t, mt.asOf, gcTimestamp)
			},
		},
		{
			// In this case we have a record which protects some data but we can
			// set the threshold to a later point.
			name: "have overlapping but can still GC some",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now().Add(-11*time.Second.Nanoseconds(), 0)
				mt.asOf = r.store.Clock().Now().Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        uuid.MakeV4().GetBytes(),
					Timestamp: ts,
					DeprecatedSpans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				// We should allow gc to proceed up to the timestamp which precedes the
				// protected timestamp. This means we expect a GC timestamp 10 seconds
				// after ts.Prev() given the policy.
				canGC, _, gcTimestamp, oldThreshold, newThreshold := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.True(t, canGC)
				require.False(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, ts.Prev().Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			// In this case we have a record which is right up against the GC
			// threshold.
			name: "have overlapping but have already GC'd right up to the threshold",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				r.mu.Lock()
				th := *r.mu.state.GCThreshold
				r.mu.Unlock()
				mt.asOf = r.store.Clock().Now().Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        uuid.MakeV4().GetBytes(),
					Timestamp: th.Next(),
					DeprecatedSpans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				// We should allow GC even if the threshold is already the
				// predecessor of the earliest valid record. However, the GC
				// queue does not enqueue ranges in such cases, so this is only
				// applicable to manually enqueued ranges.
				canGC, _, gcTimestamp, oldThreshold, newThreshold := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.True(t, canGC)
				require.True(t, newThreshold.Equal(oldThreshold))
				require.Equal(t, th.Add(10*time.Second.Nanoseconds(), 0), gcTimestamp)
			},
		},
		{
			name: "failed record does not prevent GC",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				thresh := ts.Next()
				r.mu.state.GCThreshold = &thresh
				mt.asOf = thresh.Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        id.GetBytes(),
					Timestamp: ts,
					DeprecatedSpans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makeTTLDuration(10))
				require.True(t, canGC)
				require.Equal(t, mt.asOf, gcTimestamp)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mc := &manualCache{}
			tsc.ProtectedTimestampCache = mc
			stopper := stop.NewStopper()
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)
			stopper.Stop(ctx)
			testCase.test(t, tc.repl, mc)
		})
	}
}

type manualCache struct {
	asOf    hlc.Timestamp
	records []*ptpb.Record
	refresh func(ctx context.Context, asOf hlc.Timestamp) error
}

func (c *manualCache) Iterate(
	ctx context.Context, start, end roachpb.Key, it protectedts.Iterator,
) hlc.Timestamp {
	query := roachpb.Span{Key: start, EndKey: end}
	for _, r := range c.records {
		for _, sp := range r.DeprecatedSpans {
			if query.Overlaps(sp) {
				it(r)
				break
			}
		}
	}
	return c.asOf
}

func (c *manualCache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	if c.refresh == nil {
		c.asOf = asOf
		return nil
	}
	return c.refresh(ctx, asOf)
}

func (c *manualCache) QueryRecord(
	ctx context.Context, id uuid.UUID,
) (exists bool, asOf hlc.Timestamp) {
	for _, r := range c.records {
		if r.ID.GetUUID() == id {
			return true, c.asOf
		}
	}
	return false, c.asOf
}

var _ protectedts.Cache = (*manualCache)(nil)
