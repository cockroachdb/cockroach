// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampRecordWillApply exercises
// Replica.protectedTimestampWillApply() at a low level.
// It does so by passing a Replica connected to an already
// shut down store to a variety of test cases.
func TestProtectedTimestampRecordWillApply(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		// Note that the store underneath the passed in Replica has been stopped.
		// This leaves the test to mutate the Replica state as it sees fit.
		test func(t *testing.T, r *Replica, mt *manualTracker)
	}{
		// There are a bunch of different cases which interact with the replica's
		// protectedtsMu state in different ways.

		// Test that if the lease started after the timestamp at which the record
		// was known to be live then we know that the Replica cannot GC until it
		// reads protected timestamp state after the lease start time. If the
		// relevant record is not found then it must have been removed.
		{
			name: "lease started after",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				r.mu.state.Lease.Start = r.store.Clock().Now()
				l, _ := r.GetLease()
				id := uuid.MakeV4()
				aliveAt := l.Start.Prev()
				ts := aliveAt.Prev()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the GC threshold is already newer than the timestamp we want to
		// protect then we failed.
		{
			name: "gc threshold is after ts",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				thresh := r.store.Clock().Now()
				r.mu.state.GCThreshold = &thresh
				id := uuid.MakeV4()
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the GC threshold we're about to protect is newer than the timestamp
		// we want to protect then we're almost certain to fail. Treat it as a
		// failure.
		{
			name: "pending GC threshold is newer than the timestamp we want to protect",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				thresh := r.store.Clock().Now()
				require.NoError(t, r.markPendingGC(hlc.Timestamp{}, thresh))
				id := uuid.MakeV4()
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the timestamp at which the record is known to be alive is newer than
		// our current view of the protected timestamp subsystem and we don't
		// already see the record, then we will promise to wait to see the record.
		//
		// Verify this by peeking underneath the protectedTimestampMu and ensuring
		// that we cannot now mark the pending GC above the promise.
		{
			name: "pending GC threshold is newer than the timestamp we want to protect",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				require.EqualValues(t, map[uuid.UUID]hlc.Timestamp{
					id: aliveAt,
				}, r.protectedTimestampMu.promisedIDs)
				require.Equal(t,
					fmt.Sprintf("cannot set gc threshold to %v because read at %v < min %v",
						ts.Next(), hlc.Timestamp{}, aliveAt),
					r.markPendingGC(hlc.Timestamp{}, ts.Next()).Error())
			},
		},
		// If the timestamp at which the record is known to be alive is older than
		// our current view of the protected timestamp subsystem and we don't
		// already see the record, then we know that the record must have been
		// deleted already. Ensure we fail.
		{
			name: "pending GC threshold is newer than the timestamp we want to protect",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = aliveAt.Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If we see the record then we know we're good.
		{
			name: "pending GC threshold is newer than the timestamp we want to protect",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = aliveAt.Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        id,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mt := &manualTracker{}
			tsc.ProtectedTimestampTracker = mt
			stopper := stop.NewStopper()
			tc.StartWithStoreConfig(t, stopper, tsc)
			stopper.Stop(ctx)
			testCase.test(t, tc.repl, mt)
		})
	}
}

// TestCheckProtectedTimestampsForGC exercises
// Replica.checkProtectedTimestampsForGC() at a low level.
// It does so by passing a Replica connected to an already
// shut down store to a variety of test cases.
func TestCheckProtectedTimestampsForGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		// Note that the store underneath the passed in Replica has been stopped.
		// This leaves the test to mutate the Replica state as it sees fit.
		test func(t *testing.T, r *Replica, mt *manualTracker)
	}{
		{
			name: "lease is too new",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				r.mu.state.Lease.Start = r.store.Clock().Now()
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
			},
		},
		{
			name: "have overlapping",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				mt.asOf = r.store.Clock().Now().Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        uuid.MakeV4(),
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
			},
		},
		{
			name: "promised record is seen before it expires",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next().Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				mt.asOf = ts.Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        id,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				require.Len(t, r.protectedTimestampMu.promisedIDs, 1)
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 0)
			},
		},
		{
			name: "promised record prevents GC",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next().Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				mt.asOf = ts.Next()
				mt.records = append(mt.records)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 1)
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 1)
			},
		},
		{
			name: "promised record expires",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next().Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				mt.asOf = aliveAt.Next()
				mt.records = append(mt.records)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 1)
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.True(t, canGC)
				require.Equal(t, mt.asOf, gcTimestamp)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 0)
			},
		},
		{
			name: "failed record does not prevent GC",
			test: func(t *testing.T, r *Replica, mt *manualTracker) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next().Next()
				thresh := ts.Next()
				willApply, err := r.protectedTimestampRecordWillApply(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				r.mu.state.GCThreshold = &thresh
				mt.asOf = aliveAt.Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        id,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				require.Len(t, r.protectedTimestampMu.promisedIDs, 1)
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx, r.store.Clock().Now())
				require.True(t, canGC)
				require.Equal(t, mt.asOf, gcTimestamp)
				require.Len(t, r.protectedTimestampMu.promisedIDs, 0)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mt := &manualTracker{}
			tsc.ProtectedTimestampTracker = mt
			stopper := stop.NewStopper()
			tc.StartWithStoreConfig(t, stopper, tsc)
			stopper.Stop(ctx)
			testCase.test(t, tc.repl, mt)
		})
	}
}

type manualTracker struct {
	asOf    hlc.Timestamp
	records []*ptpb.Record
}

func (t *manualTracker) ProtectedBy(
	ctx context.Context, span roachpb.Span, it func(*ptpb.Record),
) hlc.Timestamp {
	for _, r := range t.records {
		for _, ps := range r.Spans {
			if span.Overlaps(ps) {
				it(r)
				break
			}
		}
	}
	return t.asOf
}

func (t *manualTracker) Start(context.Context, *stop.Stopper) error {
	panic("not implemented")
}

var _ protectedts.Tracker = (*manualTracker)(nil)
