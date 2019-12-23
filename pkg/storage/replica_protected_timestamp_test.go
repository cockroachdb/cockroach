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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestProtectedTimestampRecordApplies exercises
// Replica.protectedTimestampWillApply() at a low level.
// It does so by passing a Replica connected to an already
// shut down store to a variety of test cases.
func TestProtectedTimestampRecordApplies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	for _, testCase := range []struct {
		name string
		// Note that the store underneath the passed in Replica has been stopped.
		// This leaves the test to mutate the Replica state as it sees fit.
		test func(t *testing.T, r *Replica, mt *manualCache)
	}{

		// Test that if the lease started after the timestamp at which the record
		// was known to be live then we know that the Replica cannot GC until it
		// reads protected timestamp state after the lease start time. If the
		// relevant record is not found then it must have been removed.
		{
			name: "lease started after",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				r.mu.state.Lease.Start = r.store.Clock().Now()
				l, _ := r.GetLease()
				id := uuid.MakeV4()
				aliveAt := l.Start.Prev()
				ts := aliveAt.Prev()
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the GC threshold is already newer than the timestamp we want to
		// protect then we failed.
		{
			name: "gc threshold is after ts",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				thresh := r.store.Clock().Now()
				r.mu.state.GCThreshold = &thresh
				id := uuid.MakeV4()
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the GC threshold we're about to protect is newer than the timestamp
		// we want to protect then we're almost certain to fail. Treat it as a
		// failure.
		{
			name: "pending GC threshold is newer than the timestamp we want to protect",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				thresh := r.store.Clock().Now()
				require.NoError(t, r.markPendingGC(hlc.Timestamp{}, thresh))
				id := uuid.MakeV4()
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If the timestamp at which the record is known to be alive is newer than
		// our current view of the protected timestamp subsystem and we don't
		// already see the record, then we will refresh. In this case we refresh
		// and find it. We also verify that we cannot set the pending gc threshold
		// to above the timestamp we put in the record.
		{
			name: "newer aliveAt triggers refresh leading to success",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					require.Equal(t, refreshTo, aliveAt)
					mt.records = append(mt.records, &ptpb.Record{
						ID:        id,
						Timestamp: ts,
						Spans: []roachpb.Span{
							{
								Key:    roachpb.Key(r.startKey()),
								EndKey: roachpb.Key(r.startKey().Next()),
							},
						},
					})
					mt.asOf = refreshTo.Next()
					return nil
				}
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
				require.Equal(t,
					fmt.Sprintf("cannot set gc threshold to %v because read at %v < min %v",
						ts.Next(), ts, aliveAt.Next()),
					r.markPendingGC(ts, ts.Next()).Error())
			},
		},
		// If the timestamp at which the record is known to be alive is older than
		// our current view of the protected timestamp subsystem and we don't
		// already see the record, then we know that the record must have been
		// deleted already. Ensure we fail.
		{
			name: "record does not exist",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = aliveAt.Next()
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.NoError(t, err)
			},
		},
		// If we see the record then we know we're good.
		{
			name: "record already exists",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
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
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.True(t, willApply)
				require.NoError(t, err)
			},
		},
		// Ensure that a failure to Refresh propagates.
		{
			name: "refresh fails",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					return errors.New("boom")
				}
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.EqualError(t, err, "boom")
			},
		},
		// Ensure NLE propagates.
		{
			name: "not leaseholder before refresh",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				r.mu.Lock()
				lease := *r.mu.state.Lease
				lease.Sequence++
				lease.Replica = roachpb.ReplicaDescriptor{
					ReplicaID: 2,
					StoreID:   2,
					NodeID:    2,
				}
				r.mu.state.Lease = &lease
				r.mu.Unlock()
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Prev().Prev()
				mt.asOf = ts.Prev()
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.Regexp(t, "NotLeaseHolderError", err.Error())
			},
		},
		// Ensure NLE after performing a refresh propagates.
		{
			name: "not leaseholder after refresh",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(ctx context.Context, refreshTo hlc.Timestamp) error {
					r.mu.Lock()
					defer r.mu.Unlock()
					lease := *r.mu.state.Lease
					lease.Sequence++
					lease.Replica = roachpb.ReplicaDescriptor{
						ReplicaID: 2,
						StoreID:   2,
						NodeID:    2,
					}
					r.mu.state.Lease = &lease
					return nil
				}
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.Regexp(t, "NotLeaseHolderError", err.Error())
			},
		},
		// If refresh succeeds but the timestamp of the cache does not advance as
		// anticipated, ensure that an assertion failure error is returned.
		{
			name: "successful refresh does not update timestamp (assertion failure)",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				id := uuid.MakeV4()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					return nil
				}
				willApply, err := r.protectedTimestampRecordApplies(ctx, ts, aliveAt, id)
				require.False(t, willApply)
				require.EqualError(t, err, "cache was not updated after being refreshed")
				require.True(t, errors.IsAssertionFailure(err), "%v", err)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mc := &manualCache{}
			tsc.ProtectedTimestampCache = mc
			stopper := stop.NewStopper()
			tc.StartWithStoreConfig(t, stopper, tsc)
			stopper.Stop(ctx)
			testCase.test(t, tc.repl, mc)
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
		test func(t *testing.T, r *Replica, mt *manualCache)
	}{
		{
			name: "lease is too new",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				r.mu.state.Lease.Start = r.store.Clock().Now()
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx)
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
			},
		},
		{
			name: "have overlapping",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
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
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx)
				require.False(t, canGC)
				require.Equal(t, hlc.Timestamp{}, gcTimestamp)
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
					ID:        id,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				canGC, gcTimestamp := r.checkProtectedTimestampsForGC(ctx)
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
			tc.StartWithStoreConfig(t, stopper, tsc)
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
		for _, sp := range r.Spans {
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
		if r.ID == id {
			return true, c.asOf
		}
	}
	return false, c.asOf
}

var _ protectedts.Cache = (*manualCache)(nil)
