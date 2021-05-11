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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	makeArgs := func(r *Replica, ts, aliveAt hlc.Timestamp) roachpb.AdminVerifyProtectedTimestampRequest {
		args := roachpb.AdminVerifyProtectedTimestampRequest{
			RecordID:      uuid.MakeV4(),
			Protected:     ts,
			RecordAliveAt: aliveAt,
		}
		args.Key, args.EndKey = r.Desc().StartKey.AsRawKey(), r.Desc().EndKey.AsRawKey()
		return args
	}
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
				r.mu.state.Lease.Start = r.store.Clock().NowAsClockTimestamp()
				l, _ := r.GetLease()
				aliveAt := l.Start.ToTimestamp().Prev()
				ts := aliveAt.Prev()
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReaason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.True(t, willApply)
				require.NoError(t, err)
				require.Empty(t, doesNotApplyReaason)
			},
		},
		// If the GC threshold is already newer than the timestamp we want to
		// protect then we failed.
		{
			name: "gc threshold is after ts",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				thresh := r.store.Clock().Now()
				r.mu.state.GCThreshold = &thresh
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.NoError(t, err)
				require.Regexp(t, fmt.Sprintf("protected ts: %s is less than equal to the GCThreshold: %s"+
					" for the range /Min - /Max", ts.String(), thresh.String()), doesNotApplyReason)
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
				ts := thresh.Prev().Prev()
				aliveAt := ts.Next()
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.NoError(t, err)
				require.Regexp(t, fmt.Sprintf("protected ts: %s is less than the pending GCThreshold: %s"+
					" for the range /Min - /Max", ts.String(), thresh.String()), doesNotApplyReason)
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
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				args := makeArgs(r, ts, aliveAt)
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					require.Equal(t, refreshTo, aliveAt)
					mt.records = append(mt.records, &ptpb.Record{
						ID:        args.RecordID,
						Timestamp: ts,
						Spans: []roachpb.Span{
							{
								Key:    roachpb.Key(r.Desc().StartKey),
								EndKey: roachpb.Key(r.Desc().StartKey.Next()),
							},
						},
					})
					mt.asOf = refreshTo.Next()
					return nil
				}
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.True(t, willApply)
				require.NoError(t, err)
				require.Empty(t, doesNotApplyReason)
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
				aliveAt := ts.Next()
				mt.asOf = aliveAt.Next()
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.NoError(t, err)
				require.Regexp(t, "protected ts record has been removed", doesNotApplyReason)
			},
		},
		// If we see the record then we know we're good.
		{
			name: "record already exists",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				aliveAt := ts.Next()
				args := makeArgs(r, ts, aliveAt)
				mt.asOf = aliveAt.Next()
				mt.records = append(mt.records, &ptpb.Record{
					ID:        args.RecordID,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.True(t, willApply)
				require.NoError(t, err)
				require.Empty(t, doesNotApplyReason)
			},
		},
		// Ensure that a failure to Refresh propagates.
		{
			name: "refresh fails",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					return errors.New("boom")
				}
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.EqualError(t, err, "boom")
				require.Empty(t, doesNotApplyReason)
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
				aliveAt := ts.Prev().Prev()
				mt.asOf = ts.Prev()
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.Error(t, err)
				require.Regexp(t, "NotLeaseHolderError", err.Error())
				require.Empty(t, doesNotApplyReason)
			},
		},
		// Ensure NLE after performing a refresh propagates.
		{
			name: "not leaseholder after refresh",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
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
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.Error(t, err)
				require.Regexp(t, "NotLeaseHolderError", err.Error())
				require.Empty(t, doesNotApplyReason)
			},
		},
		// If refresh succeeds but the timestamp of the cache does not advance as
		// anticipated, ensure that an assertion failure error is returned.
		{
			name: "successful refresh does not update timestamp (assertion failure)",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				mt.refresh = func(_ context.Context, refreshTo hlc.Timestamp) error {
					return nil
				}
				args := makeArgs(r, ts, aliveAt)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.EqualError(t, err, "cache was not updated after being refreshed")
				require.True(t, errors.IsAssertionFailure(err), "%v", err)
				require.Empty(t, doesNotApplyReason)
			},
		},
		// If a request header is for a key span which is not owned by this replica,
		// ensure that a roachpb.RangeKeyMismatchError is returned.
		{
			name: "request span is respected",
			test: func(t *testing.T, r *Replica, mt *manualCache) {
				ts := r.store.Clock().Now()
				aliveAt := ts.Next()
				mt.asOf = ts.Prev()
				args := makeArgs(r, ts, aliveAt)
				r.mu.state.Desc.StartKey = roachpb.RKey(keys.TableDataMax)
				willApply, doesNotApplyReason, err := r.protectedTimestampRecordApplies(ctx, &args)
				require.False(t, willApply)
				require.Error(t, err)
				require.Regexp(t, "key range /Min-/Max outside of bounds of range /Table/Max-/Max", err.Error())
				require.Empty(t, doesNotApplyReason)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			mc := &manualCache{}
			tsc.ProtectedTimestampCache = mc
			// Under extreme stressrace scenarios the single replica can somehow
			// lose the lease. Make the timeout extremely long.
			tsc.RaftConfig.RangeLeaseRaftElectionTimeoutMultiplier = 100
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	makePolicy := func(ttlSec int32) zonepb.GCPolicy {
		return zonepb.GCPolicy{TTLSeconds: ttlSec}
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
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makePolicy(10))
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
					ID:        uuid.MakeV4(),
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				// We should allow gc to proceed with the normal new threshold if that
				// threshold is earlier than all of the records.
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makePolicy(10))
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
					ID:        uuid.MakeV4(),
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				// We should allow gc to proceed up to the timestamp which precedes the
				// protected timestamp. This means we expect a GC timestamp 10 seconds
				// after ts.Prev() given the policy.
				canGC, _, gcTimestamp, oldThreshold, newThreshold := r.checkProtectedTimestampsForGC(ctx, makePolicy(10))
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
					ID:        uuid.MakeV4(),
					Timestamp: th.Next(),
					Spans: []roachpb.Span{
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
				canGC, _, gcTimestamp, oldThreshold, newThreshold := r.checkProtectedTimestampsForGC(ctx, makePolicy(10))
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
					ID:        id,
					Timestamp: ts,
					Spans: []roachpb.Span{
						{
							Key:    keys.MinKey,
							EndKey: keys.MaxKey,
						},
					},
				})
				canGC, _, gcTimestamp, _, _ := r.checkProtectedTimestampsForGC(ctx, makePolicy(10))
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
