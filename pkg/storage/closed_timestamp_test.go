// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestStoreMinProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := storage.TestStoreConfig(nil)
	cfg.ClosedTimestampInterval = 5 * time.Second
	mtc := &multiTestContext{
		storeConfig: &cfg,
	}
	defer mtc.Stop()
	mtc.Start(t, 1)
	verifyLiveness(t, mtc)
	ctx := context.Background()
	store := mtc.stores[0]

	mp1, cleanup1 := store.GetMinProposal(ctx)
	mp2, cleanup2 := store.GetMinProposal(ctx)
	if mp1 != mp2 {
		t.Errorf("expected mp1 and mp2 equal; got %s vs %s", mp1, mp2)
	}

	// Initial call to get closed timestamp establishes baseline.
	closed1, epoch1 := store.GetClosedTimestampAndEpoch(ctx)
	if closed1 != mp1 {
		t.Errorf("expected closed and mp1 equal; got %s vs %s", closed1, mp2)
	}
	if epoch1 != 1 {
		t.Errorf("exected epoch == 1; got %d", epoch1)
	}

	// Now get more min proposals, which should occur at a higher timestamp.
	mp3, cleanup3 := store.GetMinProposal(ctx)
	if !mp1.Less(mp3) {
		t.Errorf("expected mp1 < mp3; got %s >= %s", mp1, mp3)
	}
	cleanup3()

	// However, the closed timestamp won't change so long as we have prior
	// min proposal cleanups outstanding.
	closed2, _ := store.GetClosedTimestampAndEpoch(ctx)
	if closed1 != closed2 {
		t.Errorf("expected closed1 and closed2 equal; got %s vs %s", closed1, closed2)
	}

	// With cleanups still outstanding, the min proposal will not advance.
	mp4, cleanup4 := store.GetMinProposal(ctx)
	if mp4 != mp3 {
		t.Errorf("expected mp3 and mp4 equal; got %s vs %s", mp3, mp4)
	}
	cleanup4()

	// Release just one of the two outstanding cleanups.
	cleanup1()

	// The closed and epoch still won't change.
	closed3, _ := store.GetClosedTimestampAndEpoch(ctx)
	if closed1 != closed3 {
		t.Errorf("expected closed1 and closed3 equal; got %s vs %s", closed1, closed3)
	}
	mp5, cleanup5 := store.GetMinProposal(ctx)
	if mp3 != mp5 {
		t.Errorf("expected mp3 and mp5 equal; got %s vs %s", mp3, mp5)
	}
	cleanup5()

	// Now cleanup the final min proposal to verify the new closed
	// equals the more recent min proposals, and the min proposal can advance.
	cleanup2()
	closed4, _ := store.GetClosedTimestampAndEpoch(ctx)
	if closed4 != mp3 {
		t.Errorf("expected closed4 and mp3 equal; got %s vs %s", closed4, mp3)
	}
	mp6, cleanup6 := store.GetMinProposal(ctx)
	if !mp3.Less(mp6) {
		t.Errorf("expected mp3 < mp6; got %s >= %s", mp3, mp6)
	}
	cleanup6()
}

// TestStoreClosedTimestamp verifies that a replica gets a closed
// timestamp from the leader with heartbeats, and continues to get
// advanced closed timestamps even as the range is quiesced.
func TestStoreClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Disable the replicate queue and the split queue, as we want to control both
	// up-replication and splits ourselves.
	sc := storage.TestStoreConfig(nil)
	sc.ClosedTimestampInterval = 1 * time.Second
	sc.TestingKnobs.DisableReplicateQueue = true
	sc.TestingKnobs.DisableSplitQueue = true
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	ctx := context.Background()

	mtc.Start(t, 3)
	// Set clock so that closed timestamps are not negative.
	mtc.manualClock.Increment(sc.ClosedTimestampInterval.Nanoseconds() + 1)
	verifyLiveness(t, mtc)

	key := roachpb.Key("a")
	if _, pErr := client.SendWrapped(ctx, mtc.distSenders[0], adminSplitArgs(key)); pErr != nil {
		t.Fatal(pErr)
	}

	// After the split, the rhs will still have an expiration-based lease.
	// Advance the clock and send a command to ensure it is renewed as an
	// epoch based lease.
	mtc.expireLeases(ctx)
	if _, err := mtc.dbs[0].Get(ctx, key); err != nil {
		t.Fatalf("%s failed to get: %s", key, err)
	}

	// Up-replicate to two stores.
	r1 := mtc.stores[0].LookupReplica(roachpb.RKey(key), nil)
	rangeID := r1.RangeID
	mtc.replicateRange(rangeID, 1, 2)
	r2 := mtc.stores[1].LookupReplica(roachpb.RKey(key), nil)
	r3 := mtc.stores[2].LookupReplica(roachpb.RKey(key), nil)

	mtc.stores[0].EnqueueRaftUpdateCheck(1)

	verifyCanServiceRead := func(r *storage.Replica, ts hlc.Timestamp, canServe bool) error {
		followerRead, _, pErr := r.CanServiceRead(ctx, true, ts)
		if pErr != nil {
			err, ok := pErr.GoError().(*roachpb.NotLeaseHolderError)
			if ok && !canServe {
				return nil
			} else if ok {
				return errors.Errorf("expected to but cannot serve follower read @%s", ts)
			}
			return err
		} else if !canServe {
			return errors.New("expected not lease holder error")
		} else if !followerRead {
			return errors.New("can't serve follower read but have no not lease holder error")
		}
		return nil
	}

	verifyFollowerRead := func(r *storage.Replica, ts hlc.Timestamp, canServe bool, expValue []byte) error {
		if repDesc, err := r.GetReplicaDescriptor(); err != nil {
			return err
		} else if l, _ := r.GetLease(); l.Replica.StoreID == repDesc.StoreID {
			return errors.Errorf("lease should not be held by follower replica %s: %s", r, l)
		}
		if err := verifyCanServiceRead(r, ts, canServe); err != nil {
			return err
		}
		if !canServe {
			return nil
		}
		// Execute the read, compare expected value.
		h := roachpb.Header{Timestamp: ts}
		reply, pErr := client.SendWrappedWith(ctx, mtc.distSenders[1], h, getArgs(key))
		if pErr != nil {
			return pErr.GoError()
		}
		v := reply.(*roachpb.GetResponse).Value
		if expValue == nil && v == nil {
			return nil
		} else if vBytes, err := v.GetBytes(); err != nil {
			return err
		} else if !bytes.Equal(vBytes, expValue) {
			return errors.Errorf("expected value %q; got %q", expValue, vBytes)
		}
		return nil
	}

	// Determine follower replicas.
	l, _ := r1.GetLease()
	var followers []*storage.Replica
	for _, r := range []*storage.Replica{r1, r2, r3} {
		if repDesc, err := r.GetReplicaDescriptor(); err != nil {
			t.Fatal(err)
		} else if l.Replica.StoreID != repDesc.StoreID {
			followers = append(followers, r)
		}
	}
	readAt := mtc.clock.Now()
	if err := mtc.advanceClock(ctx, sc.ClosedTimestampInterval+1); err != nil {
		t.Fatal(err)
	}

	// Verify follower replicas can serve follower reads earlier than
	// the closed timestamp interval once we've received the first
	// coalesced heartbeat.
	log.VEventf(ctx, 2, "verifying first follower read @%s now=%s", readAt, mtc.clock.Now())
	testutils.SucceedsSoon(t, func() error {
		for _, r := range followers {
			if err := verifyFollowerRead(r, readAt, true, nil); err != nil {
				log.VEventf(ctx, 3, "%s: failed verify: %s", r, err)
				return err
			}
		}
		return nil
	})

	// Now write a value at the current timestamp.
	value := []byte("value")
	putArgs := putArgs(key, value)
	log.VEventf(ctx, 2, "putting value at %s", key)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], putArgs); pErr != nil {
		t.Fatal(pErr)
	}
	// Follower replicas will not be able to serve follower reads at the
	// current timestamp.
	readAt = mtc.clock.Now()
	for _, r := range followers {
		if err := verifyFollowerRead(r, readAt, false, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Move the clock forward by the closed timestamp interval. Because
	// coalesced heartbeats happen every 50ms with the testing config,
	// we can wait in a SucceedsSoon loop to verify we can read the new
	// value.
	log.VEventf(ctx, 2, "advancing clock and verifying can read after new heartbeat")
	if err := mtc.advanceClock(ctx, sc.ClosedTimestampInterval+1); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		for _, r := range followers {
			if err := verifyFollowerRead(r, readAt, true, value); err != nil {
				return err
			}
			// However, we should be unable to do any follower read at the
			// current wall clock time, which will always be ahead of any
			// reported closed timestamp, even if only by logical ticks.
			if err := verifyFollowerRead(r, mtc.clock.Now(), false, value); err != nil {
				return err
			}
		}
		return nil
	})

	// Range 2 (the rhs of the split) will quiesce on the following
	// heartbeat. We verify that when it does, the closed timestamp
	// advances.
	log.VEventf(ctx, 2, "verifying range quiesces")
	testutils.SucceedsSoon(t, func() error {
		for _, r := range followers {
			if !r.IsQuiescent() {
				log.VEventf(ctx, 3, "%s @%s: range not quiescent", r, readAt)
				return errors.New("range not quiescent")
			}
		}
		return nil
	})
	log.VEventf(ctx, 2, "advancing clock and waiting for follower read to become available at advanced timestamp")
	readAt = mtc.clock.Now()
	if err := mtc.advanceClock(ctx, sc.ClosedTimestampInterval+1); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		for _, r := range followers {
			if err := verifyCanServiceRead(r, readAt, true); err != nil {
				log.VEventf(ctx, 3, "%s: failed to service read: %s", r, err)
				return err
			}
		}
		return nil
	})
}
