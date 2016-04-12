// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var testIdent = roachpb.StoreIdent{
	ClusterID: uuid.MakeV4(),
	NodeID:    1,
	StoreID:   1,
}

// setTestRetryOptions sets aggressive retries with a limit on number
// of attempts so we don't get stuck behind indefinite backoff/retry
// loops.
func setTestRetryOptions(s *Store) {
	s.SetRangeRetryOptions(retry.Options{
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		Multiplier:     2,
		MaxRetries:     1,
	})
}

// testSender is an implementation of the client.Sender interface
// which passes all requests through to a single store.
type testSender struct {
	store *Store
}

func (s *Store) testSender() client.Sender {
	return client.Wrap(s, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID == 0 {
			ba.RangeID = 1
		}
		return ba
	})
}

// Send forwards the call to the single store. This is a poor man's
// version of kv.TxnCoordSender, but it serves the purposes of
// supporting tests in this package. Transactions are not supported.
// Since kv/ depends on storage/, we can't get access to a
// TxnCoordSender from here.
// TODO(tschottdorf): {kv->storage}.LocalSender
func (db *testSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if et, ok := ba.GetArg(roachpb.EndTransaction); ok {
		return nil, roachpb.NewErrorf("%s method not supported", et.Method())
	}
	// Lookup range and direct request.
	rs, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	rng := db.store.LookupReplica(rs.Key, rs.EndKey)
	if rng == nil {
		return nil, roachpb.NewError(roachpb.NewRangeKeyMismatchError(rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), nil))
	}
	ba.RangeID = rng.RangeID
	replica := rng.GetReplica()
	if replica == nil {
		return nil, roachpb.NewErrorf("own replica missing in range")
	}
	ba.Replica = *replica
	br, pErr := db.store.Send(ctx, ba)
	if br != nil && br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(db.store, br))
	}
	if pErr != nil {
		return nil, pErr
	}
	return br, nil
}

// createTestStoreWithoutStart creates a test store using an in-memory
// engine without starting the store. It returns the store, the store
// clock's manual unix nanos time and a stopper. The caller is
// responsible for stopping the stopper upon completion.
// Some fields of ctx are populated by this function.
func createTestStoreWithoutStart(t *testing.T, ctx *StoreContext) (*Store, *hlc.ManualClock, *stop.Stopper) {
	stopper := stop.NewStopper()
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)
	rpcContext := rpc.NewContext(nil, nil, stopper)
	ctx.Gossip = gossip.New(rpcContext, nil, stopper)
	ctx.Gossip.SetNodeID(1)
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	ctx.StorePool = NewStorePool(ctx.Gossip, ctx.Clock, TestTimeUntilStoreDeadOff, stopper)
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20, stopper)
	ctx.Transport = NewDummyRaftTransport()
	sender := &testSender{}
	ctx.DB = client.NewDB(sender)
	store := NewStore(*ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})
	sender.store = store
	if err := store.Bootstrap(roachpb.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapRange(nil); err != nil {
		t.Fatal(err)
	}
	// Have to call g.SetNodeID before call g.AddInfo
	store.Gossip().SetNodeID(roachpb.NodeID(1))
	return store, manual, stopper
}

func createTestStore(t *testing.T) (*Store, *hlc.ManualClock, *stop.Stopper) {
	ctx := TestStoreContext()
	return createTestStoreWithContext(t, &ctx)
}

// createTestStore creates a test store using an in-memory
// engine. It returns the store, the store clock's manual unix nanos time
// and a stopper. The caller is responsible for stopping the stopper
// upon completion.
func createTestStoreWithContext(t *testing.T, ctx *StoreContext) (
	*Store, *hlc.ManualClock, *stop.Stopper) {

	store, manual, stopper := createTestStoreWithoutStart(t, ctx)
	// Put an empty system config into gossip.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig,
		&config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()
	return store, manual, stopper
}

// TestStoreInitAndBootstrap verifies store initialization and bootstrap.
func TestStoreInitAndBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := TestStoreContext()
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
	ctx.Transport = NewDummyRaftTransport()
	store := NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})

	// Can't start as haven't bootstrapped.
	if err := store.Start(stopper); err == nil {
		t.Error("expected failure starting un-bootstrapped store")
	}

	// Bootstrap with a fake ident.
	if err := store.Bootstrap(testIdent, stopper); err != nil {
		t.Errorf("error bootstrapping store: %s", err)
	}

	// Verify we can read the store ident after a flush.
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if value, _, err := engine.MVCCGet(context.Background(), eng, keys.StoreIdentKey(), roachpb.ZeroTimestamp, true, nil); err != nil {
		t.Fatal(err)
	} else if value == nil {
		t.Fatalf("unable to read store ident")
	}

	// Try to get 1st range--non-existent.
	if _, err := store.GetReplica(1); err == nil {
		t.Error("expected error fetching non-existent range")
	}

	// Bootstrap first range.
	if err := store.BootstrapRange(nil); err != nil {
		t.Errorf("failure to create first range: %s", err)
	}

	// Now, attempt to initialize a store with a now-bootstrapped range.
	store = NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})
	if err := store.Start(stopper); err != nil {
		t.Errorf("failure initializing bootstrapped store: %s", err)
	}
	// 1st range should be available.
	r, err := store.GetReplica(1)
	if err != nil {
		t.Errorf("failure fetching 1st range: %s", err)
	}
	rs := r.GetMVCCStats()

	// Stats should agree with a recomputation.
	now := r.store.Clock().Timestamp()
	if ms, err := ComputeStatsForRange(r.Desc(), eng, now.WallTime); err != nil {
		t.Errorf("failure computing range's stats: %s", err)
	} else if ms != rs {
		t.Errorf("expected range's stats to agree with recomputation: got\n%+v\nrecomputed\n%+v", ms, rs)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	// Put some random garbage into the engine.
	if err := eng.Put(engine.MakeMVCCMetadataKey(roachpb.Key("foo")), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %s", err)
	}
	ctx := TestStoreContext()
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	ctx.Transport = NewDummyRaftTransport()
	store := NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})

	// Can't init as haven't bootstrapped.
	if err := store.Start(stopper); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap should fail on non-empty engine.
	if err := store.Bootstrap(testIdent, stopper); err == nil {
		t.Error("expected bootstrap error on non-empty store")
	}
}

func createRange(s *Store, rangeID roachpb.RangeID, start, end roachpb.RKey) *Replica {
	desc := &roachpb.RangeDescriptor{
		RangeID:  rangeID,
		StartKey: start,
		EndKey:   end,
		Replicas: []roachpb.ReplicaDescriptor{{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}},
		NextReplicaID: 2,
	}
	r, err := NewReplica(desc, s, 0)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func TestStoreAddRemoveRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	if _, err := store.GetReplica(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}
	// Create a new range (id=2).
	rng2 := createRange(store, 2, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplicaTest(rng2); err != nil {
		t.Fatal(err)
	}
	// Try to add the same range twice
	err = store.AddReplicaTest(rng2)
	if err == nil {
		t.Fatal("expected error re-adding same range")
	}
	if _, ok := err.(rangeAlreadyExists); !ok {
		t.Fatalf("expected rangeAlreadyExists error; got %s", err)
	}
	// Try to remove range 1 again.
	if err := store.RemoveReplica(rng1, *rng1.Desc(), true); err == nil {
		t.Fatal("expected error re-removing same range")
	}
	// Try to add a range with previously-used (but now removed) ID.
	rng2Dup := createRange(store, 1, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplicaTest(rng2Dup); err == nil {
		t.Fatal("expected error inserting a duplicated range")
	}
	// Add another range with different key range and then test lookup.
	rng3 := createRange(store, 3, roachpb.RKey("c"), roachpb.RKey("d"))
	if err := store.AddReplicaTest(rng3); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		start, end roachpb.RKey
		expRng     *Replica
	}{
		{roachpb.RKey("a"), roachpb.RKey("a\x00"), rng2},
		{roachpb.RKey("a"), roachpb.RKey("b"), rng2},
		{roachpb.RKey("a\xff\xff"), roachpb.RKey("b"), rng2},
		{roachpb.RKey("c"), roachpb.RKey("c\x00"), rng3},
		{roachpb.RKey("c"), roachpb.RKey("d"), rng3},
		{roachpb.RKey("c\xff\xff"), roachpb.RKey("d"), rng3},
		{roachpb.RKey("x60\xff\xff"), roachpb.RKey("a"), nil},
		{roachpb.RKey("x60\xff\xff"), roachpb.RKey("a\x00"), nil},
		{roachpb.RKey("d"), roachpb.RKey("d"), nil},
		{roachpb.RKey("c\xff\xff"), roachpb.RKey("d\x00"), nil},
		{roachpb.RKey("a"), nil, rng2},
		{roachpb.RKey("d"), nil, nil},
	}

	for i, test := range testCases {
		if r := store.LookupReplica(test.start, test.end); r != test.expRng {
			t.Errorf("%d: expected range %v; got %v", i, test.expRng, r)
		}
	}
}

func TestStoreRemoveReplicaOldDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	origDesc := rng1.Desc()
	newDesc := protoutil.Clone(origDesc).(*roachpb.RangeDescriptor)
	_, newRep := newDesc.FindReplica(store.StoreID())
	newRep.ReplicaID++
	newDesc.NextReplicaID++
	if err := rng1.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveReplica(rng1, *origDesc, true); !testutils.IsError(err, "replica ID has changed") {
		t.Fatalf("expected error 'replica ID has changed' but got %s", err)
	}

	// Now try the latest descriptor and succeed.
	if err := store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRangeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Remove range 1.
	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}
	// Add 10 new ranges.
	const newCount = 10
	for i := 0; i < newCount; i++ {
		rng := createRange(store, roachpb.RangeID(i+1), roachpb.RKey(fmt.Sprintf("a%02d", i)), roachpb.RKey(fmt.Sprintf("a%02d", i+1)))
		if err := store.AddReplicaTest(rng); err != nil {
			t.Fatal(err)
		}
	}

	// Verify two passes of the visit.
	ranges := newStoreRangeSet(store)
	for pass := 0; pass < 2; pass++ {
		if ec := ranges.EstimatedCount(); ec != 10 {
			t.Errorf("expected 10 remaining; got %d", ec)
		}
		i := 1
		ranges.Visit(func(rng *Replica) bool {
			if rng.RangeID != roachpb.RangeID(i) {
				t.Errorf("expected range with Range ID %d; got %v", i, rng)
			}
			if ec := ranges.EstimatedCount(); ec != 10-i {
				t.Errorf("expected %d remaining; got %d", 10-i, ec)
			}
			i++
			return true
		})
		if ec := ranges.EstimatedCount(); ec != 10 {
			t.Errorf("expected 10 remaining; got %d", ec)
		}
	}

	// Try visiting with an addition and a removal.
	visited := make(chan struct{})
	updated := make(chan struct{})
	done := make(chan struct{})
	go func() {
		i := 1
		ranges.Visit(func(rng *Replica) bool {
			if i == 1 {
				if rng.RangeID != roachpb.RangeID(i) {
					t.Errorf("expected range with Range ID %d; got %v", i, rng)
				}
				close(visited)
				<-updated
			} else {
				// The second range will be removed and skipped.
				if rng.RangeID != roachpb.RangeID(i+1) {
					t.Errorf("expected range with Range ID %d; got %v", i+1, rng)
				}
			}
			i++
			return true
		})
		if i != 10 {
			t.Errorf("expected visit of 9 ranges, but got %v", i-1)
		}
		close(done)
	}()

	<-visited
	if ec := ranges.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}

	// Split the first range to insert a new range as second range.
	// The range is never visited with this iteration.
	rng := createRange(store, 11, roachpb.RKey("a000"), roachpb.RKey("a01"))
	if err = store.SplitRange(store.LookupReplica(roachpb.RKey("a00"), nil), rng); err != nil {
		t.Fatal(err)
	}
	// Estimated count will still be 9, as it's cached.
	if ec := ranges.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}

	// Now, remove the next range in the iteration and verify we skip the removed range.
	rng = store.LookupReplica(roachpb.RKey("a01"), nil)
	if rng.RangeID != 2 {
		t.Errorf("expected fetch of rangeID=2; got %d", rng.RangeID)
	}
	if err := store.RemoveReplica(rng, *rng.Desc(), true); err != nil {
		t.Error(err)
	}
	if ec := ranges.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}

	close(updated)
	<-done
}

func TestHasOverlappingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	if _, err := store.GetReplica(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveReplica(rng1, *rng1.Desc(), true); err != nil {
		t.Error(err)
	}

	// Create ranges.
	rngDescs := []struct {
		id         int
		start, end roachpb.RKey
	}{
		{2, roachpb.RKey("b"), roachpb.RKey("c")},
		{3, roachpb.RKey("c"), roachpb.RKey("d")},
		{4, roachpb.RKey("d"), roachpb.RKey("f")},
	}

	for _, desc := range rngDescs {
		rng := createRange(store, roachpb.RangeID(desc.id), desc.start, desc.end)
		if err := store.AddReplicaTest(rng); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		start, end roachpb.RKey
		exp        bool
	}{
		{roachpb.RKey("a"), roachpb.RKey("c"), true},
		{roachpb.RKey("b"), roachpb.RKey("c"), true},
		{roachpb.RKey("b"), roachpb.RKey("d"), true},
		{roachpb.RKey("d"), roachpb.RKey("e"), true},
		{roachpb.RKey("d"), roachpb.RKey("g"), true},
		{roachpb.RKey("e"), roachpb.RKey("e\x00"), true},

		{roachpb.RKey("f"), roachpb.RKey("g"), false},
		{roachpb.RKey("a"), roachpb.RKey("b"), false},
	}

	for i, test := range testCases {
		rngDesc := &roachpb.RangeDescriptor{StartKey: test.start, EndKey: test.end}
		if r := store.hasOverlappingReplicaLocked(rngDesc); r != test.exp {
			t.Errorf("%d: expected range %v; got %v", i, test.exp, r)
		}
	}
}

// TestStoreSend verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	gArgs := getArgs([]byte("a"))

	// Try a successful get request.
	if _, pErr := client.SendWrapped(store.testSender(), nil, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("aaa"))
	if _, pErr := client.SendWrapped(store.testSender(), nil, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestStoreObservedTimestamp verifies that execution of a transactional
// command on a Store always returns a timestamp observation, either per the
// error's or the response's transaction, as well as an originating NodeID.
func TestStoreObservedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	badKey := []byte("a")
	goodKey := []byte("b")
	desc := roachpb.ReplicaDescriptor{
		NodeID: 5,
		// not relevant
		StoreID:   1,
		ReplicaID: 2,
	}
	const wallTime = 1000

	testCases := []struct {
		key   roachpb.Key
		check func(roachpb.Response, *roachpb.Error)
	}{
		{badKey,
			func(_ roachpb.Response, pErr *roachpb.Error) {
				if pErr == nil {
					t.Fatal("expected an error")
				}
				txn := pErr.GetTxn()
				if txn == nil || txn.ID == nil {
					t.Fatalf("expected nontrivial transaction in %s", pErr)
				}
				if ts, _ := txn.GetObservedTimestamp(desc.NodeID); ts.WallTime != wallTime {
					t.Fatalf("unexpected observed timestamps, expected %d->%d but got map %+v",
						desc.NodeID, wallTime, txn.ObservedTimestamps)
				}
				if pErr.OriginNode != desc.NodeID {
					t.Fatalf("unexpected OriginNode %d, expected %d",
						pErr.OriginNode, desc.NodeID)
				}

			}},
		{goodKey,
			func(pReply roachpb.Response, pErr *roachpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				txn := pReply.Header().Txn
				if txn == nil || txn.ID == nil {
					t.Fatal("expected transactional response")
				}
				obs, _ := txn.GetObservedTimestamp(desc.NodeID)
				if act, exp := obs.WallTime, int64(wallTime); exp != act {
					t.Fatalf("unexpected observed wall time: %d, wanted %d", act, exp)
				}
			}},
	}

	for _, test := range testCases {
		func() {
			ctx := TestStoreContext()
			ctx.TestingKnobs.TestingCommandFilter =
				func(filterArgs storageutils.FilterArgs) *roachpb.Error {
					if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
						return roachpb.NewError(util.Errorf("boom"))
					}
					return nil
				}
			store, mc, stopper := createTestStoreWithContext(t, &ctx)
			defer stopper.Stop()
			txn := newTransaction("test", test.key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
			txn.MaxTimestamp = roachpb.MaxTimestamp
			pArgs := putArgs(test.key, []byte("value"))
			h := roachpb.Header{
				Txn:     txn,
				Replica: desc,
			}
			mc.Set(wallTime)
			test.check(client.SendWrappedWith(store.testSender(), context.Background(), h, &pArgs))
		}()
	}
}

// TestStoreAnnotateNow verifies that the Store sets Now on the batch responses.
func TestStoreAnnotateNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	badKey := []byte("a")
	goodKey := []byte("b")
	desc := roachpb.ReplicaDescriptor{
		NodeID: 5,
		// not relevant
		StoreID:   1,
		ReplicaID: 2,
	}

	testCases := []struct {
		key   roachpb.Key
		check func(*roachpb.BatchResponse, *roachpb.Error)
	}{
		{badKey,
			func(_ *roachpb.BatchResponse, pErr *roachpb.Error) {
				if pErr == nil {
					t.Fatal("expected an error")
				}
				if pErr.Now == roachpb.ZeroTimestamp {
					t.Fatal("timestamp not annotated on error")
				}
			}},
		{goodKey,
			func(pReply *roachpb.BatchResponse, pErr *roachpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				if pReply.Now == roachpb.ZeroTimestamp {
					t.Fatal("timestamp not annotated on batch response")
				}
			}},
	}

	for _, useTxn := range []bool{false, true} {
		for _, test := range testCases {
			func() {
				ctx := TestStoreContext()
				ctx.TestingKnobs.TestingCommandFilter =
					func(filterArgs storageutils.FilterArgs) *roachpb.Error {
						if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
							return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
						}
						return nil
					}
				store, _, stopper := createTestStoreWithContext(t, &ctx)
				defer stopper.Stop()
				var txn *roachpb.Transaction
				if useTxn {
					txn = newTransaction("test", test.key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
					txn.MaxTimestamp = roachpb.MaxTimestamp
				}
				pArgs := putArgs(test.key, []byte("value"))
				ba := roachpb.BatchRequest{
					Header: roachpb.Header{
						Txn:     txn,
						Replica: desc,
					},
				}
				ba.Add(&pArgs)

				test.check(store.testSender().Send(context.Background(), ba))
			}()
		}
	}
}

func TestStoreExecuteNoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	ba := roachpb.BatchRequest{}
	ba.RangeID = 1
	ba.Replica = roachpb.ReplicaDescriptor{StoreID: store.StoreID()}
	ba.Add(&roachpb.GetRequest{Span: roachpb.Span{Key: roachpb.Key("a")}})
	ba.Add(&roachpb.NoopRequest{})

	br, pErr := store.Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	reply := br.Responses[1].GetInner()
	if _, ok := reply.(*roachpb.NoopResponse); !ok {
		t.Errorf("expected *roachpb.NoopResponse, got %T", reply)
	}
}

// TestStoreVerifyKeys checks that key length is enforced and
// that end keys must sort >= start.
func TestStoreVerifyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	// Try a start key == KeyMax.
	gArgs := getArgs(roachpb.KeyMax)
	if _, pErr := client.SendWrapped(store.testSender(), nil, &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("expected error for start key == KeyMax: %v", pErr)
	}
	// Try a get with an end key specified (get requires only a start key and should fail).
	gArgs.EndKey = roachpb.KeyMax
	if _, pErr := client.SendWrapped(store.testSender(), nil, &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("unexpected error for end key specified on a non-range-based operation: %v", pErr)
	}
	// Try a scan with end key < start key.
	sArgs := scanArgs([]byte("b"), []byte("a"))
	if _, pErr := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for end key < start: %v", pErr)
	}
	// Try a scan with start key == end key.
	sArgs.Key = []byte("a")
	sArgs.EndKey = sArgs.Key
	if _, pErr := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for start == end key: %v", pErr)
	}
	// Try a scan with range-local start key, but "regular" end key.
	sArgs.Key = keys.MakeRangeKey([]byte("test"), []byte("sffx"), nil)
	sArgs.EndKey = []byte("z")
	if _, pErr := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsPError(pErr, "range-local") {
		t.Fatalf("unexpected error for local start, non-local end key: %v", pErr)
	}

	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	meta2KeyMax := testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax)
	pArgs := putArgs(meta2KeyMax, []byte("value"))
	if _, pErr := client.SendWrapped(store.testSender(), nil, &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", pErr)
	}
	// Try to put a range descriptor record for a start key which is
	// maximum length.
	key := append([]byte{}, roachpb.RKeyMax...)
	key[len(key)-1] = 0x01
	pArgs = putArgs(keys.RangeDescriptorKey(key), []byte("value"))
	if _, pErr := client.SendWrapped(store.testSender(), nil, &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to range descriptor for KeyMax value: %s", pErr)
	}
	// Try a put to txn record for a meta2 key (note that this doesn't
	// actually happen in practice, as txn records are not put directly,
	// but are instead manipulated only through txn methods).
	pArgs = putArgs(keys.TransactionKey(meta2KeyMax, uuid.NewV4()), []byte("value"))
	if _, pErr := client.SendWrapped(store.testSender(), nil, &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to txn meta2 value: %s", pErr)
	}
}

// TestStoreSendUpdateTime verifies that the node clock is updated.
func TestStoreSendUpdateTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("a"))
	reqTS := store.ctx.Clock.Now()
	reqTS.WallTime += (100 * time.Millisecond).Nanoseconds()
	_, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: reqTS}, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	ts := store.ctx.Clock.Timestamp()
	if ts.WallTime != reqTS.WallTime || ts.Logical <= reqTS.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", reqTS, ts)
	}
}

// TestStoreSendWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreSendWithZeroTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, mc, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("a"))

	// Set clock to time 1.
	mc.Set(1)
	_, respH, pErr := SendWrapped(store.testSender(), context.Background(), roachpb.Header{}, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if respH.Timestamp.WallTime != store.ctx.Clock.Now().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.ctx.Clock.Now(), respH.Timestamp)
	}
}

// TestStoreSendWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails.
func TestStoreSendWithClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, mc, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("a"))

	// Set clock to time 1.
	mc.Set(1)
	// Set clock max offset to 250ms.
	maxOffset := 250 * time.Millisecond
	store.ctx.Clock.SetMaxOffset(maxOffset)
	// Set args timestamp to exceed max offset.
	ts := store.ctx.Clock.Now().Add(maxOffset.Nanoseconds()+1, 0)
	if _, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: ts}, &args); pErr == nil {
		t.Error("expected max offset clock error")
	}
}

// TestStoreSendBadRange passes a bad range.
func TestStoreSendBadRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("0"))
	if _, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
		RangeID: 2, // no such range
	}, &args); pErr == nil {
		t.Error("expected invalid range")
	}
}

// splitTestRange splits a range. This does *not* fully emulate a real split
// and should not be used in new tests. Tests that need splits should live in
// client_split_test.go and use AdminSplit instead of this function.
// See #702
// TODO(bdarnell): convert tests that use this function to use AdminSplit instead.
func splitTestRange(store *Store, key, splitKey roachpb.RKey, t *testing.T) *Replica {
	rng := store.LookupReplica(key, nil)
	if rng == nil {
		t.Fatalf("couldn't lookup range for key %q", key)
	}
	desc, err := store.NewRangeDescriptor(splitKey, rng.Desc().EndKey, rng.Desc().Replicas)
	if err != nil {
		t.Fatal(err)
	}
	newRng, err := NewReplica(desc, store, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SplitRange(rng, newRng); err != nil {
		t.Fatal(err)
	}
	return newRng
}

// TestStoreSendOutOfRange passes a key not contained
// within the range's key range.
func TestStoreSendOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	rng2 := splitTestRange(store, roachpb.RKeyMin, roachpb.RKey(roachpb.Key("b")), t)

	// Range 1 is from KeyMin to "b", so reading "b" from range 1 should
	// fail because it's just after the range boundary.
	args := getArgs([]byte("b"))
	if _, err := client.SendWrapped(store.testSender(), nil, &args); err == nil {
		t.Error("expected key to be out of range")
	}

	// Range 2 is from "b" to KeyMax, so reading "a" from range 2 should
	// fail because it's before the start of the range.
	args = getArgs([]byte("a"))
	if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
		RangeID: rng2.RangeID,
	}, &args); err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRangeIDAllocation verifies that  range IDs are
// allocated in successive blocks.
func TestStoreRangeIDAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Range IDs should be allocated from ID 2 (first alloc'd range)
	// to rangeIDAllocCount * 3 + 1.
	for i := 0; i < rangeIDAllocCount*3; i++ {
		replicas := []roachpb.ReplicaDescriptor{{StoreID: store.StoreID()}}
		desc, err := store.NewRangeDescriptor(roachpb.RKey(fmt.Sprintf("%03d", i)), roachpb.RKey(fmt.Sprintf("%03d", i+1)), replicas)
		if err != nil {
			t.Fatal(err)
		}
		if desc.RangeID != roachpb.RangeID(2+i) {
			t.Errorf("expected range id %d; got %d", 2+i, desc.RangeID)
		}
	}
}

// TestStoreRangesByKey verifies we can lookup ranges by key using
// the sorted rangesByKey slice.
func TestStoreRangesByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	r0 := store.LookupReplica(roachpb.RKeyMin, nil)
	r1 := splitTestRange(store, roachpb.RKeyMin, roachpb.RKey("A"), t)
	r2 := splitTestRange(store, roachpb.RKey("A"), roachpb.RKey("C"), t)
	r3 := splitTestRange(store, roachpb.RKey("C"), roachpb.RKey("X"), t)
	r4 := splitTestRange(store, roachpb.RKey("X"), roachpb.RKey("ZZ"), t)

	if r := store.LookupReplica(roachpb.RKey("0"), nil); r != r0 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r0.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("B"), nil); r != r1 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r1.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("C"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r2.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("M"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r2.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("X"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r3.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("Z"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r3.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("ZZ"), nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r4.Desc())
	}
	if r := store.LookupReplica(roachpb.RKey("\xff\x00"), nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r4.Desc())
	}
	if store.LookupReplica(roachpb.RKeyMax, nil) != nil {
		t.Errorf("expected roachpb.KeyMax to not have an associated range")
	}
}

// TestStoreSetRangesMaxBytes creates a set of ranges via splitting
// and then sets the config zone to a custom max bytes value to
// verify the ranges' max bytes are updated appropriately.
func TestStoreSetRangesMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	baseID := uint32(keys.MaxReservedDescID + 1)
	testData := []struct {
		rng         *Replica
		expMaxBytes int64
	}{
		{store.LookupReplica(roachpb.RKeyMin, nil),
			config.DefaultZoneConfig().RangeMaxBytes},
		{splitTestRange(store, roachpb.RKeyMin, keys.MakeTablePrefix(baseID), t),
			1 << 20},
		{splitTestRange(store, keys.MakeTablePrefix(baseID), keys.MakeTablePrefix(baseID+1), t),
			config.DefaultZoneConfig().RangeMaxBytes},
		{splitTestRange(store, keys.MakeTablePrefix(baseID+1), keys.MakeTablePrefix(baseID+2), t),
			2 << 20},
	}

	// Set zone configs.
	config.TestingSetZoneConfig(baseID, &config.ZoneConfig{RangeMaxBytes: 1 << 20})
	config.TestingSetZoneConfig(baseID+2, &config.ZoneConfig{RangeMaxBytes: 2 << 20})

	// Despite faking the zone configs, we still need to have a gossip entry.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		for _, test := range testData {
			if mb := test.rng.GetMaxBytes(); mb != test.expMaxBytes {
				return util.Errorf("range max bytes values did not change to %d; got %d", test.expMaxBytes, mb)
			}
		}
		return nil
	})
}

// TestStoreLongTxnStarvation sets up a test which guarantees that
// every time a txn writes, it gets a write-too-old error by always
// having a non-transactional write succeed before the txn can retry,
// which would force endless retries unless batch requests are
// allowed to go ahead and lay down intents with advanced timestamps.
// Verifies no starvation for both serializable and snapshot txns.
func TestStoreLongTxnStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	setTestRetryOptions(store)
	defer stopper.Stop()

	for i, iso := range []roachpb.IsolationType{roachpb.SERIALIZABLE, roachpb.SNAPSHOT} {
		key := roachpb.Key(fmt.Sprintf("a-%d", i))
		txn := newTransaction("test", key, 1, iso, store.ctx.Clock)
		txn.Priority = math.MaxInt32

		for retry := 0; ; retry++ {
			if retry > 1 {
				t.Fatalf("%d: too many retries", i)
			}
			// Always send non-transactional put to push the transaction
			// and write a non-intent version.
			nakedPut := putArgs(key, []byte("naked"))
			_, pErr := client.SendWrapped(store.testSender(), context.Background(), &nakedPut)
			if pErr != nil && retry == 0 {
				t.Fatalf("%d: unexpected error on first put: %s", i, pErr)
			} else if retry == 1 {
				if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
					t.Fatalf("%d: expected write intent error; got %s", i, pErr)
				}
			}

			// Within the transaction, write same key.
			txn.Sequence++
			var ba roachpb.BatchRequest
			bt, btH := beginTxnArgs(key, txn)
			put := putArgs(key, []byte("value"))
			et, _ := endTxnArgs(txn, true)
			ba.Header = btH
			ba.Add(&bt)
			ba.Add(&put)
			ba.Add(&et)
			_, pErr = store.testSender().Send(context.Background(), ba)
			if retry == 0 {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
					t.Fatalf("%d: expected retry error on first txn put: %s", i, pErr)
				}
				txn = pErr.GetTxn()
			} else {
				if pErr != nil {
					t.Fatalf("%d: unexpected error: %s", i, pErr)
				}
				break
			}
			txn.Restart(1, 1, store.ctx.Clock.Now())
		}
	}
}

// TestStoreResolveWriteIntent adds write intent and then verifies
// that a put returns success and aborts intent's txn in the event the
// pushee has lower priority. Otherwise, verifies that a
// TransactionPushError is returned.
func TestStoreResolveWriteIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var mc *hlc.ManualClock
	var store *Store
	var stopper *stop.Stopper
	ctx := TestStoreContext()
	ctx.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			pr, ok := filterArgs.Req.(*roachpb.PushTxnRequest)
			if !ok || pr.PusherTxn.Name != "test" {
				return nil
			}
			if exp, act := mc.UnixNano(), pr.PushTo.WallTime; exp > act {
				return roachpb.NewError(fmt.Errorf("expected PushTo >= WallTime, but got %d < %d:\n%+v", act, exp, pr))
			}
			return nil
		}
	store, mc, stopper = createTestStoreWithContext(t, &ctx)
	defer stopper.Stop()

	for i, resolvable := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
		if resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First lay down intent using the pushee's txn.
		pArgs := putArgs(key, []byte("value"))
		h := roachpb.Header{Txn: pushee}
		pushee.Sequence++
		if _, err := maybeWrapWithBeginTransaction(store.testSender(), nil, h, &pArgs); err != nil {
			t.Fatal(err)
		}

		mc.Increment(100)
		// Now, try a put using the pusher's txn.
		h.Txn = pusher
		_, pErr := client.SendWrappedWith(store.testSender(), nil, h, &pArgs)
		if resolvable {
			if pErr != nil {
				t.Fatalf("expected intent resolved; got unexpected error: %s", pErr)
			}
			txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
			var txn roachpb.Transaction
			ok, err := engine.MVCCGetProto(context.Background(), store.Engine(), txnKey, roachpb.ZeroTimestamp, true, nil, &txn)
			if !ok || err != nil {
				t.Fatalf("not found or err: %s", err)
			}
			if txn.Status != roachpb.ABORTED {
				t.Fatalf("expected pushee to be aborted; got %s", txn.Status)
			}
		} else {
			if rErr, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
				t.Fatalf("expected txn push error; got %s", pErr)
			} else if !roachpb.TxnIDEqual(rErr.PusheeTxn.ID, pushee.ID) {
				t.Fatalf("expected txn to match pushee %q; got %s", pushee.ID, rErr)
			}
			// Trying again should fail again.
			h.Txn.Sequence++
			if _, pErr := client.SendWrappedWith(store.testSender(), nil, h, &pArgs); pErr == nil {
				t.Fatalf("expected another error on latent write intent but succeeded")
			}
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

	// First lay down intent using the pushee's txn.
	args := incrementArgs(key, 1)
	h := roachpb.Header{Txn: pushee}
	pushee.Sequence++
	if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, h, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try a put using the pusher's txn.
	h.Txn = pusher
	args.Increment = 2
	if resp, pErr := client.SendWrappedWith(store.testSender(), nil, h, &args); pErr != nil {
		t.Errorf("expected increment to succeed: %s", pErr)
	} else if reply := resp.(*roachpb.IncrementResponse); reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a
// write intent for a read will push the timestamp. On failure to
// push, verify a write intent error is returned with !Resolvable.
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	setTestRetryOptions(store)

	testCases := []struct {
		resolvable bool
		pusheeIso  roachpb.IsolationType
	}{
		// Resolvable is true, so we can read, but SERIALIZABLE means we can't commit.
		{true, roachpb.SERIALIZABLE},
		// Pushee is SNAPSHOT, meaning we can commit.
		{true, roachpb.SNAPSHOT},
		// Resolvable is false and SERIALIZABLE so can't read.
		{false, roachpb.SERIALIZABLE},
		// Resolvable is false, but SNAPSHOT means we can push it anyway, so can read.
		{false, roachpb.SNAPSHOT},
	}
	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
		pushee := newTransaction("test", key, 1, test.pusheeIso, store.ctx.Clock)
		if test.resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First, write original value.
		args := putArgs(key, []byte("value1"))
		if _, pErr := client.SendWrapped(store.testSender(), nil, &args); pErr != nil {
			t.Fatal(pErr)
		}

		// Second, lay down intent using the pushee's txn.
		_, btH := beginTxnArgs(key, pushee)
		args.Value.SetBytes([]byte("value2"))
		if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, btH, &args); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, try to read value using the pusher's txn.
		now := store.Clock().Now()
		pusher.OrigTimestamp.Forward(now)
		pusher.Timestamp.Forward(now)
		gArgs := getArgs(key)
		firstReply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Txn: pusher}, &gArgs)
		if test.resolvable {
			if pErr != nil {
				t.Errorf("%d: expected read to succeed: %s", i, pErr)
			} else if replyBytes, err := firstReply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(replyBytes, []byte("value1")) {
				t.Errorf("%d: expected bytes to be %q, got %q", i, "value1", replyBytes)
			}

			// Finally, try to end the pushee's transaction; if we have
			// SNAPSHOT isolation, the commit should work: verify the txn
			// commit timestamp is greater than pusher's Timestamp.
			// Otherwise, verify commit fails with TransactionRetryError.
			etArgs, h := endTxnArgs(pushee, true)
			pushee.Sequence++
			reply, cErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)

			minExpTS := pusher.Timestamp
			minExpTS.Logical++
			if test.pusheeIso == roachpb.SNAPSHOT {
				if cErr != nil {
					t.Errorf("unexpected error on commit: %s", cErr)
				}
				etReply := reply.(*roachpb.EndTransactionResponse)
				if etReply.Txn.Status != roachpb.COMMITTED || etReply.Txn.Timestamp.Less(minExpTS) {
					t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
						minExpTS, etReply.Txn)
				}
			} else {
				if _, ok := cErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
					t.Errorf("expected transaction retry error; got %s", cErr)
				}
			}
		} else {
			// If isolation of pushee is SNAPSHOT, we can always push, so
			// even a non-resolvable read will succeed. Otherwise, verify we
			// receive a transaction retry error (because we max out retries).
			if test.pusheeIso == roachpb.SNAPSHOT {
				if pErr != nil {
					t.Errorf("expected read to succeed: %s", pErr)
				} else if replyBytes, err := firstReply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(replyBytes, []byte("value1")) {
					t.Errorf("expected bytes to be %q, got %q", "value1", replyBytes)
				}
			} else {
				if pErr == nil {
					t.Errorf("expected read to fail")
				}
				if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
					t.Errorf("iso=%s; expected transaction retry error; got %T", test.pusheeIso, pErr.GetDetail())
				}
			}
		}
	}
}

// TestStoreResolveWriteIntentSnapshotIsolation verifies that the
// timestamp can always be pushed if txn has snapshot isolation.
func TestStoreResolveWriteIntentSnapshotIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")

	// First, write original value.
	args := putArgs(key, []byte("value1"))
	ts := store.ctx.Clock.Now()
	if _, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: ts}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Lay down intent using the pushee's txn.
	pushee := newTransaction("test", key, 1, roachpb.SNAPSHOT, store.ctx.Clock)
	pushee.Priority = 2
	h := roachpb.Header{Txn: pushee}
	args.Value.SetBytes([]byte("value2"))
	if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, h, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try to read value using the pusher's txn.
	gArgs := getArgs(key)
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pusher.Priority = 1 // Pusher would lose based on priority.
	h.Txn = pusher
	if reply, pErr := client.SendWrappedWith(store.testSender(), nil, h, &gArgs); pErr != nil {
		t.Errorf("expected read to succeed: %s", pErr)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, []byte("value1")) {
		t.Errorf("expected bytes to be %q, got %q", "value0", replyBytes)
	}

	// Finally, try to end the pushee's transaction; since it's got
	// SNAPSHOT isolation, the end should work, but verify the txn
	// commit timestamp is equal to gArgs.Timestamp + 1.
	etArgs, h := endTxnArgs(pushee, true)
	h.Txn.Sequence++
	reply, pErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	etReply := reply.(*roachpb.EndTransactionResponse)
	minExpTS := pusher.Timestamp
	minExpTS.Logical++
	if etReply.Txn.Status != roachpb.COMMITTED || etReply.Txn.Timestamp.Less(minExpTS) {
		t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
			minExpTS, etReply.Txn)
	}
}

// TestStoreResolveWriteIntentNoTxn verifies that reads and writes
// which are not part of a transaction can push intents.
func TestStoreResolveWriteIntentNoTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 0 // pushee should lose all conflicts

	// First, lay down intent from pushee.
	args := putArgs(key, []byte("value1"))
	if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, roachpb.Header{Txn: pushee}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try to read outside a transaction.
	getTS := store.ctx.Clock.Now() // accessed later
	{
		gArgs := getArgs(key)
		if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			Timestamp:    getTS,
			UserPriority: -math.MaxInt32,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			t.Errorf("expected value to be nil, got %+v", gReply.Value)
		}
	}

	{
		// Next, try to write outside of a transaction. We will succeed in pushing txn.
		putTS := store.ctx.Clock.Now()
		args.Value.SetBytes([]byte("value2"))
		if _, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			Timestamp:    putTS,
			UserPriority: -math.MaxInt32,
		}, &args); pErr != nil {
			t.Errorf("expected success aborting pushee's txn; got %s", pErr)
		}
	}

	// Read pushee's txn.
	txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
	var txn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(context.Background(), store.Engine(), txnKey, roachpb.ZeroTimestamp, true, nil, &txn); !ok || err != nil {
		t.Fatalf("not found or err: %s", err)
	}
	if txn.Status != roachpb.ABORTED {
		t.Errorf("expected pushee to be aborted; got %s", txn.Status)
	}

	// Verify that the pushee's timestamp was moved forward on
	// former read, since we have it available in write intent error.
	minExpTS := getTS
	minExpTS.Logical++
	if txn.Timestamp.Less(minExpTS) {
		t.Errorf("expected pushee timestamp pushed to %s; got %s", minExpTS, txn.Timestamp)
	}
	// Similarly, verify that pushee's priority was moved from 0
	// to math.MaxInt32-1 during push.
	if txn.Priority != math.MaxInt32-1 {
		t.Errorf("expected pushee priority to be pushed to %d; got %d", math.MaxInt32-1, txn.Priority)
	}

	// Finally, try to end the pushee's transaction; it should have
	// been aborted.
	etArgs, h := endTxnArgs(pushee, true)
	pushee.Sequence++
	_, pErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)
	if pErr == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", pErr)
	}
}

func setTxnAutoGC(to bool) func() {
	orig := txnAutoGC
	f := func() {
		txnAutoGC = orig
	}
	txnAutoGC = to
	return f
}

// TestStoreReadInconsistent verifies that gets and scans with read
// consistency set to INCONSISTENT either push or simply ignore extant
// intents (if they cannot be pushed), depending on the intent priority.
func TestStoreReadInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The test relies on being able to commit a Txn without specifying the
	// intent, while preserving the Txn record. Turn off
	// automatic cleanup for this to work.
	defer setTxnAutoGC(false)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	for _, canPush := range []bool{true, false} {
		keyA := roachpb.Key(fmt.Sprintf("%t-a", canPush))
		keyB := roachpb.Key(fmt.Sprintf("%t-b", canPush))

		// First, write keyA.
		args := putArgs(keyA, []byte("value1"))
		if _, pErr := client.SendWrapped(store.testSender(), nil, &args); pErr != nil {
			t.Fatal(pErr)
		}

		// Next, write intents for keyA and keyB. Note that the
		// transactions have unpushable priorities if canPush is true and
		// very pushable ones otherwise.
		priority := roachpb.UserPriority(-math.MaxInt32)
		if canPush {
			priority = -1
		}
		args.Value.SetBytes([]byte("value2"))
		txnA := newTransaction("testA", keyA, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
		txnB := newTransaction("testB", keyB, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
		for _, txn := range []*roachpb.Transaction{txnA, txnB} {
			args.Key = txn.Key
			if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); pErr != nil {
				t.Fatal(pErr)
			}
		}
		// End txn B, but without resolving the intent.
		etArgs, h := endTxnArgs(txnB, true)
		txnB.Sequence++
		if _, pErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, get from both keys and verify. Whether we can push or not, we
		// will be able to read with INCONSISTENT.
		gArgs := getArgs(keyA)

		if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(replyBytes, []byte("value1")) {
			t.Errorf("expected value %q, got %+v", []byte("value1"), reply)
		}
		gArgs.Key = keyB

		if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			// The new value of B will not be read at first.
			t.Errorf("expected value nil, got %+v", gReply.Value)
		}
		// However, it will be read eventually, as B's intent can be
		// resolved asynchronously as txn B is committed.
		util.SucceedsSoon(t, func() error {
			if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
				ReadConsistency: roachpb.INCONSISTENT,
			}, &gArgs); pErr != nil {
				return util.Errorf("expected read to succeed: %s", pErr)
			} else if gReply := reply.(*roachpb.GetResponse).Value; gReply == nil {
				return util.Errorf("value is nil")
			} else if replyBytes, err := gReply.GetBytes(); err != nil {
				return err
			} else if !bytes.Equal(replyBytes, []byte("value2")) {
				return util.Errorf("expected value %q, got %+v", []byte("value2"), reply)
			}
			return nil
		})

		// Scan keys and verify results.
		sArgs := scanArgs(keyA, keyB.Next())
		reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &sArgs)
		if pErr != nil {
			t.Errorf("expected scan to succeed: %s", pErr)
		}
		sReply := reply.(*roachpb.ScanResponse)
		if l := len(sReply.Rows); l != 2 {
			t.Errorf("expected 2 results; got %d", l)
		} else if key := sReply.Rows[0].Key; !key.Equal(keyA) {
			t.Errorf("expected key %q; got %q", keyA, key)
		} else if key := sReply.Rows[1].Key; !key.Equal(keyB) {
			t.Errorf("expected key %q; got %q", keyB, key)
		} else if val1, err := sReply.Rows[0].Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(val1, []byte("value1")) {
			t.Errorf("expected value %q, got %q", []byte("value1"), val1)
		} else if val2, err := sReply.Rows[1].Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(val2, []byte("value2")) {
			t.Errorf("expected value %q, got %q", []byte("value2"), val2)
		}
	}
}

// TestStoreScanIntents verifies that a scan across 10 intents resolves
// them in one fell swoop using both consistent and inconsistent reads.
func TestStoreScanIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := TestStoreContext()
	var count int32
	countPtr := &count

	ctx.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if _, ok := filterArgs.Req.(*roachpb.ScanRequest); ok {
				atomic.AddInt32(countPtr, 1)
			}
			return nil
		}
	store, _, stopper := createTestStoreWithContext(t, &ctx)
	defer stopper.Stop()

	testCases := []struct {
		consistent bool
		canPush    bool  // can the txn be pushed?
		expFinish  bool  // do we expect the scan to finish?
		expCount   int32 // how many times do we expect to scan?
	}{
		// Consistent which can push will make two loops.
		{true, true, true, 2},
		// Consistent but can't push will backoff and retry and not finish.
		{true, false, false, -1},
		// Inconsistent and can push will make one loop, with async resolves.
		{false, true, true, 1},
		// Inconsistent and can't push will just read inconsistent (will read nils).
		{false, false, true, 1},
	}
	for i, test := range testCases {
		// The command filter just counts the number of scan requests which are
		// submitted to the range.
		atomic.StoreInt32(countPtr, 0)

		// Lay down 10 intents to scan over.
		var txn *roachpb.Transaction
		keys := []roachpb.Key{}
		for j := 0; j < 10; j++ {
			key := roachpb.Key(fmt.Sprintf("key%d-%02d", i, j))
			keys = append(keys, key)
			if txn == nil {
				priority := roachpb.UserPriority(-1)
				if !test.canPush {
					priority = -math.MaxInt32
				}
				txn = newTransaction(fmt.Sprintf("test-%d", i), key, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
			}
			args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
			if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); pErr != nil {
				t.Fatal(pErr)
			}
			txn.Sequence++
			txn.Writing = true
		}

		// Scan the range and verify count. Do this in a goroutine in case
		// it isn't expected to finish.
		sArgs := scanArgs(keys[0], keys[9].Next())
		var sReply *roachpb.ScanResponse
		ts := store.Clock().Now()
		consistency := roachpb.CONSISTENT
		if !test.consistent {
			consistency = roachpb.INCONSISTENT
		}
		done := make(chan struct{})
		go func() {
			if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
				Timestamp:       ts,
				ReadConsistency: consistency,
			}, &sArgs); pErr != nil {
				t.Fatal(pErr)
			} else {
				sReply = reply.(*roachpb.ScanResponse)
			}
			close(done)
		}()

		wait := 1 * time.Second
		if !test.expFinish {
			wait = 10 * time.Millisecond
		}
		select {
		case <-done:
			if len(sReply.Rows) != 0 {
				t.Errorf("expected empty scan result; got %+v", sReply.Rows)
			}
			if countVal := atomic.LoadInt32(countPtr); countVal != test.expCount {
				t.Errorf("%d: expected scan count %d; got %d", i, test.expCount, countVal)
			}
		case <-time.After(wait):
			if test.expFinish {
				t.Errorf("%d: scan failed to finish after %s", i, wait)
			} else {
				// Commit the unpushable txn so the read can finish.
				etArgs, h := endTxnArgs(txn, true)
				txn.Sequence++
				for _, key := range keys {
					etArgs.IntentSpans = append(etArgs.IntentSpans, roachpb.Span{Key: key})
				}
				if _, pErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); pErr != nil {
					t.Fatal(pErr)
				}
				<-done
			}
		}
	}
}

// TestStoreScanInconsistentResolvesIntents lays down 10 intents,
// commits the txn without resolving intents, then does repeated
// inconsistent reads until the data shows up, showing that the
// inconsistent reads are triggering intent resolution.
func TestStoreScanInconsistentResolvesIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test relies on having a committed Txn record and open intents on
	// the same Range. This only works with auto-gc turned off; alternatively
	// the test could move to splitting its underlying Range.
	defer setTxnAutoGC(false)()
	var intercept atomic.Value
	intercept.Store(true)
	ctx := TestStoreContext()
	ctx.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			_, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest)
			if ok && intercept.Load().(bool) {
				return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	store, _, stopper := createTestStoreWithContext(t, &ctx)
	defer stopper.Stop()

	// Lay down 10 intents to scan over.
	txn := newTransaction("test", roachpb.Key("foo"), 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	keys := []roachpb.Key{}
	for j := 0; j < 10; j++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", j))
		keys = append(keys, key)
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
		txn.Sequence++
		if _, pErr := maybeWrapWithBeginTransaction(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true
	}

	// Now, commit txn without resolving intents. If we hadn't disabled auto-gc
	// of Txn entries in this test, the Txn entry would be removed and later
	// attempts to resolve the intents would fail.
	etArgs, h := endTxnArgs(txn, true)
	txn.Sequence++
	if _, pErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); pErr != nil {
		t.Fatal(pErr)
	}

	intercept.Store(false) // allow async intent resolution

	// Scan the range repeatedly until we've verified count.
	sArgs := scanArgs(keys[0], keys[9].Next())
	util.SucceedsSoon(t, func() error {
		if reply, pErr := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &sArgs); pErr != nil {
			return pErr.GoError()
		} else if sReply := reply.(*roachpb.ScanResponse); len(sReply.Rows) != 10 {
			return util.Errorf("could not read rows as expected")
		}
		return nil
	})
}

// TestStoreBadRequests verifies that Send returns errors for
// bad requests that do not pass key verification.
//
// TODO(kkaneda): Add more test cases.
func TestStoreBadRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	txn := newTransaction("test", roachpb.Key("a"), 1 /* priority */, roachpb.SERIALIZABLE, store.ctx.Clock)

	args1 := getArgs(roachpb.Key("a"))
	args1.EndKey = roachpb.Key("b")

	args2 := getArgs(roachpb.RKeyMax)

	args3 := scanArgs(roachpb.Key("a"), roachpb.Key("a"))
	args4 := scanArgs(roachpb.Key("b"), roachpb.Key("a"))

	args5 := scanArgs(roachpb.RKeyMin, roachpb.Key("a"))
	rangeTreeRootAddr, err := keys.Addr(keys.RangeTreeRoot)
	if err != nil {
		t.Fatal(err)
	}
	args6 := scanArgs(keys.RangeTreeNodeKey(rangeTreeRootAddr), roachpb.Key("a"))

	tArgs0, _ := endTxnArgs(txn, false /* commit */)
	tArgs1, _ := heartbeatArgs(txn)

	tArgs2, tHeader2 := endTxnArgs(txn, false /* commit */)
	tHeader2.Txn.Key = tHeader2.Txn.Key.Next()

	tArgs3, tHeader3 := heartbeatArgs(txn)
	tHeader3.Txn.Key = tHeader3.Txn.Key.Next()

	tArgs4 := pushTxnArgs(txn, txn, roachpb.PUSH_ABORT)
	tArgs4.PusheeTxn.Key = txn.Key.Next()

	testCases := []struct {
		args   roachpb.Request
		header *roachpb.Header
		err    string
	}{
		// EndKey for non-Range is invalid.
		{&args1, nil, "should not be specified"},
		// Start key must be less than KeyMax.
		{&args2, nil, "must be less than"},
		// End key must be greater than start.
		{&args3, nil, "must be greater than"},
		{&args4, nil, "must be greater than"},
		// Can't range from local to global.
		{&args5, nil, "must be greater than LocalMax"},
		{&args6, nil, "is range-local, but"},
		// Txn must be specified in Header.
		{&tArgs0, nil, "no transaction specified"},
		{&tArgs1, nil, "no transaction specified"},
		// Txn key must be same as the request key.
		{&tArgs2, &tHeader2, "request key .* should match txn key .*"},
		{&tArgs3, &tHeader3, "request key .* should match txn key .*"},
		{&tArgs4, nil, "request key .* should match pushee"},
	}
	for i, test := range testCases {
		if test.header == nil {
			test.header = &roachpb.Header{}
		}
		if test.header.Txn != nil {
			test.header.Txn.Sequence++
		}
		if _, pErr := client.SendWrappedWith(store.testSender(), nil, *test.header, test.args); pErr == nil || test.err == "" || !testutils.IsPError(pErr, test.err) {
			t.Errorf("%d unexpected result: %s", i, pErr)
		}
	}
}

// fakeRangeQueue implements the rangeQueue interface and
// records which range is passed to MaybeRemove.
type fakeRangeQueue struct {
	maybeRemovedRngs chan *Replica
}

func (fq *fakeRangeQueue) Start(clock *hlc.Clock, stopper *stop.Stopper) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeAdd(rng *Replica, t roachpb.Timestamp) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeRemove(rng *Replica) {
	fq.maybeRemovedRngs <- rng
}

// TestMaybeRemove tests that MaybeRemove is called when a range is removed.
func TestMaybeRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := TestStoreContext()
	store, _, stopper := createTestStoreWithoutStart(t, &ctx)
	defer stopper.Stop()

	// Add a queue to the scanner before starting the store and running the scanner.
	// This is necessary to avoid data race.
	fq := &fakeRangeQueue{
		maybeRemovedRngs: make(chan *Replica),
	}
	store.scanner.AddQueues(fq)

	if err := store.Start(stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()

	rng, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(rng, *rng.Desc(), true); err != nil {
		t.Error(err)
	}
	// MaybeRemove is called.
	removedRng := <-fq.maybeRemovedRngs
	if removedRng != rng {
		t.Errorf("Unexpected removed range %v", removedRng)
	}
}
