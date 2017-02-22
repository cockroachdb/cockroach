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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var defaultMuLogger = thresholdLogger(
	context.Background(),
	10*time.Second,
	log.Warningf,
)

var testIdent = roachpb.StoreIdent{
	ClusterID: uuid.MakeV4(),
	NodeID:    1,
	StoreID:   1,
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
func (db *testSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if et, ok := ba.GetArg(roachpb.EndTransaction); ok {
		return nil, roachpb.NewErrorf("%s method not supported", et.Method())
	}
	// Lookup range and direct request.
	rs, err := keys.Range(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	repl := db.store.LookupReplica(rs.Key, rs.EndKey)
	if repl == nil {
		return nil, roachpb.NewError(roachpb.NewRangeKeyMismatchError(rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), nil))
	}
	ba.RangeID = repl.RangeID
	repDesc, err := repl.GetReplicaDescriptor()
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	ba.Replica = repDesc
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
func createTestStoreWithoutStart(t testing.TB, stopper *stop.Stopper, cfg *StoreConfig) *Store {
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)

	rpcContext := rpc.NewContext(log.AmbientContext{}, &base.Config{Insecure: true}, cfg.Clock, stopper)
	server := rpc.NewServer(rpcContext) // never started
	cfg.Gossip = gossip.NewTest(1, rpcContext, server, nil, stopper, metric.NewRegistry())
	cfg.StorePool = NewStorePool(
		log.AmbientContext{},
		cfg.Gossip,
		cfg.Clock,
		StorePoolNodeLivenessTrue,
		TestTimeUntilStoreDeadOff,
		/* deterministic */ false,
	)
	// Many tests using this test harness (as opposed to higher-level
	// ones like multiTestContext or TestServer) want to micro-manage
	// replicas and the background queues just get in the way. The
	// scanner doesn't run frequently enough to expose races reliably,
	// so just disable the scanner for all tests that use this function
	// instead of figuring out exactly which tests need it.
	cfg.TestingKnobs.DisableScanner = true
	// The scanner affects background operations; we must also disable
	// the split queue separately to cover event-driven splits.
	cfg.TestingKnobs.DisableSplitQueue = true
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	stopper.AddCloser(eng)
	cfg.Transport = NewDummyRaftTransport()
	sender := &testSender{}
	cfg.DB = client.NewDB(sender)
	store := NewStore(*cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})
	sender.store = store
	if err := store.Bootstrap(roachpb.StoreIdent{NodeID: 1, StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.BootstrapRange(nil); err != nil {
		t.Fatal(err)
	}
	return store
}

func createTestStore(t testing.TB, stopper *stop.Stopper) (*Store, *hlc.ManualClock) {
	manual := hlc.NewManualClock(123)
	cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	store := createTestStoreWithConfig(t, stopper, &cfg)
	return store, manual
}

// createTestStore creates a test store using an in-memory
// engine. It returns the store, the store clock's manual unix nanos time
// and a stopper. The caller is responsible for stopping the stopper
// upon completion.
func createTestStoreWithConfig(t testing.TB, stopper *stop.Stopper, cfg *StoreConfig) *Store {
	store := createTestStoreWithoutStart(t, stopper, cfg)
	// Put an empty system config into gossip.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig,
		&config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.Start(context.Background(), stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()
	return store
}

// TestStoreInitAndBootstrap verifies store initialization and bootstrap.
func TestStoreInitAndBootstrap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We need a fixed clock to avoid LastUpdateNanos drifting on us.
	cfg := TestStoreConfig(hlc.NewClock(func() int64 { return 123 }, time.Nanosecond))
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	stopper.AddCloser(eng)
	cfg.Transport = NewDummyRaftTransport()

	{
		store := NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})
		// Can't start as haven't bootstrapped.
		if err := store.Start(context.Background(), stopper); err == nil {
			t.Error("expected failure starting un-bootstrapped store")
		}

		// Bootstrap with a fake ident.
		if err := store.Bootstrap(testIdent); err != nil {
			t.Errorf("error bootstrapping store: %s", err)
		}

		// Verify we can read the store ident after a flush.
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadStoreIdent(context.Background(), eng); err != nil {
			t.Fatalf("unable to read store ident: %s", err)
		}

		// Try to get 1st range--non-existent.
		if _, err := store.GetReplica(1); err == nil {
			t.Error("expected error fetching non-existent range")
		}

		// Bootstrap first range.
		if err := store.BootstrapRange(nil); err != nil {
			t.Errorf("failure to create first range: %s", err)
		}
	}

	// Now, attempt to initialize a store with a now-bootstrapped range.
	{
		store := NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})
		if err := store.Start(context.Background(), stopper); err != nil {
			t.Fatalf("failure initializing bootstrapped store: %s", err)
		}
		// 1st range should be available.
		r, err := store.GetReplica(1)
		if err != nil {
			t.Fatalf("failure fetching 1st range: %s", err)
		}
		rs := r.GetMVCCStats()

		// Stats should agree with a recomputation.
		now := r.store.Clock().Now()
		if ms, err := ComputeStatsForRange(r.Desc(), eng, now.WallTime); err != nil {
			t.Errorf("failure computing range's stats: %s", err)
		} else if ms != rs {
			t.Errorf("expected range's stats to agree with recomputation: %s", pretty.Diff(ms, rs))
		}
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	stopper.AddCloser(eng)

	// Put some random garbage into the engine.
	if err := eng.Put(engine.MakeMVCCMetadataKey(roachpb.Key("foo")), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %s", err)
	}
	cfg := TestStoreConfig(nil)
	cfg.Transport = NewDummyRaftTransport()
	store := NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})

	// Can't init as haven't bootstrapped.
	switch err := errors.Cause(store.Start(context.Background(), stopper)); err.(type) {
	case *NotBootstrappedError:
	default:
		t.Errorf("unexpected error initializing un-bootstrapped store: %v", err)
	}

	// Bootstrap should fail on non-empty engine.
	switch err := errors.Cause(store.Bootstrap(testIdent)); err.(type) {
	case *NotBootstrappedError:
	default:
		t.Errorf("unexpected error bootstrapping non-empty store: %v", err)
	}
}

// create a Replica and add it to the store. Note that replicas
// created in this way do not have their raft groups fully initialized
// so most KV operations will not work on them. This function is
// deprecated; new tests should create replicas by splitting from a
// properly-bootstrapped initial range.
func createReplica(s *Store, rangeID roachpb.RangeID, start, end roachpb.RKey) *Replica {
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
		log.Fatal(context.Background(), err)
	}
	return r
}

func TestStoreAddRemoveRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	if _, err := store.GetReplica(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Error(err)
	}
	// Create a new range (id=2).
	repl2 := createReplica(store, 2, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplica(repl2); err != nil {
		t.Fatal(err)
	}
	// Try to add the same range twice
	err = store.AddReplica(repl2)
	if err == nil {
		t.Fatal("expected error re-adding same range")
	}
	// Try to remove range 1 again.
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err == nil {
		t.Fatal("expected error re-removing same range")
	}
	// Try to add a range with previously-used (but now removed) ID.
	repl2Dup := createReplica(store, 1, roachpb.RKey("a"), roachpb.RKey("b"))
	if err := store.AddReplica(repl2Dup); err == nil {
		t.Fatal("expected error inserting a duplicated range")
	}
	// Add another range with different key range and then test lookup.
	repl3 := createReplica(store, 3, roachpb.RKey("c"), roachpb.RKey("d"))
	if err := store.AddReplica(repl3); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		start, end roachpb.RKey
		expRng     *Replica
	}{
		{roachpb.RKey("a"), roachpb.RKey("a\x00"), repl2},
		{roachpb.RKey("a"), roachpb.RKey("b"), repl2},
		{roachpb.RKey("a\xff\xff"), roachpb.RKey("b"), repl2},
		{roachpb.RKey("c"), roachpb.RKey("c\x00"), repl3},
		{roachpb.RKey("c"), roachpb.RKey("d"), repl3},
		{roachpb.RKey("c\xff\xff"), roachpb.RKey("d"), repl3},
		{roachpb.RKey("x60\xff\xff"), roachpb.RKey("a"), nil},
		{roachpb.RKey("x60\xff\xff"), roachpb.RKey("a\x00"), nil},
		{roachpb.RKey("d"), roachpb.RKey("d"), nil},
		{roachpb.RKey("c\xff\xff"), roachpb.RKey("d\x00"), nil},
		{roachpb.RKey("a"), nil, repl2},
		{roachpb.RKey("d"), nil, nil},
	}

	for i, test := range testCases {
		if r := store.LookupReplica(test.start, test.end); r != test.expRng {
			t.Errorf("%d: expected range %v; got %v", i, test.expRng, r)
		}
	}
}

// TestReplicasByKey tests that operations that depend on the
// store.replicasByKey map function correctly when the underlying replicas'
// start and end keys are manipulated in place. This mutation happens when a
// snapshot is applied that advances a replica past a split.
func TestReplicasByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	// Shrink the main replica.
	rep, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	rep.mu.Lock()
	desc := *rep.mu.state.Desc // shallow copy to replace desc wholesale
	desc.EndKey = roachpb.RKey("e")
	rep.mu.state.Desc = &desc
	rep.mu.Unlock()

	// Ensure that this shrinkage is recognized by future additions to replicasByKey.
	reps := []*struct {
		replica            *Replica
		id                 int
		start, end         roachpb.RKey
		expectedErrorOnAdd string
	}{
		// [a,c) is contained in [KeyMin, e)
		{nil, 2, roachpb.RKey("a"), roachpb.RKey("c"), ".*has overlapping range"},
		// [c,f) partially overlaps with [KeyMin, e)
		{nil, 3, roachpb.RKey("c"), roachpb.RKey("f"), ".*has overlapping range"},
		// [e, f) is disjoint from [KeyMin, e)
		{nil, 4, roachpb.RKey("e"), roachpb.RKey("f"), ""},
	}

	for i, desc := range reps {
		desc.replica = createReplica(store, roachpb.RangeID(desc.id), desc.start, desc.end)
		err := store.AddReplica(desc.replica)
		if !testutils.IsError(err, desc.expectedErrorOnAdd) {
			t.Fatalf("adding replica %d: expected err %q, but encountered %v", i, desc.expectedErrorOnAdd, err)
		}
	}
}

func TestStoreRemoveReplicaOldDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	rep, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// First try and fail with a stale descriptor.
	origDesc := rep.Desc()
	newDesc := protoutil.Clone(origDesc).(*roachpb.RangeDescriptor)
	for i := range newDesc.Replicas {
		if newDesc.Replicas[i].StoreID == store.StoreID() {
			newDesc.Replicas[i].ReplicaID++
			newDesc.NextReplicaID++
			break
		}
	}

	if err := rep.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveReplica(context.Background(), rep, *origDesc, true); !testutils.IsError(err, "replica ID has changed") {
		t.Fatalf("expected error 'replica ID has changed' but got %v", err)
	}

	// Now try a fresh descriptor and succeed.
	if err := store.RemoveReplica(context.Background(), rep, *rep.Desc(), true); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRemoveReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Fatal(err)
	}

	// Verify that removal of a replica marks it as destroyed so that future raft
	// commands on the Replica will silently be dropped.
	if err := repl1.withRaftGroup(func(r *raft.RawNode) (bool, error) {
		return true, errors.Errorf("unexpectedly created a raft group")
	}); err != nil {
		t.Fatal(err)
	}

	repl1.mu.Lock()
	expErr := repl1.mu.destroyed
	lease := repl1.mu.state.Lease
	repl1.mu.Unlock()

	if expErr == nil {
		t.Fatal("replica was not marked as destroyed")
	}

	if _, _, err := repl1.propose(
		context.Background(), lease, roachpb.BatchRequest{}, nil,
	); err != expErr {
		t.Fatalf("expected error %s, but got %v", expErr, err)
	}
}

func TestStoreReplicaVisitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	// Remove range 1.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Error(err)
	}

	// Add 10 new ranges.
	const newCount = 10
	for i := 0; i < newCount; i++ {
		repl := createReplica(store, roachpb.RangeID(i+1), roachpb.RKey(fmt.Sprintf("a%02d", i)), roachpb.RKey(fmt.Sprintf("a%02d", i+1)))
		if err := store.AddReplica(repl); err != nil {
			t.Fatal(err)
		}
	}

	// Verify two passes of the visit.
	visitor := newStoreReplicaVisitor(store)
	exp := make(map[roachpb.RangeID]struct{})
	for i := 0; i < newCount; i++ {
		exp[roachpb.RangeID(i+1)] = struct{}{}
	}

	for pass := 0; pass < 2; pass++ {
		if ec := visitor.EstimatedCount(); ec != 10 {
			t.Fatalf("expected 10 remaining; got %d", ec)
		}
		i := 1
		seen := make(map[roachpb.RangeID]struct{})
		visitor.Visit(func(repl *Replica) bool {
			_, ok := seen[repl.RangeID]
			if ok {
				t.Fatalf("already saw %d", repl.RangeID)
			}

			seen[repl.RangeID] = struct{}{}
			if ec := visitor.EstimatedCount(); ec != 10-i {
				t.Fatalf(
					"expected %d remaining; got %d after seeing %+v",
					10-i, ec, seen,
				)
			}
			i++
			return true
		})
		if ec := visitor.EstimatedCount(); ec != 10 {
			t.Fatalf("expected 10 remaining; got %d", ec)
		}
		if !reflect.DeepEqual(exp, seen) {
			t.Fatalf("got %v, expected %v", seen, exp)
		}
	}
}

func TestHasOverlappingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	if _, err := store.GetReplica(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
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
		repl := createReplica(store, roachpb.RangeID(desc.id), desc.start, desc.end)
		if err := store.AddReplica(repl); err != nil {
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
		if r := store.getOverlappingKeyRangeLocked(rngDesc) != nil; r != test.exp {
			t.Errorf("%d: expected range %v; got %v", i, test.exp, r)
		}
	}
}

func TestProcessRangeDescriptorUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	// Clobber the existing range so we can test overlaps that aren't KeyMin or KeyMax.
	repl1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Error(err)
	}

	repl := createReplica(store, roachpb.RangeID(2), roachpb.RKey("a"), roachpb.RKey("c"))
	if err := store.AddReplica(repl); err != nil {
		t.Fatal(err)
	}

	newRangeID := roachpb.RangeID(3)
	desc := &roachpb.RangeDescriptor{
		RangeID: newRangeID,
		Replicas: []roachpb.ReplicaDescriptor{{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 1,
		}},
		NextReplicaID: 2,
	}

	r := &Replica{
		RangeID:     desc.RangeID,
		stateLoader: makeReplicaStateLoader(desc.RangeID),
		store:       store,
		abortCache:  NewAbortCache(desc.RangeID),
	}
	r.mu.timedMutex = makeTimedMutex(defaultMuLogger)
	r.cmdQMu.timedMutex = makeTimedMutex(defaultMuLogger)
	if err := r.init(desc, store.Clock(), 0); err != nil {
		t.Fatal(err)
	}

	expectedResult := "attempted to process uninitialized range.*"
	ctx := r.AnnotateCtx(context.TODO())
	if err := store.processRangeDescriptorUpdate(ctx, r); !testutils.IsError(err, expectedResult) {
		t.Errorf("expected processRangeDescriptorUpdate with uninitialized replica to fail, got %v", err)
	}

	// Initialize the range with start and end keys.
	r.mu.Lock()
	r.mu.state.Desc.StartKey = roachpb.RKey("b")
	r.mu.state.Desc.EndKey = roachpb.RKey("d")
	r.mu.Unlock()

	if err := store.processRangeDescriptorUpdateLocked(ctx, r); err != nil {
		t.Errorf("expected processRangeDescriptorUpdate on a replica that's not in the uninit map to silently succeed, got %v", err)
	}

	store.mu.Lock()
	store.mu.uninitReplicas[newRangeID] = r
	store.mu.Unlock()

	expectedResult = ".*cannot processRangeDescriptorUpdate.*"
	if err := store.processRangeDescriptorUpdate(ctx, r); !testutils.IsError(err, expectedResult) {
		t.Errorf("expected processRangeDescriptorUpdate with overlapping keys to fail, got %v", err)
	}
}

// TestStoreSend verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreSend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	gArgs := getArgs([]byte("a"))

	// Try a successful get request.
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &gArgs); pErr != nil {
		t.Fatal(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("aaa"))
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs); pErr != nil {
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

	testCases := []struct {
		key   roachpb.Key
		check func(int64, roachpb.Response, *roachpb.Error)
	}{
		{badKey,
			func(wallNanos int64, _ roachpb.Response, pErr *roachpb.Error) {
				if pErr == nil {
					t.Fatal("expected an error")
				}
				txn := pErr.GetTxn()
				if txn == nil || txn.ID == nil {
					t.Fatalf("expected nontrivial transaction in %s", pErr)
				}
				if ts, _ := txn.GetObservedTimestamp(desc.NodeID); ts.WallTime != wallNanos {
					t.Fatalf("unexpected observed timestamps, expected %d->%d but got map %+v",
						desc.NodeID, wallNanos, txn.ObservedTimestamps)
				}
				if pErr.OriginNode != desc.NodeID {
					t.Fatalf("unexpected OriginNode %d, expected %d",
						pErr.OriginNode, desc.NodeID)
				}

			}},
		{goodKey,
			func(wallNanos int64, pReply roachpb.Response, pErr *roachpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				txn := pReply.Header().Txn
				if txn == nil || txn.ID == nil {
					t.Fatal("expected transactional response")
				}
				obs, _ := txn.GetObservedTimestamp(desc.NodeID)
				if act, exp := obs.WallTime, wallNanos; exp != act {
					t.Fatalf("unexpected observed wall time: %d, wanted %d", act, exp)
				}
			}},
	}

	for _, test := range testCases {
		func() {
			manual := hlc.NewManualClock(123)
			cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
			cfg.TestingKnobs.TestingEvalFilter =
				func(filterArgs storagebase.FilterArgs) *roachpb.Error {
					if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
						return roachpb.NewError(errors.Errorf("boom"))
					}
					return nil
				}
			stopper := stop.NewStopper()
			defer stopper.Stop()
			store := createTestStoreWithConfig(t, stopper, &cfg)
			txn := newTransaction("test", test.key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
			txn.MaxTimestamp = hlc.MaxTimestamp
			pArgs := putArgs(test.key, []byte("value"))
			h := roachpb.Header{
				Txn:     txn,
				Replica: desc,
			}
			pReply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &pArgs)
			test.check(manual.UnixNano(), pReply, pErr)
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
				if pErr.Now == (hlc.Timestamp{}) {
					t.Fatal("timestamp not annotated on error")
				}
			}},
		{goodKey,
			func(pReply *roachpb.BatchResponse, pErr *roachpb.Error) {
				if pErr != nil {
					t.Fatal(pErr)
				}
				if pReply.Now == (hlc.Timestamp{}) {
					t.Fatal("timestamp not annotated on batch response")
				}
			}},
	}

	for _, useTxn := range []bool{false, true} {
		for _, test := range testCases {
			func() {
				cfg := TestStoreConfig(nil)
				cfg.TestingKnobs.TestingEvalFilter =
					func(filterArgs storagebase.FilterArgs) *roachpb.Error {
						if bytes.Equal(filterArgs.Req.Header().Key, badKey) {
							return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
						}
						return nil
					}
				stopper := stop.NewStopper()
				defer stopper.Stop()
				store := createTestStoreWithConfig(t, stopper, &cfg)
				var txn *roachpb.Transaction
				if useTxn {
					txn = newTransaction("test", test.key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
					txn.MaxTimestamp = hlc.MaxTimestamp
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	// Try a start key == KeyMax.
	gArgs := getArgs(roachpb.KeyMax)
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("expected error for start key == KeyMax: %v", pErr)
	}
	// Try a get with an end key specified (get requires only a start key and should fail).
	gArgs.EndKey = roachpb.KeyMax
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &gArgs); !testutils.IsPError(pErr, "must be less than KeyMax") {
		t.Fatalf("unexpected error for end key specified on a non-range-based operation: %v", pErr)
	}
	// Try a scan with end key < start key.
	sArgs := scanArgs([]byte("b"), []byte("a"))
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for end key < start: %v", pErr)
	}
	// Try a scan with start key == end key.
	sArgs.Key = []byte("a")
	sArgs.EndKey = sArgs.Key
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &sArgs); !testutils.IsPError(pErr, "must be greater than") {
		t.Fatalf("unexpected error for start == end key: %v", pErr)
	}
	// Try a scan with range-local start key, but "regular" end key.
	sArgs.Key = keys.MakeRangeKey([]byte("test"), []byte("sffx"), nil)
	sArgs.EndKey = []byte("z")
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &sArgs); !testutils.IsPError(pErr, "range-local") {
		t.Fatalf("unexpected error for local start, non-local end key: %v", pErr)
	}

	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	meta2KeyMax := testutils.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax)
	pArgs := putArgs(meta2KeyMax, []byte("value"))
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", pErr)
	}
	// Try to put a range descriptor record for a start key which is
	// maximum length.
	key := append([]byte{}, roachpb.RKeyMax...)
	key[len(key)-1] = 0x01
	pArgs = putArgs(keys.RangeDescriptorKey(key), []byte("value"))
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to range descriptor for KeyMax value: %s", pErr)
	}
	// Try a put to txn record for a meta2 key (note that this doesn't
	// actually happen in practice, as txn records are not put directly,
	// but are instead manipulated only through txn methods).
	pArgs = putArgs(keys.TransactionKey(meta2KeyMax, uuid.MakeV4()), []byte("value"))
	if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs); pErr != nil {
		t.Fatalf("unexpected error on put to txn meta2 value: %s", pErr)
	}
}

// TestStoreSendUpdateTime verifies that the node clock is updated.
func TestStoreSendUpdateTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	args := getArgs([]byte("a"))
	reqTS := store.cfg.Clock.Now().Add(store.cfg.Clock.MaxOffset().Nanoseconds(), 0)
	_, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{Timestamp: reqTS}, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	ts := store.cfg.Clock.Now()
	if ts.WallTime != reqTS.WallTime || ts.Logical <= reqTS.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", reqTS, ts)
	}
}

// TestStoreSendWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreSendWithZeroTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	args := getArgs([]byte("a"))

	_, respH, pErr := SendWrapped(context.Background(), store.testSender(), roachpb.Header{}, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if respH.Timestamp.WallTime != store.cfg.Clock.Now().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.cfg.Clock.Now(), respH.Timestamp)
	}
}

// TestStoreSendWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails.
func TestStoreSendWithClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	args := getArgs([]byte("a"))
	// Set args timestamp to exceed max offset.
	reqTS := store.cfg.Clock.Now().Add(store.cfg.Clock.MaxOffset().Nanoseconds()+1, 0)
	_, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{Timestamp: reqTS}, &args)
	if !testutils.IsPError(pErr, "rejecting command with timestamp in the future") {
		t.Errorf("unexpected error: %v", pErr)
	}
}

// TestStoreSendBadRange passes a bad range.
func TestStoreSendBadRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)
	args := getArgs([]byte("0"))
	if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
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
	repl := store.LookupReplica(key, nil)
	if repl == nil {
		t.Fatalf("couldn't lookup range for key %q", key)
	}
	desc, err := store.NewRangeDescriptor(splitKey, repl.Desc().EndKey, repl.Desc().Replicas)
	if err != nil {
		t.Fatal(err)
	}
	// Minimal amount of work to keep this deprecated machinery working: Write
	// some required Raft keys.
	if _, err := writeInitialState(
		context.Background(), store.engine, enginepb.MVCCStats{}, *desc, raftpb.HardState{}, &roachpb.Lease{},
	); err != nil {
		t.Fatal(err)
	}
	newRng, err := NewReplica(desc, store, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SplitRange(repl.AnnotateCtx(context.TODO()), repl, newRng); err != nil {
		t.Fatal(err)
	}
	return newRng
}

// TestStoreSendOutOfRange passes a key not contained
// within the range's key range.
func TestStoreSendOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	repl2 := splitTestRange(store, roachpb.RKeyMin, roachpb.RKey(roachpb.Key("b")), t)

	// Range 1 is from KeyMin to "b", so reading "b" from range 1 should
	// fail because it's just after the range boundary.
	args := getArgs([]byte("b"))
	if _, err := client.SendWrapped(context.Background(), store.testSender(), &args); err == nil {
		t.Error("expected key to be out of range")
	}

	// Range 2 is from "b" to KeyMax, so reading "a" from range 2 should
	// fail because it's before the start of the range.
	args = getArgs([]byte("a"))
	if _, err := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
		RangeID: repl2.RangeID,
	}, &args); err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRangeIDAllocation verifies that  range IDs are
// allocated in successive blocks.
func TestStoreRangeIDAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	// Range IDs should be allocated from ID 2 (first allocated range)
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

// TestStoreReplicasByKey verifies we can lookup ranges by key using
// the sorted replicasByKey slice.
func TestStoreReplicasByKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	r0 := store.LookupReplica(roachpb.RKeyMin, nil)
	r1 := splitTestRange(store, roachpb.RKeyMin, roachpb.RKey("A"), t)
	r2 := splitTestRange(store, roachpb.RKey("A"), roachpb.RKey("C"), t)
	r3 := splitTestRange(store, roachpb.RKey("C"), roachpb.RKey("X"), t)
	r4 := splitTestRange(store, roachpb.RKey("X"), roachpb.RKey("ZZ"), t)

	if r := store.LookupReplica(roachpb.RKey("0"), nil); r != r0 {
		t.Errorf("mismatched replica %s != %s", r, r0)
	}
	if r := store.LookupReplica(roachpb.RKey("B"), nil); r != r1 {
		t.Errorf("mismatched replica %s != %s", r, r1)
	}
	if r := store.LookupReplica(roachpb.RKey("C"), nil); r != r2 {
		t.Errorf("mismatched replica %s != %s", r, r2)
	}
	if r := store.LookupReplica(roachpb.RKey("M"), nil); r != r2 {
		t.Errorf("mismatched replica %s != %s", r, r2)
	}
	if r := store.LookupReplica(roachpb.RKey("X"), nil); r != r3 {
		t.Errorf("mismatched replica %s != %s", r, r3)
	}
	if r := store.LookupReplica(roachpb.RKey("Z"), nil); r != r3 {
		t.Errorf("mismatched replica %s != %s", r, r3)
	}
	if r := store.LookupReplica(roachpb.RKey("ZZ"), nil); r != r4 {
		t.Errorf("mismatched replica %s != %s", r, r4)
	}
	if r := store.LookupReplica(roachpb.RKey("\xff\x00"), nil); r != r4 {
		t.Errorf("mismatched replica %s != %s", r, r4)
	}
	if store.LookupReplica(roachpb.RKeyMax, nil) != nil {
		t.Errorf("expected roachpb.KeyMax to not have an associated replica")
	}
}

// TestStoreSetRangesMaxBytes creates a set of ranges via splitting
// and then sets the config zone to a custom max bytes value to
// verify the ranges' max bytes are updated appropriately.
func TestStoreSetRangesMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	baseID := uint32(keys.MaxReservedDescID + 1)
	testData := []struct {
		repl        *Replica
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
	config.TestingSetZoneConfig(baseID, config.ZoneConfig{RangeMaxBytes: 1 << 20})
	config.TestingSetZoneConfig(baseID+2, config.ZoneConfig{RangeMaxBytes: 2 << 20})

	// Despite faking the zone configs, we still need to have a gossip entry.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		for _, test := range testData {
			if mb := test.repl.GetMaxBytes(); mb != test.expMaxBytes {
				return errors.Errorf("range max bytes values did not change to %d; got %d", test.expMaxBytes, mb)
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
	storeCfg := TestStoreConfig(nil)
	storeCfg.DontRetryPushTxnFailures = true
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithConfig(t, stopper, &storeCfg)

	for i, iso := range []enginepb.IsolationType{enginepb.SERIALIZABLE, enginepb.SNAPSHOT} {
		key := roachpb.Key(fmt.Sprintf("a-%d", i))
		txn := newTransaction("test", key, 1, iso, store.cfg.Clock)

		for retry := 0; ; retry++ {
			if retry > 1 {
				t.Fatalf("%d: too many retries", i)
			}
			// Always send non-transactional put to push the transaction
			// and write a non-intent version.
			nakedPut := putArgs(key, []byte("naked"))
			_, pErr := client.SendWrapped(context.Background(), store.testSender(), &nakedPut)
			if pErr != nil && retry == 0 {
				t.Fatalf("%d: unexpected error on first put: %s", i, pErr)
			} else if retry == 1 {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
					t.Fatalf("%d: expected TransactionPushError; got %s", i, pErr)
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
			txn.Restart(1, 1, store.cfg.Clock.Now())
		}
	}
}

// TestStoreResolveWriteIntent adds a write intent and then verifies
// that a put returns success and aborts intent's txn in the event the
// pushee has lower priority. Otherwise, verifies that the put blocks
// until the original txn is ended.
func TestStoreResolveWriteIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := hlc.NewManualClock(123)
	cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			pr, ok := filterArgs.Req.(*roachpb.PushTxnRequest)
			if !ok || pr.PusherTxn.Name != "test" {
				return nil
			}
			if exp, act := manual.UnixNano(), pr.PushTo.WallTime; exp > act {
				return roachpb.NewError(fmt.Errorf("expected PushTo >= WallTime, but got %d < %d:\n%+v", act, exp, pr))
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithConfig(t, stopper, &cfg)

	for i, resolvable := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
		if resolvable {
			pushee.Priority = roachpb.MinTxnPriority
			pusher.Priority = roachpb.MaxTxnPriority // Pusher will win.
		} else {
			pushee.Priority = roachpb.MaxTxnPriority
			pusher.Priority = roachpb.MinTxnPriority // Pusher will lose.
		}

		// First lay down intent using the pushee's txn.
		pArgs := putArgs(key, []byte("value"))
		h := roachpb.Header{Txn: pushee}
		pushee.Sequence++
		if _, err := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), h, &pArgs); err != nil {
			t.Fatal(err)
		}

		manual.Increment(100)
		// Now, try a put using the pusher's txn.
		h.Txn = pusher
		resultCh := make(chan *roachpb.Error, 1)
		go func() {
			_, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &pArgs)
			resultCh <- pErr
		}()

		if resolvable {
			pErr := <-resultCh
			if pErr != nil {
				t.Fatalf("expected intent resolved; got unexpected error: %s", pErr)
			}
			txnKey := keys.TransactionKey(pushee.Key, *pushee.ID)
			var txn roachpb.Transaction
			ok, err := engine.MVCCGetProto(context.Background(), store.Engine(), txnKey, hlc.Timestamp{}, true, nil, &txn)
			if !ok || err != nil {
				t.Fatalf("not found or err: %s", err)
			}
			if txn.Status != roachpb.ABORTED {
				t.Fatalf("expected pushee to be aborted; got %s", txn.Status)
			}
		} else {
			var pErr *roachpb.Error
			select {
			case pErr = <-resultCh:
				t.Fatalf("did not expect put to complete with lower priority: %s", pErr)
			case <-time.After(10 * time.Millisecond):
				// Send an end transaction to allow the original push to complete.
				etArgs, h := endTxnArgs(pushee, true)
				pushee.Sequence++
				_, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs)
				if pErr != nil {
					t.Fatal(pErr)
				}
				pErr = <-resultCh
			}
			if pErr != nil {
				t.Fatalf("expected successful put after pushee txn ended; got %s", pErr)
			}
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	key := roachpb.Key("a")
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
	pushee.Priority = roachpb.MinTxnPriority
	pusher.Priority = roachpb.MaxTxnPriority // Pusher will win.

	// First lay down intent using the pushee's txn.
	args := incrementArgs(key, 1)
	h := roachpb.Header{Txn: pushee}
	pushee.Sequence++
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), h, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try a put using the pusher's txn.
	h.Txn = pusher
	args.Increment = 2
	if resp, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &args); pErr != nil {
		t.Errorf("expected increment to succeed: %s", pErr)
	} else if reply := resp.(*roachpb.IncrementResponse); reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a write intent
// for a read will push the timestamp. On failure to push, verify a write
// intent error is returned with !Resolvable.
//
// TODO(tschottdorf): this test (but likely a lot of others) always need to
// manually update the transaction for each received response, or they behave
// like real clients aren't allowed to (for instance, dropping WriteTooOld
// flags or timestamp bumps).
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := TestStoreConfig(nil)
	storeCfg.DontRetryPushTxnFailures = true
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithConfig(t, stopper, &storeCfg)

	testCases := []struct {
		resolvable bool
		pusheeIso  enginepb.IsolationType
	}{
		// Resolvable is true, so we can read, but SERIALIZABLE means we can't commit.
		{true, enginepb.SERIALIZABLE},
		// Pushee is SNAPSHOT, meaning we can commit.
		{true, enginepb.SNAPSHOT},
		// Resolvable is false and SERIALIZABLE so can't read.
		{false, enginepb.SERIALIZABLE},
		// Resolvable is false, but SNAPSHOT means we can push it anyway, so can read.
		{false, enginepb.SNAPSHOT},
	}
	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
		pushee := newTransaction("test", key, 1, test.pusheeIso, store.cfg.Clock)

		if test.resolvable {
			pushee.Priority = roachpb.MinTxnPriority
			pusher.Priority = roachpb.MaxTxnPriority // Pusher will win.
		} else {
			pushee.Priority = roachpb.MaxTxnPriority
			pusher.Priority = roachpb.MinTxnPriority // Pusher will lose.
		}
		// First, write original value.
		{
			args := putArgs(key, []byte("value1"))
			if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &args); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Second, lay down intent using the pushee's txn.
		{
			_, btH := beginTxnArgs(key, pushee)
			args := putArgs(key, []byte("value2"))
			if reply, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), btH, &args); pErr != nil {
				t.Fatal(pErr)
			} else {
				pushee.Update(reply.(*roachpb.PutResponse).Txn)
				if pushee.WriteTooOld {
					// See test comment for the TODO mentioned below.
					t.Logf("%d: unsetting WriteTooOld flag as a hack to keep this test passing; should address the TODO", i)
					pushee.WriteTooOld = false
				}
			}
		}

		// Now, try to read value using the pusher's txn.
		now := store.Clock().Now()
		pusher.OrigTimestamp.Forward(now)
		pusher.Timestamp.Forward(now)
		gArgs := getArgs(key)
		firstReply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{Txn: pusher}, &gArgs)
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
			reply, cErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs)

			minExpTS := pusher.Timestamp
			minExpTS.Logical++
			if test.pusheeIso == enginepb.SNAPSHOT {
				if cErr != nil {
					t.Fatalf("unexpected error on commit: %s", cErr)
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
			if test.pusheeIso == enginepb.SNAPSHOT {
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
				if _, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
					t.Errorf("iso=%s; expected transaction push error; got %T", test.pusheeIso, pErr.GetDetail())
				}
			}
		}
	}
}

// TestStoreResolveWriteIntentSnapshotIsolation verifies that the
// timestamp can always be pushed if txn has snapshot isolation.
func TestStoreResolveWriteIntentSnapshotIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	key := roachpb.Key("a")

	// First, write original value.
	args := putArgs(key, []byte("value1"))
	ts := store.cfg.Clock.Now()
	if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{Timestamp: ts}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Lay down intent using the pushee's txn.
	pushee := newTransaction("test", key, 1, enginepb.SNAPSHOT, store.cfg.Clock)
	h := roachpb.Header{Txn: pushee}
	args.Value.SetBytes([]byte("value2"))
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), h, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try to read value using the pusher's txn.
	gArgs := getArgs(key)
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)
	h.Txn = pusher
	if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &gArgs); pErr != nil {
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
	reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs)
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, store.cfg.Clock)

	// First, lay down intent from pushee.
	args := putArgs(key, []byte("value1"))
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), roachpb.Header{Txn: pushee}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, try to read outside a transaction.
	getTS := store.cfg.Clock.Now() // accessed later
	{
		gArgs := getArgs(key)
		if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
			Timestamp:    getTS,
			UserPriority: roachpb.MaxUserPriority,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			t.Errorf("expected value to be nil, got %+v", gReply.Value)
		}
	}

	{
		// Next, try to write outside of a transaction. We will succeed in pushing txn.
		putTS := store.cfg.Clock.Now()
		args.Value.SetBytes([]byte("value2"))
		if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
			Timestamp:    putTS,
			UserPriority: roachpb.MaxUserPriority,
		}, &args); pErr != nil {
			t.Errorf("expected success aborting pushee's txn; got %s", pErr)
		}
	}

	// Read pushee's txn.
	txnKey := keys.TransactionKey(pushee.Key, *pushee.ID)
	var txn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(context.Background(), store.Engine(), txnKey, hlc.Timestamp{}, true, nil, &txn); !ok || err != nil {
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
	// to MaxTxnPriority-1 during push.
	if txn.Priority != roachpb.MaxTxnPriority-1 {
		t.Errorf("expected pushee priority to be pushed to %d; got %d", roachpb.MaxTxnPriority-1, txn.Priority)
	}

	// Finally, try to end the pushee's transaction; it should have
	// been aborted.
	etArgs, h := endTxnArgs(pushee, true)
	pushee.Sequence++
	_, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs)
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	for _, canPush := range []bool{true, false} {
		keyA := roachpb.Key(fmt.Sprintf("%t-a", canPush))
		keyB := roachpb.Key(fmt.Sprintf("%t-b", canPush))

		// First, write keyA.
		args := putArgs(keyA, []byte("value1"))
		if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &args); pErr != nil {
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
		txnA := newTransaction("testA", keyA, priority, enginepb.SERIALIZABLE, store.cfg.Clock)
		txnB := newTransaction("testB", keyB, priority, enginepb.SERIALIZABLE, store.cfg.Clock)
		for _, txn := range []*roachpb.Transaction{txnA, txnB} {
			args.Key = txn.Key
			if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), roachpb.Header{Txn: txn}, &args); pErr != nil {
				t.Fatal(pErr)
			}
		}
		// End txn B, but without resolving the intent.
		etArgs, h := endTxnArgs(txnB, true)
		txnB.Sequence++
		if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, get from both keys and verify. Whether we can push or not, we
		// will be able to read with INCONSISTENT.
		gArgs := getArgs(keyA)

		if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(replyBytes, []byte("value1")) {
			t.Errorf("expected value %q, got %+v", []byte("value1"), reply)
		}
		gArgs.Key = keyB

		if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); pErr != nil {
			t.Errorf("expected read to succeed: %s", pErr)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			// The new value of B will not be read at first.
			t.Errorf("expected value nil, got %+v", gReply.Value)
		}
		// However, it will be read eventually, as B's intent can be
		// resolved asynchronously as txn B is committed.
		testutils.SucceedsSoon(t, func() error {
			if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
				ReadConsistency: roachpb.INCONSISTENT,
			}, &gArgs); pErr != nil {
				return errors.Errorf("expected read to succeed: %s", pErr)
			} else if gReply := reply.(*roachpb.GetResponse).Value; gReply == nil {
				return errors.Errorf("value is nil")
			} else if replyBytes, err := gReply.GetBytes(); err != nil {
				return err
			} else if !bytes.Equal(replyBytes, []byte("value2")) {
				return errors.Errorf("expected value %q, got %+v", []byte("value2"), reply)
			}
			return nil
		})

		// Scan keys and verify results.
		sArgs := scanArgs(keyA, keyB.Next())
		reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
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

	cfg := TestStoreConfig(nil)
	var count int32
	countPtr := &count

	cfg.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if _, ok := filterArgs.Req.(*roachpb.ScanRequest); ok {
				atomic.AddInt32(countPtr, 1)
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithConfig(t, stopper, &cfg)

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
				priority := roachpb.UserPriority(1)
				if test.canPush {
					priority = roachpb.MinUserPriority
				}
				txn = newTransaction(fmt.Sprintf("test-%d", i), key, priority, enginepb.SERIALIZABLE, store.cfg.Clock)
			}
			args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
			if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), roachpb.Header{Txn: txn}, &args); pErr != nil {
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
		errChan := make(chan *roachpb.Error, 1)
		go func() {
			reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
				Timestamp:       ts,
				ReadConsistency: consistency,
			}, &sArgs)
			if pErr == nil {
				sReply = reply.(*roachpb.ScanResponse)
			}
			errChan <- pErr
		}()

		wait := 1 * time.Second
		if !test.expFinish {
			wait = 10 * time.Millisecond
		}
		select {
		case pErr := <-errChan:
			if pErr != nil {
				t.Fatal(pErr)
			}
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
				if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs); pErr != nil {
					t.Fatal(pErr)
				}
				<-errChan
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
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			_, ok := filterArgs.Req.(*roachpb.ResolveIntentRequest)
			if ok && intercept.Load().(bool) {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithConfig(t, stopper, &cfg)

	// Lay down 10 intents to scan over.
	txn := newTransaction("test", roachpb.Key("foo"), 1, enginepb.SERIALIZABLE, store.cfg.Clock)
	keys := []roachpb.Key{}
	for j := 0; j < 10; j++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", j))
		keys = append(keys, key)
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
		txn.Sequence++
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), store.testSender(), roachpb.Header{Txn: txn}, &args); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true
	}

	// Now, commit txn without resolving intents. If we hadn't disabled auto-gc
	// of Txn entries in this test, the Txn entry would be removed and later
	// attempts to resolve the intents would fail.
	etArgs, h := endTxnArgs(txn, true)
	txn.Sequence++
	if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), h, &etArgs); pErr != nil {
		t.Fatal(pErr)
	}

	intercept.Store(false) // allow async intent resolution

	// Scan the range repeatedly until we've verified count.
	sArgs := scanArgs(keys[0], keys[9].Next())
	testutils.SucceedsSoon(t, func() error {
		if reply, pErr := client.SendWrappedWith(context.Background(), store.testSender(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &sArgs); pErr != nil {
			return pErr.GoError()
		} else if sReply := reply.(*roachpb.ScanResponse); len(sReply.Rows) != 10 {
			return errors.Errorf("could not read rows as expected")
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store, _ := createTestStore(t, stopper)

	txn := newTransaction("test", roachpb.Key("a"), 1 /* priority */, enginepb.SERIALIZABLE, store.cfg.Clock)

	args1 := getArgs(roachpb.Key("a"))
	args1.EndKey = roachpb.Key("b")

	args2 := getArgs(roachpb.RKeyMax)

	args3 := scanArgs(roachpb.Key("a"), roachpb.Key("a"))
	args4 := scanArgs(roachpb.Key("b"), roachpb.Key("a"))

	args5 := scanArgs(roachpb.RKeyMin, roachpb.Key("a"))
	args6 := scanArgs(keys.RangeDescriptorKey(roachpb.RKey(keys.MinKey)), roachpb.Key("a"))

	tArgs0, _ := endTxnArgs(txn, false /* commit */)
	tArgs1, _ := heartbeatArgs(txn)

	tArgs2, tHeader2 := endTxnArgs(txn, false /* commit */)
	tHeader2.Txn.Key = roachpb.Key(tHeader2.Txn.Key).Next()

	tArgs3, tHeader3 := heartbeatArgs(txn)
	tHeader3.Txn.Key = roachpb.Key(tHeader3.Txn.Key).Next()

	tArgs4 := pushTxnArgs(txn, txn, roachpb.PUSH_ABORT)
	tArgs4.PusheeTxn.Key = roachpb.Key(txn.Key).Next()

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
		if _, pErr := client.SendWrappedWith(context.Background(), store.testSender(), *test.header, test.args); !testutils.IsPError(pErr, test.err) {
			t.Errorf("%d expected error %q, got error %v", i, test.err, pErr)
		}
	}
}

// fakeRangeQueue implements the rangeQueue interface and
// records which range is passed to MaybeRemove.
type fakeRangeQueue struct {
	maybeRemovedRngs chan roachpb.RangeID
}

func (fq *fakeRangeQueue) Start(_ *hlc.Clock, _ *stop.Stopper) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeAdd(_ *Replica, _ hlc.Timestamp) {
	// Do nothing
}

func (fq *fakeRangeQueue) MaybeRemove(rangeID roachpb.RangeID) {
	fq.maybeRemovedRngs <- rangeID
}

// TestMaybeRemove tests that MaybeRemove is called when a range is removed.
func TestMaybeRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := TestStoreConfig(nil)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	store := createTestStoreWithoutStart(t, stopper, &cfg)

	// Add a queue to the scanner before starting the store and running the scanner.
	// This is necessary to avoid data race.
	fq := &fakeRangeQueue{
		maybeRemovedRngs: make(chan roachpb.RangeID),
	}
	store.scanner.AddQueues(fq)

	if err := store.Start(context.Background(), stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()

	repl, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(context.Background(), repl, *repl.Desc(), true); err != nil {
		t.Error(err)
	}
	// MaybeRemove is called.
	removedRng := <-fq.maybeRemovedRngs
	if removedRng != repl.RangeID {
		t.Errorf("Unexpected removed range %v", removedRng)
	}
}

func TestStoreChangeFrozen(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	store := tc.store

	assertFrozen := func(b storagebase.ReplicaState_FrozenEnum) {
		repl, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		repl.mu.Lock()
		frozen := repl.mu.state.Frozen
		repl.mu.Unlock()
		pFrozen, err := repl.stateLoader.loadFrozenStatus(context.Background(), store.Engine())
		if err != nil {
			t.Fatal(err)
		}
		if pFrozen != frozen {
			t.Fatal(errors.Errorf("persisted != in-memory frozen status: %v vs %v",
				pFrozen, frozen))
		}
		if pFrozen != b {
			t.Fatal(errors.Errorf("expected status %v, got %v", b, pFrozen))
		}
		collectFrozen := pFrozen == storagebase.ReplicaState_UNFROZEN
		results := store.FrozenStatus(collectFrozen)
		if len(results) != 0 {
			t.Fatal(errors.Errorf(
				"expected frozen=%v, got %d mismatching replicas: %+v",
				pFrozen, len(results), results))
		}
	}

	fReqVersMismatch := roachpb.NewChangeFrozen(keys.LocalMax, keys.LocalMax.Next(),
		true /* frozen */, "notvalidversion").(*roachpb.ChangeFrozenRequest)

	yes := storagebase.ReplicaState_FROZEN
	no := storagebase.ReplicaState_UNFROZEN

	// When processing a freeze from a different version, we log a message (not
	// tested here) but otherwise keep going. We may want to indicate replica
	// corruption for this in the future.
	{
		b := tc.store.Engine().NewBatch()
		defer b.Close()
		var h roachpb.Header
		if _, err := evalChangeFrozen(context.Background(), b, CommandArgs{Repl: tc.repl, Header: h, Args: fReqVersMismatch}, &roachpb.ChangeFrozenResponse{}); err != nil {
			t.Fatal(err)
		}
		assertFrozen(no) // since we do not commit the above batch
	}

	fReqValid := roachpb.NewChangeFrozen(keys.LocalMax, keys.LocalMax.Next(),
		true /* frozen */, build.GetInfo().Tag).(*roachpb.ChangeFrozenRequest)
	{
		fResp, pErr := client.SendWrapped(context.Background(), store.testSender(), fReqValid)
		if pErr != nil {
			t.Fatal(pErr)
		}
		assertFrozen(yes)
		resp := fResp.(*roachpb.ChangeFrozenResponse)
		if resp.RangesAffected != 1 {
			t.Fatalf("expected one affected range, got %d", resp.RangesAffected)
		}
		if len(resp.MinStartKey) != 0 {
			t.Fatalf("expected KeyMin as smallest affected range, got %s", resp.MinStartKey)
		}
	}

	pArgs := putArgs(roachpb.Key("a"), roachpb.Key("b"))

	// Now that we're frozen, can't use Raft.
	{
		_, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs)
		if !testutils.IsPError(pErr, "range is frozen") {
			t.Fatal(pErr)
		}
	}

	// The successful freeze goes through again idempotently, not affecting the
	// Range.
	{
		fResp, pErr := client.SendWrapped(context.Background(), store.testSender(), fReqValid)
		if pErr != nil {
			t.Fatal(pErr)
		}
		assertFrozen(yes)

		resp := fResp.(*roachpb.ChangeFrozenResponse)
		if resp.RangesAffected != 0 {
			t.Fatalf("expected no affected ranges, got %d", resp.RangesAffected)
		}
		assertFrozen(yes)
	}

	// Still frozen.
	{
		_, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs)
		if !testutils.IsPError(pErr, "range is frozen") {
			t.Fatal(pErr)
		}
	}

	uReq := roachpb.NewChangeFrozen(keys.LocalMax, keys.LocalMax.Next(),
		false /* !frozen */, "anyversiondoesit")
	{
		if _, pErr := client.SendWrapped(context.Background(), store.testSender(), uReq); pErr != nil {
			t.Fatal(pErr)
		}
		assertFrozen(no)
	}

	// Not frozen.
	{
		if _, pErr := client.SendWrapped(context.Background(), store.testSender(), &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

func TestStoreGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	store := tc.store

	assertThreshold := func(ts hlc.Timestamp) {
		repl, err := store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		repl.mu.Lock()
		gcThreshold := repl.mu.state.GCThreshold
		repl.mu.Unlock()
		pgcThreshold, err := repl.stateLoader.loadGCThreshold(context.Background(), store.Engine())
		if err != nil {
			t.Fatal(err)
		}
		if gcThreshold != pgcThreshold {
			t.Fatalf("persisted != in-memory threshold: %s vs %s", pgcThreshold, gcThreshold)
		}
		if pgcThreshold != ts {
			t.Fatalf("expected timestamp %s, got %s", ts, pgcThreshold)
		}
	}

	// Threshold should start at zero.
	assertThreshold(hlc.Timestamp{})

	threshold := hlc.Timestamp{
		WallTime: 2E9,
	}

	gcr := roachpb.GCRequest{
		// Bogus span to make it a valid request.
		Span: roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		},
		Threshold: threshold,
	}
	if _, pErr := tc.SendWrapped(&gcr); pErr != nil {
		t.Fatal(pErr)
	}

	assertThreshold(threshold)
}

func TestStoreRangePlaceholders(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	s := tc.store

	s.mu.Lock()
	numPlaceholders := len(s.mu.replicaPlaceholders)
	s.mu.Unlock()

	if numPlaceholders != 0 {
		t.Fatal("new store should have zero replica placeholders")
	}

	// Clobber the existing range so we can test non-overlapping placeholders.
	repl1, err := s.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := s.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Error(err)
	}

	repID := roachpb.RangeID(2)
	rep := createReplica(s, repID, roachpb.RKeyMin, roachpb.RKey("c"))
	if err := s.AddReplica(rep); err != nil {
		t.Fatal(err)
	}

	placeholder1 := &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(7),
			StartKey: roachpb.RKey("c"),
			EndKey:   roachpb.RKey("d"),
		},
	}
	placeholder2 := &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(8),
			StartKey: roachpb.RKey("d"),
			EndKey:   roachpb.RKeyMax,
		},
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Test that simple insertion works.
	if err := s.addPlaceholderLocked(placeholder1); err != nil {
		t.Fatalf("could not add placeholder to empty store, got %s", err)
	}
	if err := s.addPlaceholderLocked(placeholder2); err != nil {
		t.Fatalf("could not add non-overlapping placeholder, got %s", err)
	}

	// Test that simple deletion works.
	if !s.removePlaceholderLocked(placeholder1.rangeDesc.RangeID) {
		t.Fatalf("could not remove placeholder that was present")
	}

	// Test cannot double insert the same placeholder.
	if err := s.addPlaceholderLocked(placeholder1); err != nil {
		t.Fatalf("could not re-add placeholder after removal, got %s", err)
	}
	if err := s.addPlaceholderLocked(placeholder1); !testutils.IsError(err, ".*overlaps with existing KeyRange") {
		t.Fatalf("should not be able to add ReplicaPlaceholder for the same key twice, got: %v", err)
	}

	// Test cannot double delete a placeholder.
	if !s.removePlaceholderLocked(placeholder1.rangeDesc.RangeID) {
		t.Fatalf("could not remove placeholder that was present")
	}
	if s.removePlaceholderLocked(placeholder1.rangeDesc.RangeID) {
		t.Fatalf("successfully removed placeholder that was not present")
	}

	// This placeholder overlaps with an existing replica.
	placeholder1 = &ReplicaPlaceholder{
		rangeDesc: roachpb.RangeDescriptor{
			RangeID:  repID,
			StartKey: roachpb.RKeyMin,
			EndKey:   roachpb.RKey("c"),
		},
	}

	// Test that placeholder cannot clobber existing replica.
	if err := s.addPlaceholderLocked(placeholder1); !testutils.IsError(err, ".*overlaps with existing KeyRange") {
		t.Fatalf("should not be able to add ReplicaPlaceholder when Replica already exists, got: %v", err)
	}

	// Test that Placeholder deletion doesn't delete replicas.
	if s.removePlaceholderLocked(repID) {
		t.Fatalf("should not be able to process removeReplicaPlaceholder for a RangeID where a Replica exists")
	}
}

// Test that we remove snapshot placeholders on error conditions.
func TestStoreRemovePlaceholderOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	s := tc.store
	ctx := context.Background()

	// Clobber the existing range so we can test nonoverlapping placeholders.
	repl1, err := s.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Fatal(err)
	}

	// Generate a minimal fake snapshot.
	snapData := &roachpb.RaftSnapshotData{
		RangeDescriptor: *repl1.Desc(),
	}
	data, err := protoutil.Marshal(snapData)
	if err != nil {
		t.Fatal(err)
	}

	// Wrap the snapshot in a minimal request. The request will error because the
	// replica tombstone for the range requires that a new replica have an ID
	// greater than 1.
	req := &RaftMessageRequest{
		RangeID: 1,
		ToReplica: roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 0,
		},
		FromReplica: roachpb.ReplicaDescriptor{
			NodeID:    2,
			StoreID:   2,
			ReplicaID: 2,
		},
		Message: raftpb.Message{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Data: data,
			},
		},
	}
	const expected = "preemptive snapshot from term 0 received"
	if err := s.processRaftRequest(ctx, req,
		IncomingSnapshot{
			SnapUUID: uuid.MakeV4(),
			State:    &storagebase.ReplicaState{Desc: repl1.Desc()},
		}); !testutils.IsPError(err, expected) {
		t.Fatalf("expected %s, but found %v", expected, err)
	}

	s.mu.Lock()
	numPlaceholders := len(s.mu.replicaPlaceholders)
	s.mu.Unlock()

	if numPlaceholders != 0 {
		t.Fatalf("expected 0 placeholders, but found %d", numPlaceholders)
	}
	if n := atomic.LoadInt32(&s.counts.removedPlaceholders); n != 1 {
		t.Fatalf("expected 1 removed placeholder, but found %d", n)
	}
}

// Test that we remove snapshot placeholders when raft ignores the
// snapshot. This is testing the removal of placeholder after handleRaftReady
// processing for an unitialized Replica.
func TestStoreRemovePlaceholderOnRaftIgnored(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	s := tc.store
	ctx := context.Background()

	// Clobber the existing range so we can test nonoverlapping placeholders.
	repl1, err := s.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveReplica(context.Background(), repl1, *repl1.Desc(), true); err != nil {
		t.Fatal(err)
	}

	if _, err := writeInitialState(
		ctx, s.Engine(), enginepb.MVCCStats{}, *repl1.Desc(), raftpb.HardState{}, &roachpb.Lease{},
	); err != nil {
		t.Fatal(err)
	}

	// Generate a minimal fake snapshot.
	snapData := &roachpb.RaftSnapshotData{
		RangeDescriptor: *repl1.Desc(),
	}
	data, err := protoutil.Marshal(snapData)
	if err != nil {
		t.Fatal(err)
	}

	// Wrap the snapshot in a minimal request. The request will be dropped
	// because the Raft log index and term are less than the hard state written
	// above.
	req := &RaftMessageRequest{
		RangeID: 1,
		ToReplica: roachpb.ReplicaDescriptor{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: 2,
		},
		FromReplica: roachpb.ReplicaDescriptor{
			NodeID:    2,
			StoreID:   2,
			ReplicaID: 2,
		},
		Message: raftpb.Message{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Data: data,
				Metadata: raftpb.SnapshotMetadata{
					Index: 1,
					Term:  1,
				},
			},
		},
	}
	if err := s.processRaftRequest(ctx, req,
		IncomingSnapshot{
			SnapUUID: uuid.MakeV4(),
			State:    &storagebase.ReplicaState{Desc: repl1.Desc()},
		}); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		s.mu.Lock()
		numPlaceholders := len(s.mu.replicaPlaceholders)
		s.mu.Unlock()

		if numPlaceholders != 0 {
			return errors.Errorf("expected 0 placeholders, but found %d", numPlaceholders)
		}
		// The count of dropped placeholders is incremented after the placeholder
		// is removed (and while not holding Store.mu), so we need to perform the
		// check of the number of dropped placeholders in this retry loop.
		if n := atomic.LoadInt32(&s.counts.droppedPlaceholders); n != 1 {
			return errors.Errorf("expected 1 dropped placeholder, but found %d", n)
		}
		return nil
	})
}

func TestCanCampaignIdleReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc.Start(t, stopper)
	s := tc.store
	ctx := context.Background()

	s.idleReplicaElectionTime.Lock()
	s.idleReplicaElectionTime.at = time.Time{}
	s.idleReplicaElectionTime.Unlock()

	// Bump the clock by the election timeout. Idle replicas can't campaign
	// eagerly yet because we haven't gossiped the store descriptor.
	electionTimeout := int64(s.cfg.RaftTickInterval * time.Duration(s.cfg.RaftElectionTimeoutTicks))
	tc.manualClock.Increment(electionTimeout)
	if s.canCampaignIdleReplica() {
		t.Fatalf("idle replica can unexpectedly campaign")
	}

	// Gossip the store descriptor.
	if err := s.GossipStore(ctx); err != nil {
		t.Fatal(err)
	}
	if s.canCampaignIdleReplica() {
		t.Fatalf("idle replica can unexpectedly campaign")
	}

	// Bump the clock to just before the idle election time.
	tc.manualClock.Increment(electionTimeout - 1)
	if s.canCampaignIdleReplica() {
		t.Fatalf("idle replica can unexpectedly campaign")
	}

	// One more nanosecond bump and idle replicas should be able to campaign
	// eagerly.
	tc.manualClock.Increment(1)
	if !s.canCampaignIdleReplica() {
		t.Fatalf("idle replica unexpectedly cannot campaign")
	}
}

type fakeSnapshotStream struct {
	nextResp *SnapshotResponse
	nextErr  error
}

func (c fakeSnapshotStream) Recv() (*SnapshotResponse, error) {
	return c.nextResp, c.nextErr
}

func (c fakeSnapshotStream) Send(request *SnapshotRequest) error {
	return nil
}

type fakeStorePool struct {
	declinedThrottles int
	failedThrottles   int
}

func (sp *fakeStorePool) throttle(reason throttleReason, toStoreID roachpb.StoreID) {
	switch reason {
	case throttleDeclined:
		sp.declinedThrottles++
	case throttleFailed:
		sp.failedThrottles++
	}
}

// TestSendSnapshotThrottling tests the store pool throttling behavior of
// store.sendSnapshot, ensuring that it properly updates the StorePool on
// various exceptional conditions and new capacity estimates.
func TestSendSnapshotThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	e := engine.NewInMem(roachpb.Attributes{}, 1<<10)
	defer e.Close()

	ctx := context.Background()
	header := SnapshotRequest_Header{
		CanDecline: true,
		State: storagebase.ReplicaState{
			Desc: &roachpb.RangeDescriptor{RangeID: 1},
		},
	}
	newBatch := e.NewBatch

	// Test that a failed Recv() fauses a fail throttle
	{
		sp := &fakeStorePool{}
		expectedErr := errors.New("")
		c := fakeSnapshotStream{nil, expectedErr}
		err := sendSnapshot(ctx, c, sp, header, nil, newBatch, nil)
		if sp.failedThrottles != 1 {
			t.Fatalf("expected 1 failed throttle, but found %d", sp.failedThrottles)
		}
		if err != expectedErr {
			t.Fatalf("expected error %s, but found %s", err, expectedErr)
		}
	}

	// Test that a declined snapshot causes a decline throttle.
	{
		sp := &fakeStorePool{}
		resp := &SnapshotResponse{
			Status: SnapshotResponse_DECLINED,
		}
		c := fakeSnapshotStream{resp, nil}
		err := sendSnapshot(ctx, c, sp, header, nil, newBatch, nil)
		if sp.declinedThrottles != 1 {
			t.Fatalf("expected 1 declined throttle, but found %d", sp.declinedThrottles)
		}
		if err == nil {
			t.Fatalf("expected error, found nil")
		}
	}

	// Test that a declined but required snapshot causes a fail throttle.
	{
		sp := &fakeStorePool{}
		header.CanDecline = false
		resp := &SnapshotResponse{
			Status: SnapshotResponse_DECLINED,
		}
		c := fakeSnapshotStream{resp, nil}
		err := sendSnapshot(ctx, c, sp, header, nil, newBatch, nil)
		if sp.failedThrottles != 1 {
			t.Fatalf("expected 1 failed throttle, but found %d", sp.failedThrottles)
		}
		if err == nil {
			t.Fatalf("expected error, found nil")
		}
	}

	// Test that an errored snapshot causes a fail throttle.
	{
		sp := &fakeStorePool{}
		resp := &SnapshotResponse{
			Status: SnapshotResponse_ERROR,
		}
		c := fakeSnapshotStream{resp, nil}
		err := sendSnapshot(ctx, c, sp, header, nil, newBatch, nil)
		if sp.failedThrottles != 1 {
			t.Fatalf("expected 1 failed throttle, but found %d", sp.failedThrottles)
		}
		if err == nil {
			t.Fatalf("expected error, found nil")
		}
	}
}

func TestReserveSnapshotThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()
	tc := testContext{}
	tc.Start(t, stopper)
	s := tc.store

	ctx := context.Background()
	cleanup1, err := s.reserveSnapshot(ctx, &SnapshotRequest_Header{})
	if err != nil {
		t.Fatal(err)
	}
	if n := s.ReservationCount(); n != 1 {
		t.Fatalf("expected 1 reservation, but found %d", n)
	}

	// Verify we don't allow concurrent snapshots by spawning a goroutine which
	// will execute the cleanup after a short delay but only if another snapshot
	// was not allowed through.
	var boom int32
	go func() {
		time.Sleep(20 * time.Millisecond)
		if atomic.LoadInt32(&boom) == 0 {
			cleanup1()
		}
	}()

	cleanup2, err := s.reserveSnapshot(ctx, &SnapshotRequest_Header{})
	if err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&boom, 1)
	cleanup2()

	if n := s.ReservationCount(); n != 0 {
		t.Fatalf("expected 0 reservations, but found %d", n)
	}
}
