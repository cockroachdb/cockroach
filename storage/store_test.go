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

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/gogo/protobuf/proto"
)

var testIdent = roachpb.StoreIdent{
	ClusterID: "cluster",
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

type requestUnion interface {
	GetValue() interface{}
}

type responseAdder interface {
	Add(reply roachpb.Response)
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
		return nil, roachpb.NewError(util.Errorf("%s method not supported", et.Method()))
	}
	// Lookup range and direct request.
	rs := keys.Range(ba)
	rng := db.store.LookupReplica(rs.Key, rs.EndKey)
	if rng == nil {
		return nil, roachpb.NewError(roachpb.NewRangeKeyMismatchError(rs.Key.AsRawKey(), rs.EndKey.AsRawKey(), nil))
	}
	ba.RangeID = rng.Desc().RangeID
	replica := rng.GetReplica()
	if replica == nil {
		return nil, roachpb.NewError(util.Errorf("own replica missing in range"))
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
func createTestStoreWithoutStart(t *testing.T) (*Store, *hlc.ManualClock, *stop.Stopper) {
	stopper := stop.NewStopper()
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)
	rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), stopper)
	ctx := TestStoreContext
	ctx.Gossip = gossip.New(rpcContext, gossip.TestBootstrap)
	ctx.Gossip.SetNodeID(1)
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	ctx.StorePool = NewStorePool(ctx.Gossip, ctx.Clock, TestTimeUntilStoreDeadOff, stopper)
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20, stopper)
	ctx.Transport = NewLocalRPCTransport(stopper)
	stopper.AddCloser(ctx.Transport)
	sender := &testSender{}
	ctx.DB = client.NewDB(sender)
	store := NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})
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

// createTestStore creates a test store using an in-memory
// engine. It returns the store, the store clock's manual unix nanos time
// and a stopper. The caller is responsible for stopping the stopper
// upon completion.
func createTestStore(t *testing.T) (*Store, *hlc.ManualClock, *stop.Stopper) {
	store, manual, stopper := createTestStoreWithoutStart(t)
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
	defer leaktest.AfterTest(t)
	ctx := TestStoreContext
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
	ctx.Transport = NewLocalRPCTransport(stopper)
	stopper.AddCloser(ctx.Transport)
	store := NewStore(ctx, eng, &roachpb.NodeDescriptor{NodeID: 1})

	// Can't start as haven't bootstrapped.
	if err := store.Start(stopper); err == nil {
		t.Error("expected failure starting un-bootstrapped store")
	}

	// Bootstrap with a fake ident.
	if err := store.Bootstrap(testIdent, stopper); err != nil {
		t.Errorf("error bootstrapping store: %s", err)
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
	if _, err := store.GetReplica(1); err != nil {
		t.Errorf("failure fetching 1st range: %s", err)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)

	// Put some random garbage into the engine.
	if err := eng.Put(engine.MakeMVCCMetadataKey(roachpb.Key("foo")), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %s", err)
	}
	ctx := TestStoreContext
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	ctx.Transport = NewLocalRPCTransport(stopper)
	stopper.AddCloser(ctx.Transport)
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
	}
	r, err := NewReplica(desc, s)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func TestStoreAddRemoveRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	if err := store.RemoveReplica(rng1, *rng1.Desc()); err != nil {
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
	if err := store.RemoveReplica(rng1, *rng1.Desc()); err == nil {
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
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}
	origDesc := rng1.Desc()
	newDesc := proto.Clone(origDesc).(*roachpb.RangeDescriptor)
	_, newRep := newDesc.FindReplica(store.StoreID())
	newRep.ReplicaID++
	newDesc.NextReplicaID++
	if err := rng1.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveReplica(rng1, *origDesc); !testutils.IsError(err, "replica ID has changed") {
		t.Fatalf("expected error 'replica ID has changed' but got %s", err)
	}

	// Now try the latest descriptor and succeed.
	if err := store.RemoveReplica(rng1, *rng1.Desc()); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRangeSet(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Remove range 1.
	rng1, err := store.GetReplica(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveReplica(rng1, *rng1.Desc()); err != nil {
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
			if rng.Desc().RangeID != roachpb.RangeID(i) {
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
				if rng.Desc().RangeID != roachpb.RangeID(i) {
					t.Errorf("expected range with Range ID %d; got %v", i, rng)
				}
				close(visited)
				<-updated
			} else {
				// The second range will be removed and skipped.
				if rng.Desc().RangeID != roachpb.RangeID(i+1) {
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
	if rng.Desc().RangeID != 2 {
		t.Errorf("expected fetch of rangeID=2; got %d", rng.Desc().RangeID)
	}
	if err := store.RemoveReplica(rng, *rng.Desc()); err != nil {
		t.Error(err)
	}
	if ec := ranges.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}

	close(updated)
	<-done
}

func TestHasOverlappingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	if err := store.RemoveReplica(rng1, *rng1.Desc()); err != nil {
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
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	gArgs := getArgs([]byte("a"))

	// Try a successful get request.
	if _, err := client.SendWrapped(store.testSender(), nil, &gArgs); err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs([]byte("a"), []byte("aaa"))
	if _, err := client.SendWrapped(store.testSender(), nil, &pArgs); err != nil {
		t.Fatal(err)
	}
}

func TestStoreExecuteNoop(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	ba := roachpb.BatchRequest{}
	ba.RangeID = 1
	ba.Replica = roachpb.ReplicaDescriptor{StoreID: store.StoreID()}
	ba.Add(&roachpb.GetRequest{Span: roachpb.Span{Key: roachpb.Key("a")}})
	ba.Add(&roachpb.NoopRequest{})

	br, pErr := store.Send(context.Background(), ba)
	if pErr != nil {
		t.Error(pErr)
	}
	reply := br.Responses[1].GetInner()
	if _, ok := reply.(*roachpb.NoopResponse); !ok {
		t.Errorf("expected *roachpb.NoopResponse, got %T", reply)
	}
}

// TestStoreVerifyKeys checks that key length is enforced and
// that end keys must sort >= start.
func TestStoreVerifyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	// Try a start key == KeyMax.
	gArgs := getArgs(roachpb.KeyMax)
	if _, err := client.SendWrapped(store.testSender(), nil, &gArgs); !testutils.IsError(err, "must be less than KeyMax") {
		t.Fatalf("expected error for start key == KeyMax: %v", err)
	}
	// Try a get with an end key specified (get requires only a start key and should fail).
	gArgs.EndKey = roachpb.KeyMax
	if _, err := client.SendWrapped(store.testSender(), nil, &gArgs); !testutils.IsError(err, "must be less than KeyMax") {
		t.Fatalf("unexpected error for end key specified on a non-range-based operation: %v", err)
	}
	// Try a scan with end key < start key.
	sArgs := scanArgs([]byte("b"), []byte("a"))
	if _, err := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsError(err, "must be greater than") {
		t.Fatalf("unexpected error for end key < start: %v", err)
	}
	// Try a scan with start key == end key.
	sArgs.Key = []byte("a")
	sArgs.EndKey = sArgs.Key
	if _, err := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsError(err, "must be greater than") {
		t.Fatalf("unexpected error for start == end key: %v", err)
	}
	// Try a scan with range-local start key, but "regular" end key.
	sArgs.Key = keys.MakeRangeKey([]byte("test"), []byte("sffx"), nil)
	sArgs.EndKey = []byte("z")
	if _, err := client.SendWrapped(store.testSender(), nil, &sArgs); !testutils.IsError(err, "range-local") {
		t.Fatalf("unexpected error for local start, non-local end key: %v", err)
	}

	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	meta2KeyMax := keys.MakeKey(keys.Meta2Prefix, roachpb.RKeyMax)
	pArgs := putArgs(meta2KeyMax, []byte("value"))
	if _, err := client.SendWrapped(store.testSender(), nil, &pArgs); err != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", err)
	}
	// Try to put a range descriptor record for a start key which is
	// maximum length.
	key := append([]byte{}, roachpb.RKeyMax...)
	key[len(key)-1] = 0x01
	pArgs = putArgs(keys.RangeDescriptorKey(key), []byte("value"))
	if _, err := client.SendWrapped(store.testSender(), nil, &pArgs); err != nil {
		t.Fatalf("unexpected error on put to range descriptor for KeyMax value: %s", err)
	}
	// Try a put to txn record for a meta2 key (note that this doesn't
	// actually happen in practice, as txn records are not put directly,
	// but are instead manipulated only through txn methods).
	pArgs = putArgs(keys.TransactionKey(meta2KeyMax, []byte(uuid.NewUUID4())),
		[]byte("value"))
	if _, err := client.SendWrapped(store.testSender(), nil, &pArgs); err != nil {
		t.Fatalf("unexpected error on put to txn meta2 value: %s", err)
	}
}

// TestStoreSendUpdateTime verifies that the node clock is updated.
func TestStoreSendUpdateTime(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("a"))
	reqTS := store.ctx.Clock.Now()
	reqTS.WallTime += (100 * time.Millisecond).Nanoseconds()
	_, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: reqTS}, &args)
	if err != nil {
		t.Fatal(err)
	}
	ts := store.ctx.Clock.Timestamp()
	if ts.WallTime != reqTS.WallTime || ts.Logical <= reqTS.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", reqTS, ts)
	}
}

// TestStoreSendWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreSendWithZeroTime(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, mc, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("a"))

	// Set clock to time 1.
	mc.Set(1)
	resp, err := client.SendWrapped(store.testSender(), nil, &args)
	if err != nil {
		t.Fatal(err)
	}
	reply := resp.(*roachpb.GetResponse)
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if reply.Timestamp.WallTime != store.ctx.Clock.Now().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.ctx.Clock.Now(), reply.Timestamp)
	}
}

// TestStoreSendWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails.
func TestStoreSendWithClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: ts}, &args); err == nil {
		t.Error("expected max offset clock error")
	}
}

// TestStoreSendBadRange passes a bad range.
func TestStoreSendBadRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args := getArgs([]byte("0"))
	if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
		RangeID: 2, // no such range
	}, &args); err == nil {
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
	newRng, err := NewReplica(desc, store)
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
	defer leaktest.AfterTest(t)
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
		RangeID: rng2.Desc().RangeID,
	}, &args); err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRangeIDAllocation verifies that  range IDs are
// allocated in successive blocks.
func TestStoreRangeIDAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	defer leaktest.AfterTest(t)
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
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	testData := []struct {
		rng         *Replica
		expMaxBytes int64
	}{
		{store.LookupReplica(roachpb.RKeyMin, nil),
			config.DefaultZoneConfig.RangeMaxBytes},
		{splitTestRange(store, roachpb.RKeyMin, keys.MakeTablePrefix(1000), t),
			1 << 20},
		{splitTestRange(store, keys.MakeTablePrefix(1000), keys.MakeTablePrefix(1001), t),
			config.DefaultZoneConfig.RangeMaxBytes},
		{splitTestRange(store, keys.MakeTablePrefix(1001), keys.MakeTablePrefix(1002), t),
			2 << 20},
	}

	// Set zone configs.
	config.TestingSetZoneConfig(1000, &config.ZoneConfig{RangeMaxBytes: 1 << 20})
	config.TestingSetZoneConfig(1002, &config.ZoneConfig{RangeMaxBytes: 2 << 20})

	// Despite faking the zone configs, we still need to have a gossip entry.
	if err := store.Gossip().AddInfoProto(gossip.KeySystemConfig, &config.SystemConfig{}, 0); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		for _, test := range testData {
			if test.rng.GetMaxBytes() != test.expMaxBytes {
				return false
			}
		}
		return true
	}, 500*time.Millisecond); err != nil {
		t.Errorf("range max bytes values did not change as expected: %s", err)
	}
}

// TestStoreResolveWriteIntent adds write intent and then verifies
// that a put returns success and aborts intent's txn in the event the
// pushee has lower priority. Othwerise, verifies that a
// TransactionPushError is returned.
func TestStoreResolveWriteIntent(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
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

		bt, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
			t.Fatal(err)
		}
		// First lay down intent using the pushee's txn.
		pArgs := putArgs(key, []byte("value"))
		h := roachpb.Header{Txn: pushee}
		pushee.Sequence++
		if _, err := client.SendWrappedWith(store.testSender(), nil, h, &pArgs); err != nil {
			t.Fatal(err)
		}

		// Now, try a put using the pusher's txn.
		h.Txn = pusher
		_, err := client.SendWrappedWith(store.testSender(), nil, h, &pArgs)
		if resolvable {
			if err != nil {
				t.Errorf("expected intent resolved; got unexpected error: %s", err)
			}
			txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
			var txn roachpb.Transaction
			ok, err := engine.MVCCGetProto(store.Engine(), txnKey, roachpb.ZeroTimestamp, true, nil, &txn)
			if !ok || err != nil {
				t.Fatalf("not found or err: %s", err)
			}
			if txn.Status != roachpb.ABORTED {
				t.Errorf("expected pushee to be aborted; got %s", txn.Status)
			}
		} else {
			if rErr, ok := err.(*roachpb.TransactionPushError); !ok {
				t.Errorf("expected txn push error; got %s", err)
			} else if !bytes.Equal(rErr.PusheeTxn.ID, pushee.ID) {
				t.Errorf("expected txn to match pushee %q; got %s", pushee.ID, rErr)
			}
			// Trying again should fail again.
			h.Txn.Sequence++
			if _, err := client.SendWrappedWith(store.testSender(), nil, h, &pArgs); err == nil {
				t.Errorf("expected another error on latent write intent but succeeded")
			}
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

	// Begin pushee's transaction.
	bt, btH := beginTxnArgs(key, pushee)
	if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
		t.Fatal(err)
	}

	// First lay down intent using the pushee's txn.
	args := incrementArgs(key, 1)
	h := roachpb.Header{Txn: pushee}
	pushee.Sequence++
	if _, err := client.SendWrappedWith(store.testSender(), nil, h, &args); err != nil {
		t.Fatal(err)
	}

	// Now, try a put using the pusher's txn.
	h.Txn = pusher
	args.Increment = 2
	if resp, err := client.SendWrappedWith(store.testSender(), nil, h, &args); err != nil {
		t.Errorf("expected increment to succeed: %s", err)
	} else if reply := resp.(*roachpb.IncrementResponse); reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a
// write intent for a read will push the timestamp. On failure to
// push, verify a write intent error is returned with !Resolvable.
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	defer leaktest.AfterTest(t)
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

		// Begin pushee's transaction.
		bt, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
			t.Fatal(err)
		}

		// First, write original value.
		args := putArgs(key, []byte("value1"))
		if _, err := client.SendWrapped(store.testSender(), nil, &args); err != nil {
			t.Fatal(err)
		}

		// Second, lay down intent using the pushee's txn.
		h := roachpb.Header{Txn: pushee}
		pushee.Sequence++
		args.Value.SetBytes([]byte("value2"))
		if _, err := client.SendWrappedWith(store.testSender(), nil, h, &args); err != nil {
			t.Fatal(err)
		}

		// Now, try to read value using the pusher's txn.
		ts := store.ctx.Clock.Now()
		gArgs := getArgs(key)
		h.Txn = pusher
		h.Timestamp = ts
		firstReply, err := client.SendWrappedWith(store.testSender(), nil, h, &gArgs)
		if test.resolvable {
			if err != nil {
				t.Errorf("%d: expected read to succeed: %s", i, err)
			} else if replyBytes, err := firstReply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(replyBytes, []byte("value1")) {
				t.Errorf("%d: expected bytes to be %q, got %q", i, "value1", replyBytes)
			}

			// Finally, try to end the pushee's transaction; if we have
			// SNAPSHOT isolation, the commit should work: verify the txn
			// commit timestamp is equal to pusher's Timestamp + 1. Otherwise,
			// verify commit fails with TransactionRetryError.
			etArgs, h := endTxnArgs(pushee, true)
			pushee.Sequence++
			reply, cErr := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)

			expTimestamp := pusher.Timestamp
			expTimestamp.Logical++
			if test.pusheeIso == roachpb.SNAPSHOT {
				if cErr != nil {
					t.Errorf("unexpected error on commit: %s", cErr)
				}
				etReply := reply.(*roachpb.EndTransactionResponse)
				if etReply.Txn.Status != roachpb.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
					t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
						expTimestamp, etReply.Txn)
				}
			} else {
				if _, ok := cErr.(*roachpb.TransactionRetryError); !ok {
					t.Errorf("expected transaction retry error; got %s", cErr)
				}
			}
		} else {
			// If isolation of pushee is SNAPSHOT, we can always push, so
			// even a non-resolvable read will succeed. Otherwise, verify we
			// receive a transaction retry error (because we max out retries).
			if test.pusheeIso == roachpb.SNAPSHOT {
				if err != nil {
					t.Errorf("expected read to succeed: %s", err)
				} else if replyBytes, err := firstReply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
					t.Fatal(err)
				} else if !bytes.Equal(replyBytes, []byte("value1")) {
					t.Errorf("expected bytes to be %q, got %q", "value1", replyBytes)
				}
			} else {
				if err == nil {
					t.Errorf("expected read to fail")
				}
				if _, ok := err.(*roachpb.TransactionRetryError); !ok {
					t.Errorf("expected transaction retry error; got %T", err)
				}
			}
		}
	}
}

// TestStoreResolveWriteIntentSnapshotIsolation verifies that the
// timestamp can always be pushed if txn has snapshot isolation.
func TestStoreResolveWriteIntentSnapshotIsolation(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee := newTransaction("test", key, 1, roachpb.SNAPSHOT, store.ctx.Clock)
	pushee.Priority = 2
	pusher.Priority = 1 // Pusher would lose based on priority.

	// Begin pushee's transaction.
	bt, btH := beginTxnArgs(key, pushee)
	if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
		t.Fatal(err)
	}

	// First, write original value.
	args := putArgs(key, []byte("value1"))
	ts := store.ctx.Clock.Now()
	if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Timestamp: ts}, &args); err != nil {
		t.Fatal(err)
	}

	// Lay down intent using the pushee's txn.
	h := roachpb.Header{Txn: pushee}
	h.Txn.Sequence++
	h.Timestamp = store.ctx.Clock.Now()
	args.Value.SetBytes([]byte("value2"))
	if _, err := client.SendWrappedWith(store.testSender(), nil, h, &args); err != nil {
		t.Fatal(err)
	}

	// Now, try to read value using the pusher's txn.
	gArgs := getArgs(key)
	gTS := store.ctx.Clock.Now()
	h.Txn = pusher
	h.Timestamp = gTS
	if reply, err := client.SendWrappedWith(store.testSender(), nil, h, &gArgs); err != nil {
		t.Errorf("expected read to succeed: %s", err)
	} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(replyBytes, []byte("value1")) {
		t.Errorf("expected bytes to be %q, got %q", "value1", replyBytes)
	}

	// Finally, try to end the pushee's transaction; since it's got
	// SNAPSHOT isolation, the end should work, but verify the txn
	// commit timestamp is equal to gArgs.Timestamp + 1.
	etArgs, h := endTxnArgs(pushee, true)
	h.Timestamp = pushee.Timestamp
	h.Txn.Sequence++
	reply, err := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)
	if err != nil {
		t.Fatal(err)
	}
	etReply := reply.(*roachpb.EndTransactionResponse)
	expTimestamp := gTS
	expTimestamp.Logical++
	if etReply.Txn.Status != roachpb.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
		t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
			expTimestamp, etReply.Txn)
	}
}

// TestStoreResolveWriteIntentNoTxn verifies that reads and writes
// which are not part of a transaction can push intents.
func TestStoreResolveWriteIntentNoTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 0 // pushee should lose all conflicts

	// Begin pushee's transaction.
	bt, btH := beginTxnArgs(key, pushee)
	if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
		t.Fatal(err)
	}

	// First, lay down intent from pushee.
	pushee.Sequence++
	args := putArgs(key, []byte("value1"))
	if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Txn: pushee}, &args); err != nil {
		t.Fatal(err)
	}

	// Now, try to read outside a transaction.
	getTS := store.ctx.Clock.Now() // accessed later
	{
		gArgs := getArgs(key)
		if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			Timestamp:    getTS,
			UserPriority: proto.Int32(math.MaxInt32),
		}, &gArgs); err != nil {
			t.Errorf("expected read to succeed: %s", err)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			t.Errorf("expected value to be nil, got %+v", gReply.Value)
		}
	}

	{
		// Next, try to write outside of a transaction. We will succeed in pushing txn.
		putTS := store.ctx.Clock.Now()
		args.Value.SetBytes([]byte("value2"))
		if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			Timestamp:    putTS,
			UserPriority: proto.Int32(math.MaxInt32),
		}, &args); err != nil {
			t.Errorf("expected success aborting pushee's txn; got %s", err)
		}
	}

	// Read pushee's txn.
	txnKey := keys.TransactionKey(pushee.Key, pushee.ID)
	var txn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(store.Engine(), txnKey, roachpb.ZeroTimestamp, true, nil, &txn); !ok || err != nil {
		t.Fatalf("not found or err: %s", err)
	}
	if txn.Status != roachpb.ABORTED {
		t.Errorf("expected pushee to be aborted; got %s", txn.Status)
	}

	// Verify that the pushee's timestamp was moved forward on
	// former read, since we have it available in write intent error.
	expTS := getTS
	expTS.Logical++
	if !txn.Timestamp.Equal(expTS) {
		t.Errorf("expected pushee timestamp pushed to %s; got %s", expTS, txn.Timestamp)
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
	_, err := client.SendWrappedWith(store.testSender(), nil, h, &etArgs)
	if err == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := err.(*roachpb.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", err)
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
	defer leaktest.AfterTest(t)
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
		if _, err := client.SendWrapped(store.testSender(), nil, &args); err != nil {
			t.Fatal(err)
		}

		// Next, write intents for keyA and keyB. Note that the
		// transactions have unpushable priorities if canPush is true and
		// very pushable ones otherwise.
		priority := int32(-roachpb.MaxPriority)
		if canPush {
			priority = -1
		}
		args.Value.SetBytes([]byte("value2"))
		txnA := newTransaction("testA", keyA, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
		txnB := newTransaction("testB", keyB, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
		for _, txn := range []*roachpb.Transaction{txnA, txnB} {
			bt, btH := beginTxnArgs(txn.Key, txn)
			if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
				t.Fatal(err)
			}
			args.Key = txn.Key
			txn.Sequence++
			if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); err != nil {
				t.Fatal(err)
			}
		}
		// End txn B, but without resolving the intent.
		etArgs, h := endTxnArgs(txnB, true)
		txnB.Sequence++
		if _, err := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); err != nil {
			t.Fatal(err)
		}

		// Now, get from both keys and verify. Whether we can push or not, we
		// will be able to read with INCONSISTENT.
		gArgs := getArgs(keyA)

		if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); err != nil {
			t.Errorf("expected read to succeed: %s", err)
		} else if replyBytes, err := reply.(*roachpb.GetResponse).Value.GetBytes(); err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(replyBytes, []byte("value1")) {
			t.Errorf("expected value %q, got %+v", []byte("value1"), reply)
		}
		gArgs.Key = keyB

		if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &gArgs); err != nil {
			t.Errorf("expected read to succeed: %s", err)
		} else if gReply := reply.(*roachpb.GetResponse); gReply.Value != nil {
			// The new value of B will not be read at first.
			t.Errorf("expected value nil, got %+v", gReply.Value)
		}
		// However, it will be read eventually, as B's intent can be
		// resolved asynchronously as txn B is committed.
		util.SucceedsWithin(t, 500*time.Millisecond, func() error {
			if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
				ReadConsistency: roachpb.INCONSISTENT,
			}, &gArgs); err != nil {
				return util.Errorf("expected read to succeed: %s", err)
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
		reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &sArgs)
		if err != nil {
			t.Errorf("expected scan to succeed: %s", err)
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
	defer leaktest.AfterTest(t)
	defer func() { TestingCommandFilter = nil }()

	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	var count int32
	countPtr := &count

	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if _, ok := args.(*roachpb.ScanRequest); ok {
			atomic.AddInt32(countPtr, 1)
		}
		return nil
	}

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
				priority := int32(-1)
				if !test.canPush {
					priority = -roachpb.MaxPriority
				}
				txn = newTransaction(fmt.Sprintf("test-%d", i), key, priority, roachpb.SERIALIZABLE, store.ctx.Clock)
				bt, btH := beginTxnArgs(txn.Key, txn)
				if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
					t.Fatal(err)
				}
			}
			args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
			txn.Sequence++
			if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); err != nil {
				t.Fatal(err)
			}
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
			if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
				Timestamp:       ts,
				ReadConsistency: consistency,
			}, &sArgs); err != nil {
				t.Fatal(err)
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
				if _, err := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); err != nil {
					t.Fatal(err)
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
	defer leaktest.AfterTest(t)
	// This test relies on having a committed Txn record and open intents on
	// the same Range. This only works with auto-gc turned off; alternatively
	// the test could move to splitting its underlying Range.
	defer setTxnAutoGC(false)()
	var intercept atomic.Value
	intercept.Store(true)
	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if _, ok := args.(*roachpb.ResolveIntentRequest); ok && intercept.Load().(bool) {
			return util.Errorf("error on purpose")
		}
		return nil
	}
	store, _, stopper := createTestStore(t)
	defer func() { TestingCommandFilter = nil }()
	defer stopper.Stop()

	// Lay down 10 intents to scan over.
	txn := newTransaction("test", roachpb.Key("foo"), 1, roachpb.SERIALIZABLE, store.ctx.Clock)
	bt, btH := beginTxnArgs(txn.Key, txn)
	if _, err := client.SendWrappedWith(store.testSender(), nil, btH, &bt); err != nil {
		t.Fatal(err)
	}

	keys := []roachpb.Key{}
	for j := 0; j < 10; j++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", j))
		keys = append(keys, key)
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", j)))
		txn.Sequence++
		if _, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{Txn: txn}, &args); err != nil {
			t.Fatal(err)
		}
	}

	// Now, commit txn without resolving intents. If we hadn't disabled auto-gc
	// of Txn entries in this test, the Txn entry would be removed and later
	// attempts to resolve the intents would fail.
	etArgs, h := endTxnArgs(txn, true)
	txn.Sequence++
	if _, err := client.SendWrappedWith(store.testSender(), nil, h, &etArgs); err != nil {
		t.Fatal(err)
	}

	intercept.Store(false) // allow async intent resolution

	// Scan the range repeatedly until we've verified count.
	sArgs := scanArgs(keys[0], keys[9].Next())
	util.SucceedsWithin(t, time.Second, func() error {
		if reply, err := client.SendWrappedWith(store.testSender(), nil, roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &sArgs); err != nil {
			return err
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
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	txn := newTransaction("test", roachpb.Key("a"), 1 /* priority */, roachpb.SERIALIZABLE, store.ctx.Clock)

	args1 := getArgs(roachpb.Key("a"))
	args1.EndKey = roachpb.Key("b")

	args2 := getArgs(roachpb.RKeyMax)

	args3 := scanArgs(roachpb.Key("a"), roachpb.Key("a"))
	args4 := scanArgs(roachpb.Key("b"), roachpb.Key("a"))

	args5 := scanArgs(roachpb.RKeyMin, roachpb.Key("a"))
	args6 := scanArgs(keys.RangeTreeNodeKey(keys.Addr(keys.RangeTreeRoot)), roachpb.Key("a"))

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
		if _, err := client.SendWrappedWith(store.testSender(), nil, *test.header, test.args); err == nil || test.err == "" || !testutils.IsError(err, test.err) {
			t.Errorf("%d unexpected result: %s", i, err)
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
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStoreWithoutStart(t)
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
	if err := store.RemoveReplica(rng, *rng.Desc()); err != nil {
		t.Error(err)
	}
	// MaybeRemove is called.
	removedRng := <-fq.maybeRemovedRngs
	if removedRng != rng {
		t.Errorf("Unexpected removed range %v", removedRng)
	}
}
