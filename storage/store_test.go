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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

var testIdent = proto.StoreIdent{
	ClusterID: "cluster",
	NodeID:    1,
	StoreID:   1,
}

// setTestRetryOptions sets aggressive retries with a limit on number
// of attempts so we don't get stuck behind indefinite backoff/retry
// loops.
func setTestRetryOptions(s *Store) {
	s.ctx.RangeRetryOptions = util.RetryOptions{
		Backoff:     1 * time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		Constant:    2,
		MaxAttempts: 2,
	}
}

// testSender is an implementation of the client.KVSender interface
// which passes all requests through to a single store.
type testSender struct {
	store *Store
}

type requestUnion interface {
	GetValue() interface{}
}

type responseAdder interface {
	Add(reply proto.Response)
}

// Send forwards the call to the single store. This is a poor man's
// version of kv/txn_coord_sender, but it serves the purposes of
// supporting tests in this package. Transactions are not supported.
// Since kv/ depends on storage/, we can't get access to a
// txn_coord_sender from here.
func (db *testSender) Send(call client.Call) {
	reqs := []requestUnion{}
	switch t := call.Args.(type) {
	case *proto.BatchRequest:
		for i := range t.Requests {
			reqs = append(reqs, &t.Requests[i])
		}
		if err := db.sendBatch(reqs, call.Reply.(*proto.BatchResponse)); err != nil {
			call.Reply.Header().SetGoError(err)
		}
	case *proto.InternalBatchRequest:
		for i := range t.Requests {
			reqs = append(reqs, &t.Requests[i])
		}
		if err := db.sendBatch(reqs, call.Reply.(*proto.InternalBatchResponse)); err != nil {
			call.Reply.Header().SetGoError(err)
		}
	default:
		db.sendOne(call)
	}
}

func (db *testSender) sendBatch(reqs []requestUnion, adder responseAdder) error {
	var batchErr error
	for _, req := range reqs {
		args := req.GetValue().(proto.Request)
		call := client.Call{Args: args, Reply: args.CreateReply()}
		db.sendOne(call)
		adder.Add(call.Reply)
		if err := call.Reply.Header().GoError(); batchErr == nil && err != nil {
			batchErr = err
		}
	}
	return batchErr
}

func (db *testSender) sendOne(call client.Call) {
	switch call.Args.(type) {
	case *proto.EndTransactionRequest:
		call.Reply.Header().SetGoError(util.Errorf("%s method not supported", call.Method()))
		return
	}
	// Lookup range and direct request.
	header := call.Args.Header()
	if rng := db.store.LookupRange(header.Key, header.EndKey); rng != nil {
		header.RaftID = rng.Desc().RaftID
		header.Replica = *rng.GetReplica()
		if err := db.store.ExecuteCmd(call); err != nil {
			call.Reply.Header().SetGoError(err)
		}
	} else {
		call.Reply.Header().SetGoError(proto.NewRangeKeyMismatchError(header.Key, header.EndKey, nil))
	}
}

// createTestStore creates a test store using an in-memory
// engine. Returns the store, the store clock's manual unix nanos time
// and a stopper. The caller is responsible for stopping the stopper
// upon completion.
func createTestStore(t *testing.T) (*Store, *hlc.ManualClock, *util.Stopper) {
	stopper := util.NewStopper()
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), security.LoadInsecureTLSConfig(), stopper)
	ctx := TestStoreContext
	ctx.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 10<<20)
	ctx.Transport = multiraft.NewLocalRPCTransport()
	stopper.AddCloser(ctx.Transport)
	store := NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: 1})
	if err := store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, stopper); err != nil {
		t.Fatal(err)
	}
	store.ctx.DB = client.NewKV(nil, &testSender{store: store})
	if err := store.BootstrapRange(); err != nil {
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
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	ctx.Transport = multiraft.NewLocalRPCTransport()
	stopper := util.NewStopper()
	stopper.AddCloser(ctx.Transport)
	defer stopper.Stop()
	store := NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: 1})

	// Can't start as haven't bootstrapped.
	if err := store.Start(stopper); err == nil {
		t.Error("expected failure starting un-bootstrapped store")
	}

	// Bootstrap with a fake ident.
	if err := store.Bootstrap(testIdent, stopper); err != nil {
		t.Errorf("error bootstrapping store: %s", err)
	}

	// Try to get 1st range--non-existent.
	if _, err := store.GetRange(1); err == nil {
		t.Error("expected error fetching non-existent range")
	}

	// Bootstrap first range.
	if err := store.BootstrapRange(); err != nil {
		t.Errorf("failure to create first range: %s", err)
	}

	// Now, attempt to initialize a store with a now-bootstrapped range.
	store = NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: 1})
	if err := store.Start(stopper); err != nil {
		t.Errorf("failure initializing bootstrapped store: %s", err)
	}
	// 1st range should be available.
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %s", err)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)

	// Put some random garbage into the engine.
	if err := eng.Put(proto.EncodedKey("foo"), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %s", err)
	}
	ctx := TestStoreContext
	manual := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manual.UnixNano)
	ctx.Transport = multiraft.NewLocalRPCTransport()
	stopper := util.NewStopper()
	stopper.AddCloser(ctx.Transport)
	defer stopper.Stop()
	store := NewStore(ctx, eng, &proto.NodeDescriptor{NodeID: 1})

	// Can't init as haven't bootstrapped.
	if err := store.Start(stopper); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap should fail on non-empty engine.
	if err := store.Bootstrap(testIdent, stopper); err == nil {
		t.Error("expected bootstrap error on non-empty store")
	}
}

func TestRangeSliceSort(t *testing.T) {
	defer leaktest.AfterTest(t)
	var rs RangeSlice
	for i := 4; i >= 0; i-- {
		r := &Range{}
		r.SetDesc(&proto.RangeDescriptor{StartKey: proto.Key(fmt.Sprintf("foo%d", i))})
		rs = append(rs, r)
	}

	sort.Sort(rs)
	for i := 0; i < 5; i++ {
		expectedKey := proto.Key(fmt.Sprintf("foo%d", i))
		if !bytes.Equal(rs[i].Desc().StartKey, expectedKey) {
			t.Errorf("Expected %s, got %s", expectedKey, rs[i].Desc().StartKey)
		}
	}
}

func createRange(s *Store, raftID int64, start, end proto.Key) *Range {
	desc := &proto.RangeDescriptor{
		RaftID:   raftID,
		StartKey: start,
		EndKey:   end,
	}
	r, err := NewRange(desc, s)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func TestStoreAddRemoveRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	if _, err := store.GetRange(0); err == nil {
		t.Error("expected GetRange to fail on missing range")
	}
	// Range 1 already exists. Make sure we can fetch it.
	rng1, err := store.GetRange(1)
	if err != nil {
		t.Error(err)
	}
	// Remove range 1.
	if err := store.RemoveRange(rng1); err != nil {
		t.Error(err)
	}
	// Create a new range (id=2).
	rng2 := createRange(store, 2, proto.Key("a"), proto.Key("b"))
	if err := store.AddRange(rng2); err != nil {
		t.Fatal(err)
	}
	// Try to add the same range twice
	err = store.AddRange(rng2)
	if err == nil {
		t.Fatal("expected error re-adding same range")
	}
	if _, ok := err.(*rangeAlreadyExists); !ok {
		t.Fatalf("expected rangeAlreadyExists error; got %s", err)
	}
	// Try to remove range 1 again.
	if err := store.RemoveRange(rng1); err == nil {
		t.Fatal("expected error re-removing same range")
	}
	// Try to add a range with previously-used (but now removed) ID.
	rng2Dup := createRange(store, 1, proto.Key("a"), proto.Key("b"))
	if err := store.AddRange(rng2Dup); err != nil {
		t.Fatal(err)
	}
	// Add another range with different key range and then test lookup.
	rng3 := createRange(store, 3, proto.Key("c"), proto.Key("d"))
	if err := store.AddRange(rng3); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		start, end proto.Key
		expRng     *Range
	}{
		{proto.Key("a"), proto.Key("a\x00"), rng2},
		{proto.Key("a"), proto.Key("b"), rng2},
		{proto.Key("a\xff\xff"), proto.Key("b"), rng2},
		{proto.Key("c"), proto.Key("c\x00"), rng3},
		{proto.Key("c"), proto.Key("d"), rng3},
		{proto.Key("c\xff\xff"), proto.Key("d"), rng3},
		{proto.Key("x60\xff\xff"), proto.Key("a"), nil},
		{proto.Key("x60\xff\xff"), proto.Key("a\x00"), nil},
		{proto.Key("d"), proto.Key("d"), nil},
		{proto.Key("c\xff\xff"), proto.Key("d\x00"), nil},
		{proto.Key("a"), nil, rng2},
		{proto.Key("d"), nil, nil},
	}

	for i, test := range testCases {
		if r := store.LookupRange(test.start, test.end); r != test.expRng {
			t.Errorf("%d: expected range %v; got %v", i, test.expRng, r)
		}
	}
}

func TestStoreRangeIterator(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Remove range 1.
	rng1, err := store.GetRange(1)
	if err != nil {
		t.Error(err)
	}
	if err := store.RemoveRange(rng1); err != nil {
		t.Error(err)
	}
	// Add 10 new ranges.
	const newCount = 10
	for i := 0; i < newCount; i++ {
		rng := createRange(store, int64(i+1), proto.Key(fmt.Sprintf("a%02d", i)), proto.Key(fmt.Sprintf("a%02d", i+1)))
		if err := store.AddRange(rng); err != nil {
			t.Fatal(err)
		}
	}

	// Verify two passes of the iteration.
	iter := newStoreRangeIterator(store)
	for pass := 0; pass < 2; pass++ {
		for i := 1; iter.EstimatedCount() > 0; i++ {
			if rng := iter.Next(); rng == nil || rng.Desc().RaftID != int64(i) {
				t.Errorf("expected range with Raft ID %d; got %v", i, rng)
			}
		}
		iter.Reset()
	}

	// Try iterating with an addition.
	iter.Next()
	if ec := iter.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}
	// Split the first range to insert a new range as second range.
	rng := createRange(store, 11, proto.Key("a000"), proto.Key("a01"))
	if err = store.SplitRange(store.LookupRange(proto.Key("a00"), nil), rng); err != nil {
		t.Fatal(err)
	}
	// Estimated count will still be 9, as it's cached, but next() will refresh.
	if ec := iter.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}
	if r := iter.Next(); r == nil || r != rng {
		t.Errorf("expected r==rng; got %d", r.Desc().RaftID)
	}
	if ec := iter.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}

	// Now, remove the next range in the iteration but verify iteration
	// continues as expected.
	rng = store.LookupRange(proto.Key("a01"), nil)
	if rng.Desc().RaftID != 2 {
		t.Errorf("expected fetch of raftID=2; got %d", rng.Desc().RaftID)
	}
	if err := store.RemoveRange(rng); err != nil {
		t.Error(err)
	}
	if ec := iter.EstimatedCount(); ec != 9 {
		t.Errorf("expected 9 remaining; got %d", ec)
	}
	// Verify we skip removed range (id=2).
	if r := iter.Next(); r.Desc().RaftID != 3 {
		t.Errorf("expected raftID=3; got %d", r.Desc().RaftID)
	}
	if ec := iter.EstimatedCount(); ec != 7 {
		t.Errorf("expected 7 remaining; got %d", ec)
	}
}

func TestStoreIteratorWithAddAndRemoval(t *testing.T) {
	defer leaktest.AfterTest(t)
}

// TestStoreExecuteCmd verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreExecuteCmd(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	gArgs, gReply := getArgs([]byte("a"), 1, store.StoreID())

	// Try a successful get request.
	if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("aaa"), 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatal(err)
	}
}

// TestStoreVerifyKeys checks that key length is enforced and
// that end keys must sort >= start.
func TestStoreVerifyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	tooLongKey := engine.MakeKey(engine.KeyMax, []byte{0})

	// Start with a too-long key on a get.
	gArgs, gReply := getArgs(tooLongKey, 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err == nil {
		t.Fatal("expected error for key too long")
	}
	// Try a start key == KeyMax.
	gArgs.Key = engine.KeyMax
	if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err == nil {
		t.Fatal("expected error for start key == KeyMax")
	}
	// Try a scan with too-long EndKey.
	sArgs, sReply := scanArgs(engine.KeyMin, tooLongKey, 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: sArgs, Reply: sReply}); err == nil {
		t.Fatal("expected error for end key too long")
	}
	// Try a scan with end key < start key.
	sArgs.Key = []byte("b")
	sArgs.EndKey = []byte("a")
	if err := store.ExecuteCmd(client.Call{Args: sArgs, Reply: sReply}); err == nil {
		t.Fatal("expected error for end key < start")
	}
	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	meta2KeyMax := engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax)
	pArgs, pReply := putArgs(meta2KeyMax, []byte("value"), 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", err)
	}
	// Try to put a range descriptor record for a start key which is
	// maximum length.
	key := append([]byte{}, engine.KeyMax...)
	key[len(key)-1] = 0x01
	pArgs, pReply = putArgs(engine.RangeDescriptorKey(key), []byte("value"), 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatalf("unexpected error on put to range descriptor for KeyMax value: %s", err)
	}
	// Try a put to txn record for a meta2 key (note that this doesn't
	// actually happen in practice, as txn records are not put directly,
	// but are instead manipulated only through txn methods).
	pArgs, pReply = putArgs(engine.TransactionKey(meta2KeyMax, []byte(util.NewUUID4())),
		[]byte("value"), 1, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
		t.Fatalf("unexpected error on put to txn meta2 value: %s", err)
	}
}

// TestStoreExecuteCmdUpdateTime verifies that the node clock is updated.
func TestStoreExecuteCmdUpdateTime(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args, reply := getArgs([]byte("a"), 1, store.StoreID())
	args.Timestamp = store.ctx.Clock.Now()
	args.Timestamp.WallTime += (100 * time.Millisecond).Nanoseconds()
	err := store.ExecuteCmd(client.Call{Args: args, Reply: reply})
	if err != nil {
		t.Fatal(err)
	}
	ts := store.ctx.Clock.Timestamp()
	if ts.WallTime != args.Timestamp.WallTime || ts.Logical <= args.Timestamp.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", args.Timestamp, ts)
	}
}

// TestStoreExecuteCmdWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreExecuteCmdWithZeroTime(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, mc, stopper := createTestStore(t)
	defer stopper.Stop()
	args, reply := getArgs([]byte("a"), 1, store.StoreID())

	// Set clock to time 1.
	mc.Set(1)
	err := store.ExecuteCmd(client.Call{Args: args, Reply: reply})
	if err != nil {
		t.Fatal(err)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if reply.Timestamp.WallTime != store.ctx.Clock.Now().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.ctx.Clock.Now(), reply.Timestamp)
	}
}

// TestStoreExecuteCmdWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails with an error.
func TestStoreExecuteCmdWithClockOffset(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, mc, stopper := createTestStore(t)
	defer stopper.Stop()
	args, reply := getArgs([]byte("a"), 1, store.StoreID())

	// Set clock to time 1.
	mc.Set(1)
	// Set clock max offset to 250ms.
	maxOffset := 250 * time.Millisecond
	store.ctx.Clock.SetMaxOffset(maxOffset)
	// Set args timestamp to exceed max offset.
	args.Timestamp = store.ctx.Clock.Now()
	args.Timestamp.WallTime += maxOffset.Nanoseconds() + 1
	err := store.ExecuteCmd(client.Call{Args: args, Reply: reply})
	if err == nil {
		t.Error("expected max offset clock error")
	}
}

// TestStoreExecuteCmdBadRange passes a bad range.
func TestStoreExecuteCmdBadRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	args, reply := getArgs([]byte("0"), 2, store.StoreID()) // no range ID 2
	err := store.ExecuteCmd(client.Call{Args: args, Reply: reply})
	if err == nil {
		t.Error("expected invalid range")
	}
}

// splitTestRange splits a range. This does *not* fully emulate a real split
// and should not be used in new tests. Tests that need splits should live in
// client_split_test.go and use AdminSplit instead of this function.
// See #702
// TODO(bdarnell): convert tests that use this function to use AdminSplit instead.
func splitTestRange(store *Store, key, splitKey proto.Key, t *testing.T) *Range {
	rng := store.LookupRange(key, nil)
	if rng == nil {
		t.Fatalf("couldn't lookup range for key %q", key)
	}
	desc, err := store.NewRangeDescriptor(splitKey, rng.Desc().EndKey, rng.Desc().Replicas)
	if err != nil {
		t.Fatal(err)
	}
	newRng, err := NewRange(desc, store)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.SplitRange(rng, newRng); err != nil {
		t.Fatal(err)
	}
	return newRng
}

// TestStoreExecuteCmdOutOfRange passes a key not contained
// within the range's key range.
func TestStoreExecuteCmdOutOfRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	rng2 := splitTestRange(store, engine.KeyMin, proto.Key("b"), t)

	// Range 1 is from KeyMin to "b", so reading "b" from range 1 should
	// fail because it's just after the range boundary.
	args, reply := getArgs([]byte("b"), 1, store.StoreID())
	err := store.ExecuteCmd(client.Call{Args: args, Reply: reply})
	if err == nil {
		t.Error("expected key to be out of range")
	}

	// Range 2 is from "b" to KeyMax, so reading "a" from range 2 should
	// fail because it's before the start of the range.
	args, reply = getArgs([]byte("a"), rng2.Desc().RaftID, store.StoreID())
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRaftIDAllocation verifies that raft IDs are
// allocated in successive blocks.
func TestStoreRaftIDAllocation(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Raft IDs should be allocated from ID 2 (first alloc'd range)
	// to raftIDAllocCount * 3 + 1.
	for i := 0; i < raftIDAllocCount*3; i++ {
		replicas := []proto.Replica{{StoreID: store.StoreID()}}
		desc, err := store.NewRangeDescriptor(proto.Key(fmt.Sprintf("%03d", i)), proto.Key(fmt.Sprintf("%03d", i+1)), replicas)
		if err != nil {
			t.Fatal(err)
		}
		if desc.RaftID != int64(2+i) {
			t.Errorf("expected Raft id %d; got %d", 2+i, desc.RaftID)
		}
	}
}

// TestStoreRangesByKey verifies we can lookup ranges by key using
// the sorted rangesByKey slice.
func TestStoreRangesByKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	r0 := store.LookupRange(engine.KeyMin, nil)
	r1 := splitTestRange(store, engine.KeyMin, proto.Key("A"), t)
	r2 := splitTestRange(store, proto.Key("A"), proto.Key("C"), t)
	r3 := splitTestRange(store, proto.Key("C"), proto.Key("X"), t)
	r4 := splitTestRange(store, proto.Key("X"), proto.Key("ZZ"), t)

	if r := store.LookupRange(proto.Key("0"), nil); r != r0 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r0.Desc())
	}
	if r := store.LookupRange(proto.Key("B"), nil); r != r1 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r1.Desc())
	}
	if r := store.LookupRange(proto.Key("C"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r2.Desc())
	}
	if r := store.LookupRange(proto.Key("M"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r2.Desc())
	}
	if r := store.LookupRange(proto.Key("X"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r3.Desc())
	}
	if r := store.LookupRange(proto.Key("Z"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r3.Desc())
	}
	if r := store.LookupRange(proto.Key("ZZ"), nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r4.Desc())
	}
	if r := store.LookupRange(proto.Key("\xff\x00"), nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc(), r4.Desc())
	}
	if store.LookupRange(engine.KeyMax, nil) != nil {
		t.Errorf("expected engine.KeyMax to not have an associated range")
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
		rng         *Range
		expMaxBytes int64
	}{
		{store.LookupRange(engine.KeyMin, nil), 64 << 20},
		{splitTestRange(store, engine.KeyMin, proto.Key("a"), t), 1 << 20},
		{splitTestRange(store, proto.Key("a"), proto.Key("aa"), t), 1 << 20},
		{splitTestRange(store, proto.Key("aa"), proto.Key("b"), t), 64 << 20},
	}

	// Now set a new zone config for the prefix "a" with a different max bytes.
	zoneConfig := &proto.ZoneConfig{
		ReplicaAttrs:  []proto.Attributes{{}, {}, {}},
		RangeMinBytes: 1 << 8,
		RangeMaxBytes: 1 << 20,
	}
	data, err := gogoproto.Marshal(zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	key := engine.MakeKey(engine.KeyConfigZonePrefix, proto.Key("a"))
	pArgs, pReply := putArgs(key, data, 1, store.StoreID())
	pArgs.Timestamp = store.ctx.Clock.Now()
	if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
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
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
		if resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First lay down intent using the pushee's txn.
		pArgs, pReply := putArgs(key, []byte("value"), 1, store.StoreID())
		pArgs.Timestamp = store.ctx.Clock.Now()
		pArgs.Txn = pushee
		if err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err != nil {
			t.Fatal(err)
		}

		// Now, try a put using the pusher's txn.
		pArgs.Timestamp = store.ctx.Clock.Now()
		pArgs.Txn = pusher
		err := store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply})
		if resolvable {
			if err != nil {
				t.Errorf("expected intent resolved; got unexpected error: %s", err)
			}
			txnKey := engine.TransactionKey(pushee.Key, pushee.ID)
			var txn proto.Transaction
			ok, err := engine.MVCCGetProto(store.Engine(), txnKey, proto.ZeroTimestamp, true, nil, &txn)
			if !ok || err != nil {
				t.Fatalf("not found or err: %s", err)
			}
			if txn.Status != proto.ABORTED {
				t.Errorf("expected pushee to be aborted; got %s", txn.Status)
			}
		} else {
			rErr, ok := err.(*proto.TransactionPushError)
			if !ok {
				t.Errorf("expected txn push error; got %s", err)
			}
			if !bytes.Equal(rErr.PusheeTxn.ID, pushee.ID) {
				t.Errorf("expected txn to match pushee %q; got %s", pushee.ID, rErr)
			}
			// Trying again should fail again.
			if err = store.ExecuteCmd(client.Call{Args: pArgs, Reply: pReply}); err == nil {
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

	key := proto.Key("a")
	pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
	pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

	// First lay down intent using the pushee's txn.
	args, reply := incrementArgs(key, 1, 1, store.StoreID())
	args.Timestamp = store.ctx.Clock.Now()
	args.Txn = pushee
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	// Now, try a put using the pusher's txn.
	args.Timestamp = store.ctx.Clock.Now()
	args.Txn = pusher
	args.Increment = 2
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Errorf("expected increment to succeed: %s", err)
	}
	if reply.NewValue != 2 {
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
		pusheeIso  proto.IsolationType
	}{
		// Resolvable is true, so we can read, but SERIALIZABLE means we can't commit.
		{true, proto.SERIALIZABLE},
		// Pushee is SNAPSHOT, meaning we can commit.
		{true, proto.SNAPSHOT},
		// Resolvable is false and SERIALIZABLE so can't read.
		{false, proto.SERIALIZABLE},
		// Resolvable is false, but SNAPSHOT means we can push it anyway, so can read.
		{false, proto.SNAPSHOT},
	}
	for i, test := range testCases {
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
		pushee := newTransaction("test", key, 1, test.pusheeIso, store.ctx.Clock)
		if test.resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First, write original value.
		args, reply := putArgs(key, []byte("value1"), 1, store.StoreID())
		args.Timestamp = store.ctx.Clock.Now()
		if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
			t.Fatal(err)
		}

		// Second, lay down intent using the pushee's txn.
		args.Timestamp = store.ctx.Clock.Now()
		args.Txn = pushee
		args.Value.Bytes = []byte("value2")
		if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
			t.Fatal(err)
		}

		// Now, try to read value using the pusher's txn.
		gArgs, gReply := getArgs(key, 1, store.StoreID())
		gArgs.Timestamp = store.ctx.Clock.Now()
		gArgs.Txn = pusher
		err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply})
		if test.resolvable {
			if err != nil {
				t.Errorf("%d: expected read to succeed: %s", i, err)
			} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
				t.Errorf("%d: expected bytes to be %q, got %q", i, "value1", gReply.Value.Bytes)
			}

			// Finally, try to end the pushee's transaction; if we have
			// SNAPSHOT isolation, the commit should work: verify the txn
			// commit timestamp is equal to gArgs.Timestamp + 1. Otherwise,
			// verify commit fails with TransactionRetryError.
			etArgs, etReply := endTxnArgs(pushee, true, 1, store.StoreID())
			etArgs.Timestamp = pushee.Timestamp
			err := store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply})

			expTimestamp := gArgs.Timestamp
			expTimestamp.Logical++
			if test.pusheeIso == proto.SNAPSHOT {
				if err != nil {
					t.Errorf("unexpected error on commit: %s", err)
				}
				if etReply.Txn.Status != proto.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
					t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
						expTimestamp, etReply.Txn)
				}
			} else {
				if _, ok := err.(*proto.TransactionRetryError); !ok {
					t.Errorf("expected transaction retry error; got %s", err)
				}
			}
		} else {
			// If isolation of pushee is SNAPSHOT, we can always push, so
			// even a non-resolvable read will succeed. Otherwise, verify we
			// receive a transaction retry error (because we max out retries).
			if test.pusheeIso == proto.SNAPSHOT {
				if err != nil {
					t.Errorf("expected read to succeed: %s", err)
				} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
					t.Errorf("expected bytes to be %q, got %q", "value1", gReply.Value.Bytes)
				}
			} else {
				if err == nil {
					t.Errorf("expected read to fail")
				}
				if _, ok := err.(*proto.TransactionRetryError); !ok {
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

	key := proto.Key("a")
	pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
	pushee := newTransaction("test", key, 1, proto.SNAPSHOT, store.ctx.Clock)
	pushee.Priority = 2
	pusher.Priority = 1 // Pusher would lose based on priority.

	// First, write original value.
	args, reply := putArgs(key, []byte("value1"), 1, store.StoreID())
	args.Timestamp = store.ctx.Clock.Now()
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	// Lay down intent using the pushee's txn.
	args.Timestamp = store.ctx.Clock.Now()
	args.Txn = pushee
	args.Value.Bytes = []byte("value2")
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	// Now, try to read value using the pusher's txn.
	gArgs, gReply := getArgs(key, 1, store.StoreID())
	gArgs.Timestamp = store.ctx.Clock.Now()
	gArgs.Txn = pusher
	if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
		t.Errorf("expected read to succeed: %s", err)
	} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
		t.Errorf("expected bytes to be %q, got %q", "value1", gReply.Value.Bytes)
	}

	// Finally, try to end the pushee's transaction; since it's got
	// SNAPSHOT isolation, the end should work, but verify the txn
	// commit timestamp is equal to gArgs.Timestamp + 1.
	etArgs, etReply := endTxnArgs(pushee, true, 1, store.StoreID())
	etArgs.Timestamp = pushee.Timestamp
	if err := store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply}); err != nil {
		t.Fatal(err)
	}
	expTimestamp := gArgs.Timestamp
	expTimestamp.Logical++
	if etReply.Txn.Status != proto.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
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

	key := proto.Key("a")
	pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, store.ctx.Clock)
	pushee.Priority = 0 // pushee should lose all conflicts

	// First, lay down intent from pushee.
	args, reply := putArgs(key, []byte("value1"), 1, store.StoreID())
	args.Timestamp = pushee.Timestamp
	args.Txn = pushee
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Fatal(err)
	}

	// Now, try to read outside a transaction.
	gArgs, gReply := getArgs(key, 1, store.StoreID())
	gArgs.Timestamp = store.ctx.Clock.Now()
	gArgs.UserPriority = gogoproto.Int32(math.MaxInt32)
	if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
		t.Errorf("expected read to succeed: %s", err)
	} else if gReply.Value != nil {
		t.Errorf("expected value to be nil, got %+v", gReply.Value)
	}

	// Next, try to write outside of a transaction. We will succeed in pushing txn.
	args.Timestamp = store.ctx.Clock.Now()
	args.Value.Bytes = []byte("value2")
	args.Txn = nil
	args.UserPriority = gogoproto.Int32(math.MaxInt32)
	if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
		t.Errorf("expected success aborting pushee's txn; got %s", err)
	}

	// Read pushee's txn.
	txnKey := engine.TransactionKey(pushee.Key, pushee.ID)
	var txn proto.Transaction
	ok, err := engine.MVCCGetProto(store.Engine(), txnKey, proto.ZeroTimestamp, true, nil, &txn)
	if !ok || err != nil {
		t.Fatalf("not found or err: %s", err)
	}
	if txn.Status != proto.ABORTED {
		t.Errorf("expected pushee to be aborted; got %s", txn.Status)
	}

	// Verify that the pushee's timestamp was moved forward on
	// former read, since we have it available in write intent error.
	expTS := gArgs.Timestamp
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
	etArgs, etReply := endTxnArgs(pushee, true, 1, store.StoreID())
	etArgs.Timestamp = pushee.Timestamp
	err = store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply})
	if err == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := err.(*proto.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", err)
	}
}

// TestStoreReadInconsistent verifies that gets and scans with read
// consistency set to INCONSISTENT either push or simply ignore extant
// intents (if they cannot be pushed), depending on the intent priority.
func TestStoreReadInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	for _, canPush := range []bool{true, false} {
		keyA := proto.Key(fmt.Sprintf("%t-a", canPush))
		keyB := proto.Key(fmt.Sprintf("%t-b", canPush))

		// First, write keyA.
		args, reply := putArgs(keyA, []byte("value1"), 1, store.StoreID())
		if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
			t.Fatal(err)
		}

		// Next, write intents for keyA and keyB. Note that the
		// transactions have unpushable priorities if canPush is true and
		// very pushable ones otherwise.
		priority := int32(-proto.MaxPriority)
		if canPush {
			priority = -1
		}
		args.Value.Bytes = []byte("value2")
		txnA := newTransaction("testA", keyA, priority, proto.SERIALIZABLE, store.ctx.Clock)
		txnB := newTransaction("testB", keyB, priority, proto.SERIALIZABLE, store.ctx.Clock)
		for _, txn := range []*proto.Transaction{txnA, txnB} {
			args.Key = txn.Key
			args.Timestamp = txn.Timestamp
			args.Txn = txn
			if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
				t.Fatal(err)
			}
		}
		// End txn B, but without resolving the intent.
		etArgs, etReply := endTxnArgs(txnB, true, 1, store.StoreID())
		etArgs.Timestamp = txnB.Timestamp
		if err := store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply}); err != nil {
			t.Fatal(err)
		}

		// Now, get from both keys and verify. Wether we can push or not,
		// we will be able to read with INCONSISTENT.
		gArgs, gReply := getArgs(keyA, 1, store.StoreID())
		gArgs.Timestamp = store.ctx.Clock.Now()
		gArgs.ReadConsistency = proto.INCONSISTENT
		if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
			t.Errorf("expected read to succeed: %s", err)
		} else if gReply.Value == nil || !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
			t.Errorf("expected value %q, got %+v", []byte("value1"), gReply.Value)
		}
		gArgs.Key = keyB
		if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
			t.Errorf("expected read to succeed: %s", err)
		}
		// The new value of B will not be read at first.
		if gReply.Value != nil {
			t.Errorf("expected value nil, got %+v", gReply.Value)
		}
		// However, it will be read eventually, as B's intent can be
		// resolved asynchronously as txn B is committed.
		util.SucceedsWithin(t, 500*time.Millisecond, func() error {
			if err := store.ExecuteCmd(client.Call{Args: gArgs, Reply: gReply}); err != nil {
				return util.Errorf("expected read to succeed: %s", err)
			} else if gReply.Value == nil || !bytes.Equal(gReply.Value.Bytes, []byte("value2")) {
				return util.Errorf("expected value %q, got %+v", []byte("value2"), gReply.Value)
			}
			return nil
		})

		// Scan keys and verify results.
		sArgs, sReply := scanArgs(keyA, keyB.Next(), 1, store.StoreID())
		sArgs.ReadConsistency = proto.INCONSISTENT
		if err := store.ExecuteCmd(client.Call{Args: sArgs, Reply: sReply}); err != nil {
			t.Errorf("expected scan to succeed: %s", err)
		}
		if l := len(sReply.Rows); l != 2 {
			t.Errorf("expected 2 results; got %d", l)
		} else if key := sReply.Rows[0].Key; !key.Equal(keyA) {
			t.Errorf("expected key %q; got %q", keyA, key)
		} else if key := sReply.Rows[1].Key; !key.Equal(keyB) {
			t.Errorf("expected key %q; got %q", keyB, key)
		} else if val := sReply.Rows[0].Value.Bytes; !bytes.Equal(val, []byte("value1")) {
			t.Errorf("expected value %q, got %q", []byte("value1"), sReply.Rows[0].Value.Bytes)
		} else if val := sReply.Rows[1].Value.Bytes; !bytes.Equal(val, []byte("value2")) {
			t.Errorf("expected value %q, got %q", []byte("value2"), sReply.Rows[1].Value.Bytes)
		}
	}
}

// TestStoreScanIntents verifies that a scan across 10 intents resolves
// them in one fell swoop using both consistent and inconsistent reads.
func TestStoreScanIntents(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _, stopper := createTestStore(t)
	defer func() { TestingCommandFilter = nil }()
	defer stopper.Stop()

	testCases := []struct {
		consistent bool
		canPush    bool // can the txn be pushed?
		expFinish  bool // do we expect the scan to finish?
		expCount   int  // how many times do we expect to scan?
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
		count := 0
		TestingCommandFilter = func(args proto.Request, reply proto.Response) bool {
			if _, ok := args.(*proto.ScanRequest); ok {
				count++
			}
			return false
		}

		// Lay down 10 intents to scan over.
		var txn *proto.Transaction
		keys := []proto.Key{}
		for j := 0; j < 10; j++ {
			key := proto.Key(fmt.Sprintf("key%d-%02d", i, j))
			keys = append(keys, key)
			if txn == nil {
				priority := int32(-1)
				if !test.canPush {
					priority = -proto.MaxPriority
				}
				txn = newTransaction(fmt.Sprintf("test-%d", i), key, priority, proto.SERIALIZABLE, store.ctx.Clock)
			}
			args, reply := putArgs(key, []byte(fmt.Sprintf("value%02d", j)), 1, store.StoreID())
			args.Txn = txn
			if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
				t.Fatal(err)
			}
		}

		// Scan the range and verify count. Do this in a goroutine in case
		// it isn't expected to finish.
		sArgs, sReply := scanArgs(keys[0], keys[9].Next(), 1, store.StoreID())
		sArgs.Timestamp = store.Clock().Now()
		if !test.consistent {
			sArgs.ReadConsistency = proto.INCONSISTENT
		}
		done := make(chan struct{})
		go func(sArgs proto.Request, sReply proto.Response) {
			if err := store.ExecuteCmd(client.Call{Args: sArgs, Reply: sReply}); err != nil {
				t.Fatal(err)
			}
			close(done)
		}(sArgs, sReply)

		wait := 1 * time.Second
		if !test.expFinish {
			wait = 10 * time.Millisecond
		}
		select {
		case <-done:
			if len(sReply.Rows) != 0 {
				t.Errorf("expected empty scan result; got %+v", sReply.Rows)
			}
			if count != test.expCount {
				t.Errorf("%d: expected scan count %d; got %d", i, test.expCount, count)
			}
		case <-time.After(wait):
			if test.expFinish {
				t.Errorf("%d: scan failed to finish after %s", i, wait)
			} else {
				// Commit the unpushable txn so the read can finish.
				etArgs, etReply := endTxnArgs(txn, true, 1, store.StoreID())
				etArgs.Timestamp = txn.Timestamp
				if err := store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply}); err != nil {
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
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Lay down 10 intents to scan over.
	txn := newTransaction("test", proto.Key("foo"), 1, proto.SERIALIZABLE, store.ctx.Clock)
	keys := []proto.Key{}
	for j := 0; j < 10; j++ {
		key := proto.Key(fmt.Sprintf("key%02d", j))
		keys = append(keys, key)
		args, reply := putArgs(key, []byte(fmt.Sprintf("value%02d", j)), 1, store.StoreID())
		args.Txn = txn
		if err := store.ExecuteCmd(client.Call{Args: args, Reply: reply}); err != nil {
			t.Fatal(err)
		}
	}

	// Now, commit txn without resolving intents.
	etArgs, etReply := endTxnArgs(txn, true, 1, store.StoreID())
	etArgs.Timestamp = txn.Timestamp
	if err := store.ExecuteCmd(client.Call{Args: etArgs, Reply: etReply}); err != nil {
		t.Fatal(err)
	}

	// Scan the range repeatedly until we've verified count.
	sArgs, sReply := scanArgs(keys[0], keys[9].Next(), 1, store.StoreID())
	sArgs.ReadConsistency = proto.INCONSISTENT
	util.SucceedsWithin(t, 500*time.Millisecond, func() error {
		if err := store.ExecuteCmd(client.Call{Args: sArgs, Reply: sReply}); err != nil {
			return err
		}
		if len(sReply.Rows) != 10 {
			return util.Errorf("could not read rows as expected")
		}
		return nil
	})
}

func TestRaftNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)
	cases := []struct {
		nodeID   proto.NodeID
		storeID  proto.StoreID
		expected multiraft.NodeID
	}{
		{0, 1, 1},
		{1, 1, 0x100000001},
		{2, 3, 0x200000003},
		{math.MaxInt32, math.MaxInt32, 0x7fffffff7fffffff},
	}
	for _, c := range cases {
		x := MakeRaftNodeID(c.nodeID, c.storeID)
		if x != c.expected {
			t.Errorf("makeRaftNodeID(%v, %v) returned %v; expected %v",
				c.nodeID, c.storeID, x, c.expected)
		}
		n, s := DecodeRaftNodeID(x)
		if n != c.nodeID || s != c.storeID {
			t.Errorf("decodeRaftNodeID(%v) returned %v, %v; expected %v, %v",
				x, n, s, c.nodeID, c.storeID)
		}
	}

	panicCases := []struct {
		nodeID  proto.NodeID
		storeID proto.StoreID
	}{
		{1, 0},
		{1, -1},
		{-1, 1},
	}
	for _, c := range panicCases {
		func() {
			defer func() {
				_ = recover()
			}()
			x := MakeRaftNodeID(c.nodeID, c.storeID)
			t.Errorf("makeRaftNodeID(%v, %v) returned %v; expected panic",
				c.nodeID, c.storeID, x)
		}()
	}
}
