// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var emptyAddr = util.MakeUnresolvedAddr("test", "<test-addr>")

func newTestInfoStore() (*infoStore, *stop.Stopper) {
	stopper := stop.NewStopper()
	nc := &base.NodeIDContainer{}
	nc.Set(context.Background(), 1)
	is := newInfoStore(log.AmbientContext{Tracer: tracing.NewTracer()}, nc, emptyAddr, stopper)
	return is, stopper
}

// TestZeroDuration verifies that specifying a zero duration sets
// TTLStamp to max int64.
func TestZeroDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	info := is.newInfo(nil, 0)
	if info.TTLStamp != math.MaxInt64 {
		t.Errorf("expected zero duration to get max TTLStamp: %d", info.TTLStamp)
	}
}

// TestNewInfo creates new info objects. Verify sequence increments.
func TestNewInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	info1 := is.newInfo(nil, time.Second)
	info2 := is.newInfo(nil, time.Second)
	if err := is.addInfo("a", info1); err != nil {
		t.Error(err)
	}
	if err := is.addInfo("b", info2); err != nil {
		t.Error(err)
	}
	if info1.OrigStamp >= info2.OrigStamp {
		t.Errorf("timestamps should increment %d, %d", info1.OrigStamp, info2.OrigStamp)
	}
}

// TestInfoStoreGetInfo adds an info, and makes sure it can be fetched
// via getInfo. Also, verifies a non-existent info can't be fetched.
func TestInfoStoreGetInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	i := is.newInfo(nil, time.Second)
	i.NodeID = 1
	if err := is.addInfo("a", i); err != nil {
		t.Error(err)
	}
	if infoCount := len(is.Infos); infoCount != 1 {
		t.Errorf("infostore count incorrect %d != 1", infoCount)
	}
	if is.highWaterStamps[1] != i.OrigStamp {
		t.Error("high water timestamps map wasn't updated")
	}
	if is.getInfo("a") != i {
		t.Error("unable to get info")
	}
	if is.getInfo("b") != nil {
		t.Error("erroneously produced non-existent info for key b")
	}
}

// Verify TTL is respected on info fetched by key.
func TestInfoStoreGetInfoTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	i := is.newInfo(nil, time.Nanosecond)
	if err := is.addInfo("a", i); err != nil {
		t.Error(err)
	}
	time.Sleep(time.Nanosecond)
	if info := is.getInfo("a"); info != nil {
		t.Errorf("shouldn't be able to get info with short TTL, got %+v", info)
	}
}

// Add infos using same key, same and lesser timestamp; verify no
// replacement.
func TestAddInfoSameKeyLessThanEqualTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	info1 := is.newInfo(nil, time.Second)
	if err := is.addInfo("a", info1); err != nil {
		t.Error(err)
	}
	info2 := is.newInfo(nil, time.Second)
	info2.Value.Timestamp.WallTime = info1.Value.Timestamp.WallTime
	if err := is.addInfo("a", info2); err == nil {
		t.Error("able to add info2 with same timestamp")
	}
	info2.Value.Timestamp.WallTime--
	if err := is.addInfo("a", info2); err == nil {
		t.Error("able to add info2 with lesser timestamp")
	}
	// Verify info2 did not replace info1.
	if is.getInfo("a") != info1 {
		t.Error("info1 was replaced, despite same timestamp")
	}
}

// Add infos using same key, same timestamp; verify no replacement.
func TestAddInfoSameKeyGreaterTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	info1 := is.newInfo(nil, time.Second)
	info2 := is.newInfo(nil, time.Second)
	if err1, err2 := is.addInfo("a", info1), is.addInfo("a", info2); err1 != nil || err2 != nil {
		t.Error(err1, err2)
	}
}

// Verify that adding two infos with different hops but same keys
// always chooses the minimum hops.
func TestAddInfoSameKeyDifferentHops(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	info1 := is.newInfo(nil, time.Second)
	info1.Hops = 1
	info2 := is.newInfo(nil, time.Second)
	info2.Value.Timestamp.WallTime = info1.Value.Timestamp.WallTime
	info2.Hops = 2
	if err := is.addInfo("a", info1); err != nil {
		t.Errorf("failed insert: %s", err)
	}
	if err := is.addInfo("a", info2); err == nil {
		t.Errorf("shouldn't have inserted info 2: %s", err)
	}

	i := is.getInfo("a")
	if i.Hops != info1.Hops || !reflect.DeepEqual(i, info1) {
		t.Error("failed to properly combine hops and value", i)
	}

	// Try yet another info, with lower hops yet (0).
	info3 := is.newInfo(nil, time.Second)
	if err := is.addInfo("a", info3); err != nil {
		t.Error(err)
	}
	i = is.getInfo("a")
	if i.Hops != info3.Hops || !reflect.DeepEqual(i, info3) {
		t.Error("failed to properly combine hops and value", i)
	}
}

func TestCombineInfosRatchetMonotonic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, local := range []bool{true, false} {
		t.Run(fmt.Sprintf("local=%t", local), func(t *testing.T) {
			is, stopper := newTestInfoStore()
			defer stopper.Stop(context.Background())

			// Generate an info with a timestamp in the future.
			info := &Info{
				NodeID:    is.nodeID.Get(),
				TTLStamp:  math.MaxInt64,
				OrigStamp: monotonicUnixNano() + int64(time.Hour),
			}
			if !local {
				info.NodeID++
			}

			// Reset the monotonic clock.
			monoTime.Lock()
			monoTime.last = 0
			monoTime.Unlock()

			fresh, err := is.combine(map[string]*Info{"hello": info}, 2)
			if err != nil {
				t.Fatal(err)
			}
			if fresh != 1 {
				t.Fatalf("expected no infos to be added, but found %d", fresh)
			}

			// Verify the monotonic clock was ratcheted if the info was generated
			// locally.
			monoTime.Lock()
			last := monoTime.last
			monoTime.Unlock()
			var expectedLast int64
			if local {
				expectedLast = info.OrigStamp
				if now := monotonicUnixNano(); now <= last {
					t.Fatalf("expected mono-time to increase: %d <= %d", now, last)
				}
			}
			if expectedLast != last {
				t.Fatalf("expected mono-time %d, but found %d", expectedLast, last)
			}

			if i := is.getInfo("hello"); i == nil {
				t.Fatalf("expected to find info\n%v", is.Infos)
			}
		})
	}
}

// Helper method creates an infostore with 10 infos.
func createTestInfoStore(t *testing.T) *infoStore {
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())

	for i := 0; i < 10; i++ {
		infoA := is.newInfo(nil, time.Second)
		infoA.NodeID = 1
		infoA.Hops = 1
		if err := is.addInfo(fmt.Sprintf("a.%d", i), infoA); err != nil {
			t.Fatal(err)
		}

		infoB := is.newInfo(nil, time.Second)
		infoB.NodeID = 2
		infoB.Hops = 2
		if err := is.addInfo(fmt.Sprintf("b.%d", i), infoB); err != nil {
			t.Fatal(err)
		}

		infoC := is.newInfo(nil, time.Second)
		infoC.NodeID = 3
		infoC.Hops = 3
		if err := is.addInfo(fmt.Sprintf("c.%d", i), infoC); err != nil {
			t.Fatal(err)
		}
	}

	return is
}

// Check infostore delta based on info high water timestamps.
func TestInfoStoreDelta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is := createTestInfoStore(t)

	// Verify deltas with successive high water timestamps & min hops.
	infos := is.delta(map[roachpb.NodeID]int64{})
	for i := 0; i < 10; i++ {
		if i > 0 {
			infoA := is.getInfo(fmt.Sprintf("a.%d", i-1))
			infoB := is.getInfo(fmt.Sprintf("b.%d", i-1))
			infoC := is.getInfo(fmt.Sprintf("c.%d", i-1))
			infos = is.delta(map[roachpb.NodeID]int64{
				1: infoA.OrigStamp,
				2: infoB.OrigStamp,
				3: infoC.OrigStamp,
			})
		}

		for _, node := range []string{"a", "b", "c"} {
			for j := 0; j < 10; j++ {
				expected := i <= j
				if _, ok := infos[fmt.Sprintf("%s.%d", node, j)]; ok != expected {
					t.Errorf("i,j=%d,%d: expected to fetch info %s.%d? %t; got %t", i, j, node, j, expected, ok)
				}
			}
		}
	}

	if infos := is.delta(map[roachpb.NodeID]int64{
		1: math.MaxInt64,
		2: math.MaxInt64,
		3: math.MaxInt64,
	}); len(infos) != 0 {
		t.Errorf("fetching delta of infostore at maximum timestamp should return empty, got %v", infos)
	}
}

// TestInfoStoreMostDistant verifies selection of most distant node &
// associated hops.
func TestInfoStoreMostDistant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := []roachpb.NodeID{
		roachpb.NodeID(1),
		roachpb.NodeID(2),
		roachpb.NodeID(3),
	}
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())

	// Start with one very distant info that shouldn't affect mostDistant
	// calculations because it isn't a node ID key.
	scInfo := is.newInfo(nil, time.Second)
	scInfo.Hops = 100
	scInfo.NodeID = nodes[0]
	if err := is.addInfo(KeySystemConfig, scInfo); err != nil {
		t.Fatal(err)
	}

	// Add info from each address, with hop count equal to index+1.
	var expectedNodeID roachpb.NodeID
	var expectedHops uint32
	for i := 0; i < len(nodes); i++ {
		inf := is.newInfo(nil, time.Second)
		inf.Hops = uint32(i + 1)
		inf.NodeID = nodes[i]
		if err := is.addInfo(MakeNodeIDKey(inf.NodeID), inf); err != nil {
			t.Fatal(err)
		}
		if inf.NodeID != 1 {
			expectedNodeID = inf.NodeID
			expectedHops = inf.Hops
		}
		nodeID, hops := is.mostDistant(func(roachpb.NodeID) bool { return false })
		if expectedNodeID != nodeID {
			t.Errorf("%d: expected n%d; got %d", i, expectedNodeID, nodeID)
		}
		if expectedHops != hops {
			t.Errorf("%d: expected hops %d; got %d", i, expectedHops, hops)
		}
	}

	// Finally, simulate a Gossip instance that has an outgoing connection
	// and expect the outgoing connection to not be recommended even though
	// it's the furthest node away.
	filteredNode := nodes[len(nodes)-1]
	expectedNode := nodes[len(nodes)-2]
	expectedHops = uint32(expectedNode)
	nodeID, hops := is.mostDistant(func(nodeID roachpb.NodeID) bool {
		return nodeID == filteredNode
	})
	if nodeID != expectedNode {
		t.Errorf("expected n%d; got %d", expectedNode, nodeID)
	}
	if hops != expectedHops {
		t.Errorf("expected hops %d; got %d", expectedHops, hops)
	}
}

// TestLeastUseful verifies that the least-contributing peer node
// can be determined.
func TestLeastUseful(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodes := []roachpb.NodeID{
		roachpb.NodeID(1),
		roachpb.NodeID(2),
	}
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())

	set := makeNodeSet(3, metric.NewGauge(metric.Metadata{Name: ""}))
	if is.leastUseful(set) != 0 {
		t.Error("not expecting a node from an empty set")
	}

	inf1 := is.newInfo(nil, time.Second)
	inf1.NodeID = 1
	inf1.PeerID = 1
	if err := is.addInfo("a1", inf1); err != nil {
		t.Fatal(err)
	}
	if is.leastUseful(set) != 0 {
		t.Error("not expecting a node from an empty set")
	}

	set.addNode(nodes[0])
	if is.leastUseful(set) != nodes[0] {
		t.Error("expecting nodes[0] as least useful")
	}

	inf2 := is.newInfo(nil, time.Second)
	inf2.NodeID = 2
	inf2.PeerID = 1
	if err := is.addInfo("a2", inf2); err != nil {
		t.Fatal(err)
	}
	if is.leastUseful(set) != nodes[0] {
		t.Error("expecting nodes[0] as least useful")
	}

	set.addNode(nodes[1])
	if is.leastUseful(set) != nodes[1] {
		t.Error("expecting nodes[1] as least useful")
	}

	inf3 := is.newInfo(nil, time.Second)
	inf3.NodeID = 2
	inf3.PeerID = 2
	if err := is.addInfo("a3", inf3); err != nil {
		t.Fatal(err)
	}
	if is.leastUseful(set) != nodes[1] {
		t.Error("expecting nodes[1] as least useful")
	}
}

type callbackRecord struct {
	keys []string
	wg   *sync.WaitGroup
	syncutil.Mutex
}

func (cr *callbackRecord) Add(key string, _ roachpb.Value) {
	cr.Lock()
	defer cr.Unlock()
	cr.keys = append(cr.keys, key)
	cr.wg.Done()
}

func (cr *callbackRecord) Keys() []string {
	cr.Lock()
	defer cr.Unlock()
	return append([]string(nil), cr.keys...)
}

func TestCallbacks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	wg := &sync.WaitGroup{}
	cb1 := callbackRecord{wg: wg}
	cb2 := callbackRecord{wg: wg}
	cbAll := callbackRecord{wg: wg}

	unregisterCB1 := is.registerCallback("key1", cb1.Add)
	is.registerCallback("key2", cb2.Add)
	is.registerCallback("key.*", cbAll.Add, Redundant)

	i1 := is.newInfo(nil, time.Second)
	i2 := is.newInfo(nil, time.Second)
	i3 := is.newInfo(nil, time.Second)

	// Add infos twice and verify callbacks aren't called for same timestamps.
	for i := 0; i < 2; i++ {
		for _, test := range []struct {
			key   string
			info  *Info
			count int
		}{
			{"key1", i1, 2},
			{"key2", i2, 2},
			{"key3", i3, 1},
		} {
			if i == 0 {
				wg.Add(test.count)
			}
			if err := is.addInfo(test.key, test.info); err != nil {
				if i == 0 {
					t.Error(err)
				}
			} else if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
			wg.Wait()
		}

		if expKeys := []string{"key1"}; !reflect.DeepEqual(cb1.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb1.Keys())
		}
		if expKeys := []string{"key2"}; !reflect.DeepEqual(cb2.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb2.Keys())
		}
		keys := cbAll.Keys()
		if expKeys := []string{"key1", "key2", "key3"}; !reflect.DeepEqual(keys, expKeys) {
			t.Errorf("expected %v, got %v", expKeys, keys)
		}
	}

	// Update an info twice.
	for i := 0; i < 2; i++ {
		i1 := is.newInfo([]byte("a"), time.Second)
		// The first time both callbacks will fire because the value has
		// changed. The second time cbAll (created with the Redundant option) will
		// fire.
		wg.Add(2 - i)
		if err := is.addInfo("key1", i1); err != nil {
			t.Error(err)
		}
		wg.Wait()

		if expKeys := []string{"key1", "key1"}; !reflect.DeepEqual(cb1.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb1.Keys())
		}
		if expKeys := []string{"key2"}; !reflect.DeepEqual(cb2.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb2.Keys())
		}
	}

	if expKeys := []string{"key1", "key2", "key3", "key1", "key1"}; !reflect.DeepEqual(cbAll.Keys(), expKeys) {
		t.Errorf("expected %v, got %v", expKeys, cbAll.Keys())
	}

	const numInfos = 3

	// Register another callback with same pattern and verify it is
	// invoked for all three keys.
	wg.Add(numInfos)
	is.registerCallback("key.*", cbAll.Add)
	wg.Wait()

	expKeys := []string{"key1", "key2", "key3"}
	keys := cbAll.Keys()
	keys = keys[len(keys)-numInfos:]
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, expKeys) {
		t.Errorf("expected %v, got %v", expKeys, keys)
	}

	// Unregister a callback and verify nothing is invoked on it.
	unregisterCB1()
	iNew := is.newInfo([]byte("b"), time.Second)
	wg.Add(2) // for the two cbAll callbacks
	if err := is.addInfo("key1", iNew); err != nil {
		t.Error(err)
	}
	wg.Wait()
	if len(cb1.Keys()) != 2 {
		t.Errorf("expected no new cb1 keys, got %v", cb1.Keys())
	}
}

// TestRegisterCallback verifies that a callback is invoked when
// registered if there are items which match its regexp in the
// infostore.
func TestRegisterCallback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	is, stopper := newTestInfoStore()
	defer stopper.Stop(context.Background())
	wg := &sync.WaitGroup{}
	cb := callbackRecord{wg: wg}

	i1 := is.newInfo(nil, time.Second)
	i2 := is.newInfo(nil, time.Second)
	if err := is.addInfo("key1", i1); err != nil {
		t.Fatal(err)
	}
	if err := is.addInfo("key2", i2); err != nil {
		t.Fatal(err)
	}

	wg.Add(2)
	is.registerCallback("key.*", cb.Add)
	wg.Wait()
	actKeys := cb.Keys()
	sort.Strings(actKeys)
	if expKeys := []string{"key1", "key2"}; !reflect.DeepEqual(actKeys, expKeys) {
		t.Errorf("expected %v, got %v", expKeys, cb.Keys())
	}
}
