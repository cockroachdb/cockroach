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

package gossip

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

var emptyAddr = util.MakeUnresolvedAddr("test", "<test-addr>")

// TestZeroDuration verifies that specifying a zero duration sets
// TTLStamp to max int64.
func TestZeroDuration(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info := is.newInfo(nil, 0)
	if info.TTLStamp != math.MaxInt64 {
		t.Errorf("expected zero duration to get max TTLStamp: %d", info.TTLStamp)
	}
}

// TestNewInfo creates new info objects. Verify sequence increments.
func TestNewInfo(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info1 := is.newInfo(nil, time.Second)
	info2 := is.newInfo(nil, time.Second)
	if info1.seq != info2.seq-1 {
		t.Errorf("sequence numbers should increment %d, %d", info1.seq, info2.seq)
	}
}

// TestInfoStoreGetInfo adds an info, and makes sure it can be fetched
// via getInfo. Also, verifies a non-existent info can't be fetched.
func TestInfoStoreGetInfo(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	i := is.newInfo(nil, time.Second)
	if err := is.addInfo("a", i); err != nil {
		t.Error(err)
	}
	if infoCount := len(is.Infos); infoCount != 1 {
		t.Errorf("infostore count incorrect %d != 1", infoCount)
	}
	if is.MaxSeq != i.seq {
		t.Error("max seq value wasn't updated")
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
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	i := is.newInfo(nil, time.Nanosecond)
	if err := is.addInfo("a", i); err != nil {
		t.Error(err)
	}
	time.Sleep(time.Nanosecond)
	if is.getInfo("a") != nil {
		t.Error("shouldn't be able to get info with short TTL")
	}
}

// Add infos using same key, same and lesser timestamp; verify no
// replacement.
func TestAddInfoSameKeyLessThanEqualTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
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
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info1 := is.newInfo(nil, time.Second)
	info2 := is.newInfo(nil, time.Second)
	if err1, err2 := is.addInfo("a", info1), is.addInfo("a", info2); err1 != nil || err2 != nil {
		t.Error(err1, err2)
	}
}

// Verify that adding two infos with different hops but same keys
// always chooses the minimum hops.
func TestAddInfoSameKeyDifferentHops(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
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
	if i.Hops != info1.Hops || !gogoproto.Equal(i, info1) {
		t.Error("failed to properly combine hops and value", i)
	}

	// Try yet another info, with lower hops yet (0).
	info3 := is.newInfo(nil, time.Second)
	if err := is.addInfo("a", info3); err != nil {
		t.Error(err)
	}
	i = is.getInfo("a")
	if i.Hops != info3.Hops || !gogoproto.Equal(i, info3) {
		t.Error("failed to properly combine hops and value", i)
	}
}

// Helper method creates an infostore with 10 infos.
func createTestInfoStore(t *testing.T) infoStore {
	is := newInfoStore(1, emptyAddr)

	for i := 0; i < 10; i++ {
		infoA := is.newInfo(nil, time.Second)
		if err := is.addInfo(fmt.Sprintf("a.%d", i), infoA); err != nil {
			t.Fatal(err)
		}

		infoB := is.newInfo(nil, time.Second)
		if err := is.addInfo(fmt.Sprintf("b.%d", i), infoB); err != nil {
			t.Fatal(err)
		}

		infoC := is.newInfo(nil, time.Second)
		if err := is.addInfo(fmt.Sprintf("c.%d", i), infoC); err != nil {
			t.Fatal(err)
		}
	}

	return is
}

// Check infostore delta based on info sequence numbers.
func TestInfoStoreDelta(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := createTestInfoStore(t)

	// Verify deltas with successive sequence numbers.
	for i := 0; i < 10; i++ {
		infos := is.delta(2, int64(i*3))

		for j := 0; j < 10-i; j++ {
			if _, ok := infos[fmt.Sprintf("c.%d", j+i)]; !ok {
				t.Errorf("unable to fetch info %d", j+i)
			}
			if i > 0 {
				if _, ok := infos[fmt.Sprintf("c.%d", 0)]; ok {
					t.Errorf("erroneously fetched info %d", j+i+1)
				}
			}
		}
	}

	if infos := is.delta(2, int64(30)); len(infos) != 0 {
		t.Error("fetching delta of infostore at maximum sequence number should return nil")
	}
}

// TestInfoStoreDistant verifies selection of infos from store with
// Hops > maxHops.
func TestInfoStoreDistant(t *testing.T) {
	defer leaktest.AfterTest(t)
	nodes := []proto.NodeID{
		proto.NodeID(1),
		proto.NodeID(2),
		proto.NodeID(3),
	}
	is := newInfoStore(1, emptyAddr)
	// Add info from each address, with hop count equal to index+1.
	for i := 0; i < len(nodes); i++ {
		inf := is.newInfo(nil, time.Second)
		inf.Hops = uint32(i + 1)
		inf.NodeID = nodes[i]
		if err := is.addInfo(fmt.Sprintf("b.%d", i), inf); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < len(nodes); i++ {
		nodesLen := is.distant(uint32(i)).len()
		if nodesLen != 3-i {
			t.Errorf("%d nodes (not %d) should be over maxHops = %d", 3-i, nodesLen, i)
		}
	}
}

// TestLeastUseful verifies that the least-contributing peer node
// can be determined.
func TestLeastUseful(t *testing.T) {
	defer leaktest.AfterTest(t)
	nodes := []proto.NodeID{
		proto.NodeID(1),
		proto.NodeID(2),
	}
	is := newInfoStore(1, emptyAddr)

	set := makeNodeSet(3)
	if is.leastUseful(set) != 0 {
		t.Error("not expecting a node from an empty set")
	}

	inf1 := is.newInfo(nil, time.Second)
	inf1.peerID = 1
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
	inf2.peerID = 1
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
	inf3.peerID = 2
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
	sync.Mutex
}

func (cr *callbackRecord) Add(key string, contentsChanged bool, _ []byte) {
	cr.Lock()
	defer cr.Unlock()
	cr.keys = append(cr.keys, fmt.Sprintf("%s-%t", key, contentsChanged))
	cr.wg.Done()
}

func (cr *callbackRecord) Keys() []string {
	cr.Lock()
	defer cr.Unlock()
	return append([]string(nil), cr.keys...)
}

func TestCallbacks(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	wg := &sync.WaitGroup{}
	cb1 := callbackRecord{wg: wg}
	cb2 := callbackRecord{wg: wg}
	cbAll := callbackRecord{wg: wg}

	is.registerCallback("key1", cb1.Add)
	is.registerCallback("key2", cb2.Add)
	is.registerCallback("key.*", cbAll.Add)

	i1 := is.newInfo(nil, time.Second)
	i2 := is.newInfo(nil, time.Second)
	i3 := is.newInfo(nil, time.Second)

	// Add infos twice and verify callbacks aren't called for same timestamps.
	wg.Add(5)
	for i := 0; i < 2; i++ {
		if err := is.addInfo("key1", i1); err != nil {
			if i == 0 {
				t.Error(err)
			}
		} else {
			if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
		}
		if err := is.addInfo("key2", i2); err != nil {
			if i == 0 {
				t.Error(err)
			}
		} else {
			if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
		}
		if err := is.addInfo("key3", i3); err != nil {
			if i == 0 {
				t.Error(err)
			}
		} else {
			if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
		}
		wg.Wait()

		if expKeys := []string{"key1-true"}; !reflect.DeepEqual(cb1.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb1.Keys())
		}
		if expKeys := []string{"key2-true"}; !reflect.DeepEqual(cb2.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb2.Keys())
		}
		keys := cbAll.Keys()
		sort.Strings(keys)
		if expKeys := []string{"key1-true", "key2-true", "key3-true"}; !reflect.DeepEqual(keys, expKeys) {
			t.Errorf("expected %v, got %v", expKeys, keys)
		}
	}

	// Update an info.
	{
		i1 := is.newInfo([]byte("a"), time.Second)
		wg.Add(2)
		if err := is.addInfo("key1", i1); err != nil {
			t.Error(err)
		}
		wg.Wait()

		if expKeys := []string{"key1-true", "key1-true"}; !reflect.DeepEqual(cb1.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb1.Keys())
		}
		if expKeys := []string{"key2-true"}; !reflect.DeepEqual(cb2.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb2.Keys())
		}
		keys := cbAll.Keys()
		sort.Strings(keys)
		if expKeys := []string{"key1-true", "key1-true", "key2-true", "key3-true"}; !reflect.DeepEqual(keys, expKeys) {
			t.Errorf("expected %v, got %v", expKeys, keys)
		}
	}

	// Update an info but not its value.
	{
		i1 := is.newInfo([]byte("a"), 2*time.Second)
		wg.Add(2)
		if err := is.addInfo("key1", i1); err != nil {
			t.Error(err)
		}
		wg.Wait()

		if expKeys := []string{"key1-true", "key1-true", "key1-false"}; !reflect.DeepEqual(cb1.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb1.Keys())
		}
		if expKeys := []string{"key2-true"}; !reflect.DeepEqual(cb2.Keys(), expKeys) {
			t.Errorf("expected %v, got %v", expKeys, cb2.Keys())
		}
		keys := cbAll.Keys()
		sort.Strings(keys)
		if expKeys := []string{"key1-false", "key1-true", "key1-true", "key2-true", "key3-true"}; !reflect.DeepEqual(keys, expKeys) {
			t.Errorf("expected %v, got %v", expKeys, keys)
		}
	}

	// Register another callback with same pattern and verify it is
	// invoked for all three keys.
	wg.Add(3)
	is.registerCallback("key.*", cbAll.Add)
	wg.Wait()

	expKeys := []string{"key1-false", "key1-true", "key2-true", "key3-true", "key1-true", "key1-true", "key2-true", "key3-true"}
	sort.Strings(expKeys)
	keys := cbAll.Keys()
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, expKeys) {
		t.Errorf("expected %v, got %v", expKeys, keys)
	}
}

// TestRegisterCallback verifies that a callback is invoked when
// registered if there are items which match its regexp in the
// infostore.
func TestRegisterCallback(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
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
	if expKeys := []string{"key1-true", "key2-true"}; !reflect.DeepEqual(actKeys, expKeys) {
		t.Errorf("expected %v, got %v", expKeys, cb.Keys())
	}
}
