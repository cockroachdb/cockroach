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
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// testAddr and emptyAddr are defined in info_test.go.

// TestRegisterGroup registers two groups and verifies operation of
// belongsToGroup.
func TestRegisterGroup(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)

	groupA := newGroup("a", 1, MinGroup)
	if is.registerGroup(groupA) != nil {
		t.Error("could not register group A")
	}
	groupB := newGroup("b", 1, MinGroup)
	if is.registerGroup(groupB) != nil {
		t.Error("could not register group B")
	}

	if is.belongsToGroup("a.b") != groupA {
		t.Error("should belong to group A")
	}
	if is.belongsToGroup("a.c") != groupA {
		t.Error("should belong to group A")
	}
	if is.belongsToGroup("b.a") != groupB {
		t.Error("should belong to group B")
	}
	if is.belongsToGroup("c.a") != nil {
		t.Error("shouldn't belong to a group")
	}

	// Try to register a group that's already been registered; will
	// succeed if identical.
	if is.registerGroup(groupA) != nil {
		t.Error("should be able to register group A twice")
	}
	// Now change the group type and try again.
	groupAAlt := newGroup("a", 1, MaxGroup)
	if is.registerGroup(groupAAlt) == nil {
		t.Error("should not be able to register group A again with different properties")
	}
}

// TestZeroDuration verifies that specifying a zero duration sets
// TTLStamp to max int64.
func TestZeroDuration(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info := is.newInfo("a", float64(1), 0)
	if info.TTLStamp != math.MaxInt64 {
		t.Errorf("expected zero duration to get max TTLStamp: %d", info.TTLStamp)
	}
}

// TestNewInfo creates new info objects. Verify sequence increments.
func TestNewInfo(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info1 := is.newInfo("a", float64(1), time.Second)
	info2 := is.newInfo("a", float64(1), time.Second)
	if info1.Seq != info2.Seq-1 {
		t.Errorf("sequence numbers should increment %d, %d", info1.Seq, info2.Seq)
	}
}

// TestInfoStoreGetInfo adds an info, and makes sure it can be fetched
// via getInfo. Also, verifies a non-existent info can't be fetched.
func TestInfoStoreGetInfo(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	i := is.newInfo("a", float64(1), time.Second)
	if err := is.addInfo(i); err != nil {
		t.Error(err)
	}
	if is.infoCount() != 1 {
		t.Errorf("infostore count incorrect %d != 1", is.infoCount())
	}
	if is.MaxSeq != i.Seq {
		t.Error("max seq value wasn't updated")
	}
	if is.getInfo("a") != i {
		t.Error("unable to get info")
	}
	if is.getInfo("b") != nil {
		t.Error("erroneously produced non-existent info for key b")
	}
}

// Verify TTL is respected on info fetched by key
// and group.
func TestInfoStoreGetInfoTTL(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	i := is.newInfo("a", float64(1), time.Nanosecond)
	if err := is.addInfo(i); err != nil {
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
	info1 := is.newInfo("a", float64(1), time.Second)
	if err := is.addInfo(info1); err != nil {
		t.Error(err)
	}
	info2 := is.newInfo("a", float64(2), time.Second)
	info2.Timestamp = info1.Timestamp
	if err := is.addInfo(info2); err == nil {
		t.Error("able to add info2 with same timestamp")
	}
	info2.Timestamp--
	if err := is.addInfo(info2); err == nil {
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
	info1 := is.newInfo("a", float64(1), time.Second)
	info2 := is.newInfo("a", float64(2), time.Second)
	if err1, err2 := is.addInfo(info1), is.addInfo(info2); err1 != nil || err2 != nil {
		t.Error(err1, err2)
	}
}

// Verify that adding two infos with different hops but same keys
// always chooses the minimum hops.
func TestAddInfoSameKeyDifferentHops(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)
	info1 := is.newInfo("a", float64(1), time.Second)
	info1.Hops = 1
	info2 := is.newInfo("a", float64(2), time.Second)
	info2.Timestamp = info1.Timestamp
	info2.Hops = 2
	if err := is.addInfo(info1); err != nil {
		t.Errorf("failed insert: %s", err)
	}
	if err := is.addInfo(info2); err == nil {
		t.Errorf("shouldn't have inserted info 2: %s", err)
	}

	i := is.getInfo("a")
	if i.Hops != info1.Hops || i.value() != info1.value() {
		t.Error("failed to properly combine hops and value", i)
	}

	// Try yet another info, with lower hops yet (0).
	info3 := is.newInfo("a", float64(3), time.Second)
	if err := is.addInfo(info3); err != nil {
		t.Error(err)
	}
	i = is.getInfo("a")
	if i.Hops != info3.Hops || i.value() != info3.value() {
		t.Error("failed to properly combine hops and value", i)
	}
}

// Register groups, add and fetch group infos from min/max groups and
// verify ordering. Add an additional non-group info and fetch that as
// well.
func TestAddGroupInfos(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := newInfoStore(1, emptyAddr)

	group := newGroup("a", 10, MinGroup)
	if is.registerGroup(group) != nil {
		t.Error("could not register group")
	}

	info1 := is.newInfo("a.a", float64(1), time.Second)
	info2 := is.newInfo("a.b", float64(2), time.Second)
	if err1, err2 := is.addInfo(info1), is.addInfo(info2); err1 != nil || err2 != nil {
		t.Error(err1, err2)
	}
	if is.infoCount() != 2 {
		t.Errorf("infostore count incorrect %d != 2", is.infoCount())
	}
	if is.MaxSeq != info2.Seq {
		t.Errorf("store max seq info2 seq %d != %d", is.MaxSeq, info2.Seq)
	}

	infos := is.getGroupInfos("a")
	if infos == nil {
		t.Error("unable to fetch group infos")
	}
	if infos[0].Key != "a.a" || infos[1].Key != "a.b" {
		t.Error("fetch group infos have incorrect order:", infos)
	}

	// Try with a max group.
	MaxGroup := newGroup("b", 10, MaxGroup)
	if is.registerGroup(MaxGroup) != nil {
		t.Error("could not register group")
	}
	info3 := is.newInfo("b.a", float64(1), time.Second)
	info4 := is.newInfo("b.b", float64(2), time.Second)
	if err1, err2 := is.addInfo(info3), is.addInfo(info4); err1 != nil || err2 != nil {
		t.Error(err1, err2)
	}
	if is.infoCount() != 4 {
		t.Errorf("infostore count incorrect %d != 4", is.infoCount())
	}
	if is.MaxSeq != info4.Seq {
		t.Errorf("store max seq info4 seq %d != %d", is.MaxSeq, info4.Seq)
	}

	infos = is.getGroupInfos("b")
	if infos == nil {
		t.Error("unable to fetch group infos")
	}
	if infos[0].Key != "b.b" || infos[1].Key != "b.a" {
		t.Error("fetch group infos have incorrect order:", infos)
	}

	// Finally, add a non-group info and verify it cannot be fetched
	// by group, but can be fetched solo.
	info5 := is.newInfo("c.a", float64(3), time.Second)
	if err := is.addInfo(info5); err != nil {
		t.Error(err)
	}
	if is.getGroupInfos("c") != nil {
		t.Error("shouldn't be able to fetch non-existent group c")
	}
	if is.getInfo("c.a") != info5 {
		t.Error("unable to fetch info5 by key")
	}
	if is.infoCount() != 5 {
		t.Errorf("infostore count incorrect %d != 5", is.infoCount())
	}
	if is.MaxSeq != info5.Seq {
		t.Errorf("store max seq info5 seq %d != %d", is.MaxSeq, info5.Seq)
	}
}

// Verify infostore combination with overlapping group and non-group
// infos.
func TestCombine(t *testing.T) {
	defer leaktest.AfterTest(t)
	is1 := newInfoStore(1, emptyAddr)

	group1 := newGroup("a", 10, MinGroup)
	group1Overlap := newGroup("b", 10, MinGroup)
	if is1.registerGroup(group1) != nil || is1.registerGroup(group1Overlap) != nil {
		t.Error("could not register group1 or group1Overlap")
	}

	info1a := is1.newInfo("a.a", float64(1), time.Second)
	info1b := is1.newInfo("a.b", float64(2), time.Second)
	info1c := is1.newInfo("a", float64(3), time.Second) // non-group info
	if is1.addInfo(info1a) != nil || is1.addInfo(info1b) != nil || is1.addInfo(info1c) != nil {
		t.Error("unable to add infos")
	}
	info1Overlap := is1.newInfo("b.a", float64(3), time.Second)
	if err := is1.addInfo(info1Overlap); err != nil {
		t.Error("unable to add info1Overlap:", err)
	}

	is2 := newInfoStore(2, testAddr("peer"))

	group2 := newGroup("c", 10, MinGroup)
	group2Overlap := newGroup("b", 10, MinGroup)
	if is2.registerGroup(group2) != nil || is2.registerGroup(group2Overlap) != nil {
		t.Error("could not register group2 or group2Overlap")
	}

	info2a := is2.newInfo("c.a", float64(1), time.Second)
	info2b := is2.newInfo("c.b", float64(2), time.Second)
	info2c := is2.newInfo("c", float64(3), time.Second)
	if is2.addInfo(info2a) != nil || is2.addInfo(info2b) != nil || is2.addInfo(info2c) != nil {
		t.Error("unable to add infos")
	}
	info2Overlap := is2.newInfo("b.a", float64(4), time.Second)
	if err := is2.addInfo(info2Overlap); err != nil {
		t.Error("unable to add info2Overlap:", err)
	}

	if freshCount := is1.combine(is2); freshCount != 4 {
		t.Error("expected 4 fresh infos on combine")
	}

	infosA := is1.getGroupInfos("a")
	if len(infosA) != 2 || infosA[0].Key != "a.a" || infosA[1].Key != "a.b" {
		t.Error("group a missing", infosA[0], infosA[1])
	}
	if infosA[0].PeerID != 1 || infosA[1].PeerID != 1 {
		t.Error("infoA peer nodes not set properly", infosA[0], infosA[1])
	}

	infosB := is1.getGroupInfos("b")
	if len(infosB) != 1 || infosB[0].Key != "b.a" || infosB[0].value() != info2Overlap.value() {
		t.Error("group b missing", infosB)
	}
	if infosB[0].PeerID != 2 {
		t.Error("infoB peer node not set properly", infosB[0])
	}

	infosC := is1.getGroupInfos("c")
	if len(infosC) != 2 || infosC[0].Key != "c.a" || infosC[1].Key != "c.b" {
		t.Error("group c missing", infosC)
	}
	if infosC[0].PeerID != 2 || infosC[1].PeerID != 2 {
		t.Error("infoC peer nodes not set properly", infosC[0], infosC[1])
	}

	if is1.getInfo("a") == nil {
		t.Error("non-group a info missing")
	}
	if is1.getInfo("c") == nil {
		t.Error("non-group c info missing")
	}

	// Combine again and verify 0 fresh infos.
	if freshCount := is1.combine(is2); freshCount != 0 {
		t.Error("expected no fresh infos on follow-up combine")
	}
}

// Helper method creates an infostore with two groups with 10
// infos each and 10 non-group infos.
func createTestInfoStore(t *testing.T) *infoStore {
	is := newInfoStore(1, emptyAddr)

	groupA := newGroup("a", 10, MinGroup)
	groupB := newGroup("b", 10, MinGroup)
	if is.registerGroup(groupA) != nil || is.registerGroup(groupB) != nil {
		t.Error("unable to register groups")
	}

	// Insert 10 keys each for groupA, groupB and non-group successively.
	for i := 0; i < 10; i++ {
		infoA := is.newInfo(fmt.Sprintf("a.%d", i), float64(i), time.Second)
		if err := is.addInfo(infoA); err != nil {
			t.Fatal(err)
		}

		infoB := is.newInfo(fmt.Sprintf("b.%d", i), float64(i+1), time.Second)
		if err := is.addInfo(infoB); err != nil {
			t.Fatal(err)
		}

		infoC := is.newInfo(fmt.Sprintf("c.%d", i), float64(i+2), time.Second)
		if err := is.addInfo(infoC); err != nil {
			t.Fatal(err)
		}
	}

	return is
}

// Check infostore delta (both group and non-group infos) based on
// info sequence numbers.
func TestInfoStoreDelta(t *testing.T) {
	defer leaktest.AfterTest(t)
	is := createTestInfoStore(t)

	// Verify deltas with successive sequence numbers.
	for i := 0; i < 10; i++ {
		delta := is.delta(2, int64(i*3))
		infosA := delta.getGroupInfos("a")
		infosB := delta.getGroupInfos("b")
		if len(infosA) != 10-i || len(infosB) != 10-i {
			t.Fatalf("expected %d infos, not %d, %d", 10-i, len(infosA), len(infosB))
		}
		for j := 0; j < 10-i; j++ {
			expAKey := fmt.Sprintf("a.%d", j+i)
			expBKey := fmt.Sprintf("b.%d", j+i)
			if infosA[j].Key != expAKey || infosB[j].Key != expBKey {
				t.Errorf("run %d: key mismatch at index %d: %s != %s, %s != %s",
					i, j, infosA[j].Key, expAKey, infosB[j].Key, expBKey)
			}

			infoC := delta.getInfo(fmt.Sprintf("c.%d", j+i))
			if infoC == nil {
				t.Errorf("unable to fetch non-group info %d", j+i)
			}
			if i > 0 {
				infoC = delta.getInfo(fmt.Sprintf("c.%d", 0))
				if infoC != nil {
					t.Errorf("erroneously fetched non-group info %d", j+i+1)
				}
			}
		}
	}

	if delta := is.delta(2, int64(30)); delta != nil {
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
		inf := is.newInfo(fmt.Sprintf("b.%d", i), float64(i), time.Second)
		inf.Hops = uint32(i + 1)
		inf.NodeID = nodes[i]
		if err := is.addInfo(inf); err != nil {
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

	set := newNodeSet(3)
	if is.leastUseful(set) != 0 {
		t.Error("not expecting a node from an empty set")
	}

	inf1 := is.newInfo("a1", float64(1), time.Second)
	inf1.PeerID = 1
	if err := is.addInfo(inf1); err != nil {
		t.Fatal(err)
	}
	if is.leastUseful(set) != 0 {
		t.Error("not expecting a node from an empty set")
	}

	set.addNode(nodes[0])
	if is.leastUseful(set) != nodes[0] {
		t.Error("expecting nodes[0] as least useful")
	}

	inf2 := is.newInfo("a2", float64(2), time.Second)
	inf2.PeerID = 1
	if err := is.addInfo(inf2); err != nil {
		t.Fatal(err)
	}
	if is.leastUseful(set) != nodes[0] {
		t.Error("expecting nodes[0] as least useful")
	}

	set.addNode(nodes[1])
	if is.leastUseful(set) != nodes[1] {
		t.Error("expecting nodes[1] as least useful")
	}

	inf3 := is.newInfo("a3", float64(3), time.Second)
	inf3.PeerID = 2
	if err := is.addInfo(inf3); err != nil {
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

func (cr *callbackRecord) Add(key string, contentsChanged bool) {
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

	i1 := is.newInfo("key1", float64(1), time.Second)
	i2 := is.newInfo("key2", float64(1), time.Second)
	i3 := is.newInfo("key3", float64(1), time.Second)

	// Add infos twice and verify callbacks aren't called for same timestamps.
	wg.Add(5)
	for i := 0; i < 2; i++ {
		if err := is.addInfo(i1); err != nil {
			if i == 0 {
				t.Error(err)
			}
		} else {
			if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
		}
		if err := is.addInfo(i2); err != nil {
			if i == 0 {
				t.Error(err)
			}
		} else {
			if i != 0 {
				t.Errorf("expected error on run #%d, but didn't get one", i)
			}
		}
		if err := is.addInfo(i3); err != nil {
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
	i1 = is.newInfo("key1", float64(2), time.Second)
	wg.Add(2)
	if err := is.addInfo(i1); err != nil {
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

	// Register another callback with same pattern and verify it is
	// invoked for all three keys.
	wg.Add(3)
	is.registerCallback("key.*", cbAll.Add)
	wg.Wait()

	expKeys := []string{"key1-true", "key2-true", "key3-true", "key1-true", "key1-true", "key2-true", "key3-true"}
	sort.Strings(expKeys)
	keys = cbAll.Keys()
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

	i1 := is.newInfo("key1", float64(1), time.Second)
	i2 := is.newInfo("key2", float64(1), time.Second)
	if err := is.addInfo(i1); err != nil {
		t.Fatal(err)
	}
	if err := is.addInfo(i2); err != nil {
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

func TestInfoStoreDeltaProtoInfos(t *testing.T) {
	defer leaktest.AfterTest(t)

	is := newInfoStore(1, emptyAddr)

	k1 := "key1"
	i1 := is.newInfo(k1, float64(1), time.Second)
	if err := is.addInfo(i1); err != nil {
		t.Fatal(err)
	}
	d := is.Proto()
	if len(d.Infos) != 1 || d.Infos[k1].Key != k1 {
		t.Error("expected InfoStoreDelta to contain info")
	}
}

func TestInfoStoreDeltaProtoGroups(t *testing.T) {
	defer leaktest.AfterTest(t)

	is := createTestInfoStore(t)
	d := is.Proto()
	if len(d.Groups) != len(is.Groups) {
		t.Errorf("expected matching group length")
	}
	for k, group := range d.Groups {
		if group != &is.Groups[k].G {
			t.Errorf("expected the same Group under key %v", k)
		}
	}
}

func TestInfoStoreFromProto(t *testing.T) {
	defer leaktest.AfterTest(t)

	is := createTestInfoStore(t)
	p := is.Proto()
	is2 := newInfoStoreFromProto(p)

	if len(is.Groups) != len(is2.Groups) {
		t.Errorf("expected matching group length")
	}
}
