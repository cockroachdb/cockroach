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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package gossip

import (
	"fmt"
	"testing"
	"time"
)

// newGroup and newInfo are defined in group_test.go

// Register two groups and verify operation of belongsToGroup.
func TestRegisterGroup(t *testing.T) {
	is := NewInfoStore()

	groupA := newGroup("a", 1, minGroup, t)
	if is.RegisterGroup(groupA) != nil {
		t.Error("could not register group A")
	}
	groupB := newGroup("b", 1, minGroup, t)
	if is.RegisterGroup(groupB) != nil {
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

	// Try to register a group that's already been registered.
	if is.RegisterGroup(groupA) == nil {
		t.Error("should not be able to register group A twice")
	}
}

// Create new info objects. Verify sequence increments.
func TestNewInfo(t *testing.T) {
	is := NewInfoStore()
	info1 := is.NewInfo("a", Float64Value(1), time.Second)
	info2 := is.NewInfo("a", Float64Value(1), time.Second)
	if info1.Seq != info2.Seq-1 {
		t.Errorf("sequence numbers should increment %d, %d", info1.Seq, info2.Seq)
	}
}

// Add an info, make sure it can be fetched via GetInfo.
// Make sure a non-existent info can't be fetched.
func TestInfoStoreGetInfo(t *testing.T) {
	is := NewInfoStore()
	info := is.NewInfo("a", Float64Value(1), time.Second)
	if !is.AddInfo(info) {
		t.Error("unable to add info")
	}
	if is.InfoCount() != 1 {
		t.Errorf("infostore count incorrect %d != 1", is.InfoCount())
	}
	if is.MaxSeq != info.Seq {
		t.Error("max seq value wasn't updated")
	}
	if is.GetInfo("a") != info {
		t.Error("unable to get info")
	}
	if is.GetInfo("b") != nil {
		t.Error("erroneously produced non-existent info for key b")
	}
}

// Verify TTL is respected on info fetched by key
// and group.
func TestInfoStoreGetInfoTTL(t *testing.T) {
	is := NewInfoStore()
	info := is.NewInfo("a", Float64Value(1), time.Nanosecond)
	if !is.AddInfo(info) {
		t.Error("unable to add info")
	}
	time.Sleep(time.Nanosecond)
	if is.GetInfo("a") != nil {
		t.Error("shouldn't be able to get info with short TTL")
	}
}

// Add infos using same key, same and lesser timestamp; verify no
// replacement.
func TestAddInfoSameKeyLessThanEqualTimestamp(t *testing.T) {
	is := NewInfoStore()
	info1 := is.NewInfo("a", Float64Value(1), time.Second)
	if !is.AddInfo(info1) {
		t.Error("unable to add info1")
	}
	info2 := is.NewInfo("a", Float64Value(2), time.Second)
	info2.Timestamp = info1.Timestamp
	if is.AddInfo(info2) {
		t.Error("able to add info2 with same timestamp")
	}
	info2.Timestamp--
	if is.AddInfo(info2) {
		t.Error("able to add info2 with lesser timestamp")
	}
	// Verify info2 did not replace info1.
	if is.GetInfo("a") != info1 {
		t.Error("info1 was replaced, despite same timestamp")
	}
}

// Add infos using same key, same timestamp; verify no replacement.
func TestAddInfoSameKeyGreaterTimestamp(t *testing.T) {
	is := NewInfoStore()
	info1 := is.NewInfo("a", Float64Value(1), time.Second)
	info2 := is.NewInfo("a", Float64Value(2), time.Second)
	if !is.AddInfo(info1) || !is.AddInfo(info2) {
		t.Error("unable to add info1 or info2")
	}
}

// Verify that adding two infos with different hops but same keys
// always chooses the minimum hops.
func TestAddInfoSameKeyDifferentHops(t *testing.T) {
	is := NewInfoStore()
	info1 := is.NewInfo("a", Float64Value(1), time.Second)
	info1.Hops = 1
	info2 := is.NewInfo("a", Float64Value(2), time.Second)
	info2.Hops = 2
	if !is.AddInfo(info1) || !is.AddInfo(info2) {
		t.Error("unable to add info1 or info2")
	}

	info := is.GetInfo("a")
	if info.Hops != info1.Hops || info.Val != info2.Val {
		t.Error("failed to properly combine hops and value", info)
	}

	// Try yet another info, with lower hops yet (0).
	info3 := is.NewInfo("a", Float64Value(3), time.Second)
	if !is.AddInfo(info3) {
		t.Error("unable to add info3")
	}
	info = is.GetInfo("a")
	if info.Hops != info3.Hops || info.Val != info3.Val {
		t.Error("failed to properly combine hops and value", info)
	}
}

// Register groups, add and fetch group infos from min/max groups and
// verify ordering. Add an additional non-group info and fetch that as
// well.
func TestAddGroupInfos(t *testing.T) {
	is := NewInfoStore()

	group := newGroup("a", 10, minGroup, t)
	if is.RegisterGroup(group) != nil {
		t.Error("could not register group")
	}

	info1 := is.NewInfo("a.a", Float64Value(1), time.Second)
	info2 := is.NewInfo("a.b", Float64Value(2), time.Second)
	if !is.AddInfo(info1) || !is.AddInfo(info2) {
		t.Error("unable to add info1 or info2")
	}
	if is.InfoCount() != 2 {
		t.Errorf("infostore count incorrect %d != 2", is.InfoCount())
	}
	if is.MaxSeq != info2.Seq {
		t.Errorf("store max seq info2 seq %d != %d", is.MaxSeq, info2.Seq)
	}

	infos := is.GetGroupInfos("a")
	if infos == nil {
		t.Error("unable to fetch group infos")
	}
	if infos[0].Key != "a.a" || infos[1].Key != "a.b" {
		t.Error("fetch group infos have incorrect order:", infos)
	}

	// Try with a max group.
	maxGroup := newGroup("b", 10, maxGroup, t)
	if is.RegisterGroup(maxGroup) != nil {
		t.Error("could not register group")
	}
	info3 := is.NewInfo("b.a", Float64Value(1), time.Second)
	info4 := is.NewInfo("b.b", Float64Value(2), time.Second)
	if !is.AddInfo(info3) || !is.AddInfo(info4) {
		t.Error("unable to add info1 or info2")
	}
	if is.InfoCount() != 4 {
		t.Errorf("infostore count incorrect %d != 4", is.InfoCount())
	}
	if is.MaxSeq != info4.Seq {
		t.Errorf("store max seq info4 seq %d != %d", is.MaxSeq, info4.Seq)
	}

	infos = is.GetGroupInfos("b")
	if infos == nil {
		t.Error("unable to fetch group infos")
	}
	if infos[0].Key != "b.b" || infos[1].Key != "b.a" {
		t.Error("fetch group infos have incorrect order:", infos)
	}

	// Finally, add a non-group info and verify it cannot be fetched
	// by group, but can be fetched solo.
	info5 := is.NewInfo("c.a", Float64Value(3), time.Second)
	if !is.AddInfo(info5) {
		t.Error("unable to add info5")
	}
	if is.GetGroupInfos("c") != nil {
		t.Error("shouldn't be able to fetch non-existent group c")
	}
	if is.GetInfo("c.a") != info5 {
		t.Error("unable to fetch info5 by key")
	}
	if is.InfoCount() != 5 {
		t.Errorf("infostore count incorrect %d != 5", is.InfoCount())
	}
	if is.MaxSeq != info5.Seq {
		t.Errorf("store max seq info5 seq %d != %d", is.MaxSeq, info5.Seq)
	}
}

// Verify infostore combination with overlapping group and non-group
// infos.
func TestCombine(t *testing.T) {
	is1 := NewInfoStore()

	group1 := newGroup("a", 10, minGroup, t)
	group1Overlap := newGroup("b", 10, minGroup, t)
	if is1.RegisterGroup(group1) != nil || is1.RegisterGroup(group1Overlap) != nil {
		t.Error("could not register group1 or group1Overlap")
	}

	info1a := is1.NewInfo("a.a", Float64Value(1), time.Second)
	info1b := is1.NewInfo("a.b", Float64Value(2), time.Second)
	info1c := is1.NewInfo("a", Float64Value(3), time.Second) // non-group info
	if !is1.AddInfo(info1a) || !is1.AddInfo(info1b) || !is1.AddInfo(info1c) {
		t.Error("unable to add infos")
	}
	info1Overlap := is1.NewInfo("b.a", Float64Value(3), time.Second)
	if !is1.AddInfo(info1Overlap) {
		t.Error("unable to add info1Overlap")
	}

	is2 := NewInfoStore()

	group2 := newGroup("c", 10, minGroup, t)
	group2Overlap := newGroup("b", 10, minGroup, t)
	if is2.RegisterGroup(group2) != nil || is2.RegisterGroup(group2Overlap) != nil {
		t.Error("could not register group2 or group2Overlap")
	}

	info2a := is2.NewInfo("c.a", Float64Value(1), time.Second)
	info2b := is2.NewInfo("c.b", Float64Value(2), time.Second)
	info2c := is2.NewInfo("c", Float64Value(3), time.Second)
	if !is2.AddInfo(info2a) || !is2.AddInfo(info2b) || !is2.AddInfo(info2c) {
		t.Error("unable to add infos")
	}
	info2Overlap := is2.NewInfo("b.a", Float64Value(4), time.Second)
	if !is2.AddInfo(info2Overlap) {
		t.Error("unable to add info2Overlap")
	}

	if err := is1.Combine(is2); err != nil {
		t.Error(err)
	}

	infosA := is1.GetGroupInfos("a")
	if len(infosA) != 2 || infosA[0].Key != "a.a" || infosA[1].Key != "a.b" {
		t.Error("group a missing", infosA)
	}

	infosB := is1.GetGroupInfos("b")
	if len(infosB) != 1 || infosB[0].Key != "b.a" || infosB[0].Val != info2Overlap.Val {
		t.Error("group b missing", infosB)
	}

	infosC := is1.GetGroupInfos("c")
	if len(infosC) != 2 || infosC[0].Key != "c.a" || infosC[1].Key != "c.b" {
		t.Error("group c missing", infosC)
	}

	if is1.GetInfo("a") == nil {
		t.Error("non-group a info missing")
	}
	if is1.GetInfo("c") == nil {
		t.Error("non-group c info missing")
	}
}

// Helper method creates an infostore with two groups with 10
// infos each and 10 non-group infos.
func createTestInfoStore(t *testing.T) *InfoStore {
	is := NewInfoStore()

	groupA := newGroup("a", 10, minGroup, t)
	groupB := newGroup("b", 10, minGroup, t)
	if is.RegisterGroup(groupA) != nil || is.RegisterGroup(groupB) != nil {
		t.Error("unable to register groups")
	}

	// Insert 10 keys each for groupA, groupB and non-group successively.
	for i := 0; i < 10; i++ {
		infoA := is.NewInfo(fmt.Sprintf("a.%d", i), Float64Value(i), time.Second)
		is.AddInfo(infoA)

		infoB := is.NewInfo(fmt.Sprintf("b.%d", i), Float64Value(i+1), time.Second)
		is.AddInfo(infoB)

		infoC := is.NewInfo(fmt.Sprintf("c.%d", i), Float64Value(i+2), time.Second)
		is.AddInfo(infoC)
	}

	return is
}

// Check infostore delta (both group and non-group infos) based on
// info sequence numbers.
func TestInfoStoreDelta(t *testing.T) {
	is := createTestInfoStore(t)

	// Verify deltas with successive sequence numbers.
	for i := 0; i < 10; i++ {
		delta, err := is.Delta(int64(i * 3))
		if err != nil {
			t.Errorf("delta failed at sequence number %d: %s", i, err)
		}
		infosA := delta.GetGroupInfos("a")
		infosB := delta.GetGroupInfos("b")
		if len(infosA) != 10-i || len(infosB) != 10-i {
			t.Errorf("expected %d infos, not %d, %d", 10-i, len(infosA), len(infosB))
		}
		for j := 0; j < 10-i; j++ {
			expAKey := fmt.Sprintf("a.%d", j+i)
			expBKey := fmt.Sprintf("b.%d", j+i)
			if infosA[j].Key != expAKey || infosB[j].Key != expBKey {
				t.Errorf("run %d: key mismatch at index %d: %s != %s, %s != %s",
					i, j, infosA[j].Key, expAKey, infosB[j].Key, expBKey)
			}

			infoC := delta.GetInfo(fmt.Sprintf("c.%d", j+i))
			if infoC == nil {
				t.Errorf("unable to fetch non-group info %d", j+i)
			}
			if i > 0 {
				infoC = delta.GetInfo(fmt.Sprintf("c.%d", 0))
				if infoC != nil {
					t.Errorf("erroneously fetched non-group info %d", j+i+1)
				}
			}
		}
	}

	if _, err := is.Delta(int64(30)); err == nil {
		t.Error("fetching delta of infostore at maximum sequence number should return error")
	}
}

// Build a filter representing the info store and verify
// keys are represented.
func TestBuildFilter(t *testing.T) {
	is := createTestInfoStore(t)
	f, err := is.BuildFilter(1)
	if err != nil {
		t.Error("unable to build filter", err)
	}

	for i := 0; i < 10; i++ {
		if !f.hasKey(fmt.Sprintf("a.%d", i)) {
			t.Error("filter should contain key a.", i)
		}
		if !f.hasKey(fmt.Sprintf("b.%d", i)) {
			t.Error("filter should contain key b.", i)
		}
		if !f.hasKey(fmt.Sprintf("c.%d", i)) {
			t.Error("filter should contain key c.", i)
		}
	}

	// Verify non-keys are not present.
	if f.hasKey("d.1") || f.hasKey("d.2") {
		t.Error("filter should not contain d.1 or d.2")
	}
}

// Build a filter where maximum hops matter. Make
// sure keys with too-high hops are not present.
func TestFilterMaxHops(t *testing.T) {
	is := NewInfoStore()

	group := newGroup("a", 10, minGroup, t)
	if is.RegisterGroup(group) != nil {
		t.Error("unable to register group")
	}

	// Insert 2 keys each for group and non-group respectively.
	// First key get 0 hops, second 1 hop.
	gInfo1 := is.NewInfo("a.1", Float64Value(1), time.Second)
	is.AddInfo(gInfo1)
	gInfo2 := is.NewInfo("a.2", Float64Value(2), time.Second)
	gInfo2.Hops = 2
	is.AddInfo(gInfo2)

	info1 := is.NewInfo("b.1", Float64Value(1), time.Second)
	is.AddInfo(info1)
	info2 := is.NewInfo("b.2", Float64Value(2), time.Second)
	info2.Hops = 2
	is.AddInfo(info2)

	f, err := is.BuildFilter(1) // max hops set to 1
	if err != nil {
		t.Error("unable to build filter", err)
	}

	if !f.hasKey("a.1") || !f.hasKey("b.1") {
		t.Error("filter should have low-hops keys for a and b")
	}
	if f.hasKey("a.2") || f.hasKey("b.2") {
		t.Error("filter shouldn't have high-hops keys for a and b")
	}
}
