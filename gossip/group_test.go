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
	"math/rand"
	"sort"
	"testing"
	"time"
)

func newInfo(key string, val float64) *Info {
	now := monotonicUnixNano()
	ttl := now + int64(time.Minute)
	return &Info{
		Key:       key,
		Val:       Float64Value(val),
		Timestamp: now,
		TTLStamp:  ttl,
	}
}

func newGroup(prefix string, limit int, typeOf GroupType, t *testing.T) *Group {
	group, err := NewGroup(prefix, limit, typeOf)
	if err != nil {
		t.Fatal(err)
	}
	return group
}

// TestNewGroup verifies NewGroup behavior.
func TestNewGroup(t *testing.T) {
	if _, err := NewGroup("a", 0, MinGroup); err == nil {
		t.Error("new group with limit=0 should be illegal")
	}
	if _, err := NewGroup("a", -1, MinGroup); err == nil {
		t.Error("new group with limit=-1 should be illegal")
	}
}

// TestMinGroupShouldInclude tests MinGroup type groups
// and group.shouldInclude() behavior.
func TestMinGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MinGroup, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.addInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.addInfo(info2) {
		t.Error("could not insert")
	}

	// A smaller insert should include fine.
	info3 := newInfo("a.c", 0)
	if !group.shouldInclude(info3) || !group.addInfo(info3) {
		t.Error("could not insert")
	}

	// A larger insert shouldn't include.
	info4 := newInfo("a.d", 3)
	if group.shouldInclude(info4) || group.addInfo(info4) {
		t.Error("shouldn't have been able to insert")
	}
}

// TestMaxGroupShouldInclude tests MaxGroup type groups and
// group.shouldInclude() behavior.
func TestMaxGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MaxGroup, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.addInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.addInfo(info2) {
		t.Error("could not insert")
	}

	// A larger insert should include fine.
	info3 := newInfo("a.c", 3)
	if !group.shouldInclude(info3) || !group.addInfo(info3) {
		t.Error("could not insert")
	}

	// A smaller insert shouldn't include.
	info4 := newInfo("a.d", 0)
	if group.shouldInclude(info4) || group.addInfo(info4) {
		t.Error("shouldn't have been able to insert")
	}
}

// TestSameKeyInserts inserts the same key into group and verifies
// earlier timestamps are ignored and later timestamps always replace it.
func TestSameKeyInserts(t *testing.T) {
	group := newGroup("a", 1, MinGroup, t)
	info1 := newInfo("a.a", 1)
	if !group.addInfo(info1) {
		t.Error("could not insert")
	}

	// Smaller timestamp should be ignored.
	info2 := newInfo("a.a", 1)
	info2.Timestamp = info1.Timestamp - 1
	if group.addInfo(info2) {
		t.Error("should not allow insert")
	}

	// Two successively larger timestamps always win.
	info3 := newInfo("a.a", 1)
	info3.Timestamp = info1.Timestamp + 1
	if !group.addInfo(info3) {
		t.Error("could not insert")
	}
	info4 := newInfo("a.a", 1)
	info4.Timestamp = info1.Timestamp + 2
	if !group.addInfo(info4) {
		t.Error("could not insert")
	}
}

// TestGroupCompactAfterTTL verifies group compaction after TTL by
// waiting and verifying a full group can be inserted into again.
func TestGroupCompactAfterTTL(t *testing.T) {
	group := newGroup("a", 2, MinGroup, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	info1.TTLStamp = info1.Timestamp + int64(time.Millisecond)
	if !group.addInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	info2.TTLStamp = info2.Timestamp + int64(time.Millisecond)
	if !group.addInfo(info2) {
		t.Error("could not insert")
	}

	// A larger insert shouldn't yet insert as we haven't surprassed TTL.
	info3 := newInfo("a.c", 3)
	if group.addInfo(info3) {
		t.Error("shouldn't be able to insert")
	}

	// Now, wait a millisecond and try again.
	time.Sleep(time.Millisecond)
	if !group.addInfo(info3) {
		t.Error("could not insert")
	}

	// Next value should also insert.
	info4 := newInfo("a.d", 4)
	if !group.addInfo(info4) {
		t.Error("could not insert")
	}
}

// insertRandomInfos inserts random values into group and returns
// a slice of info objects.
func insertRandomInfos(group *Group, count int) InfoArray {
	infos := make(InfoArray, count)

	for i := 0; i < count; i++ {
		infos[i] = newInfo(fmt.Sprintf("a.%d", i), rand.Float64())
		group.addInfo(infos[i])
	}

	return infos
}

// TestGroups100Keys verifies behavior of MinGroup and MaxGroup with a
// limit of 100 keys after inserting 1000.
func TestGroups100Keys(t *testing.T) {
	// Start by adding random infos to min group.
	minGroup := newGroup("a", 100, MinGroup, t)
	infos := insertRandomInfos(minGroup, 1000)

	// Insert same infos into the max group.
	maxGroup := newGroup("a", 100, MaxGroup, t)
	for _, info := range infos {
		maxGroup.addInfo(info)
	}
	sort.Sort(infos)

	minInfos := minGroup.infosAsArray()
	sort.Sort(minInfos)

	maxInfos := maxGroup.infosAsArray()
	sort.Sort(maxInfos)

	for i := 0; i < 100; i++ {
		if infos[i].Key != minInfos[i].Key {
			t.Errorf("key %d (%s != %s)", i, infos[i].Key, minInfos[i].Key)
		}
		if infos[1000-100+i].Key != maxInfos[i].Key {
			t.Errorf("key %d (%s != %s)", i, infos[1000-100+i].Key, maxInfos[i].Key)
		}
	}
}

// TestSameKeySameTimestamp verifies that adding two infos with identical
// timestamps don't update group. This is a common occurence when updating
// gossip info from multiple independent peers passing along overlapping
// information. We don't want each new update with overlap to generate
// unnecessary delta info.
func TestSameKeySameTimestamp(t *testing.T) {
	group := newGroup("a", 2, MinGroup, t)
	info1 := newInfo("a.a", 1.0)
	info2 := newInfo("a.a", 1.0)
	info2.Timestamp = info1.Timestamp
	if !group.addInfo(info1) {
		t.Error("failed first insert")
	}
	if group.addInfo(info2) {
		t.Error("second insert with identical key & timestamp should have failed")
	}
}

// TestSameKeyDifferentHops verifies that adding two infos with the
// same key and different Hops values preserves the lower Hops count.
func TestSameKeyDifferentHops(t *testing.T) {
	info1 := newInfo("a.a", 1.0)
	info2 := newInfo("a.a", 1.0)
	info1.Hops = 1
	info2.Hops = 2

	// Add info1 first, then info2.
	group1 := newGroup("a", 1, MinGroup, t)
	if !group1.addInfo(info1) || !group1.addInfo(info2) {
		t.Error("failed insertions", info1, info2)
	}
	if info := group1.getInfo("a.a"); info == nil || info.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", info)
	}

	// Add info1 first, then info2.
	group2 := newGroup("a", 1, MinGroup, t)
	if !group2.addInfo(info1) || !group2.addInfo(info2) {
		t.Error("failed insertions")
	}
	if info := group2.getInfo("a.a"); info == nil || info.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", info)
	}
}

// TestGroupGetInfo verifies info selection by key.
func TestGroupGetInfo(t *testing.T) {
	group := newGroup("a", 10, MinGroup, t)
	infos := insertRandomInfos(group, 10)
	for _, info := range infos {
		if info != group.getInfo(info.Key) {
			t.Error("could not fetch info", info)
		}
	}

	// Test non-existent key.
	if group.getInfo("b.a") != nil {
		t.Error("fetched something for non-existing key \"b.a\"")
	}
}

// TestGroupGetInfoTTL verifies GetInfo with a short TTL.
func TestGroupGetInfoTTL(t *testing.T) {
	group := newGroup("a", 10, MinGroup, t)
	info := newInfo("a.a", 1)
	info.TTLStamp = info.Timestamp + int64(time.Nanosecond)
	group.addInfo(info)
	time.Sleep(time.Nanosecond)
	if group.getInfo(info.Key) != nil {
		t.Error("shouldn't have been able to fetch key with short TTL")
	}

	// Try 2 infos, one with short TTL and one with long TTL and
	// verify operation of infosAsArray.
	info1 := newInfo("a.1", 1)
	info2 := newInfo("a.2", 2)
	info2.TTLStamp = info.Timestamp + int64(time.Nanosecond)
	group.addInfo(info1)
	group.addInfo(info2)

	time.Sleep(time.Nanosecond)
	infos := group.infosAsArray()
	if len(infos) != 1 || infos[0].Val != info1.Val {
		t.Error("only one info should be returned", infos)
	}
}
