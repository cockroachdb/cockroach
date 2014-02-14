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
	now := MonotonicUnixNano()
	ttl := now + int64(time.Minute)
	return &Info{key, Float64Value(val), now, ttl, 0, "", 0}
}

func newGroup(prefix string, limit int, typeOf GroupType, t *testing.T) *Group {
	group, err := NewGroup(prefix, limit, typeOf)
	if err != nil {
		t.Fatal(err)
	}
	return group
}

// Verify NewGroup behavior.
func TestNewGroup(t *testing.T) {
	if _, err := NewGroup("a", 0, MIN_GROUP); err == nil {
		t.Error("new group with limit=0 should be illegal")
	}
	if _, err := NewGroup("a", -1, MIN_GROUP); err == nil {
		t.Error("new group with limit=-1 should be illegal")
	}
}

// MIN_GROUP type groups and group.shouldInclude() behavior.
func TestMinGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MIN_GROUP, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.AddInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.AddInfo(info2) {
		t.Error("could not insert")
	}

	// A smaller insert should include fine.
	info3 := newInfo("a.c", 0)
	if !group.shouldInclude(info3) || !group.AddInfo(info3) {
		t.Error("could not insert")
	}

	// A larger insert shouldn't include.
	info4 := newInfo("a.d", 3)
	if group.shouldInclude(info4) || group.AddInfo(info4) {
		t.Error("shouldn't have been able to insert")
	}
}

// MAX_GROUP type groups and group.shouldInclude() behavior.
func TestMaxGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MAX_GROUP, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.AddInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.AddInfo(info2) {
		t.Error("could not insert")
	}

	// A larger insert should include fine.
	info3 := newInfo("a.c", 3)
	if !group.shouldInclude(info3) || !group.AddInfo(info3) {
		t.Error("could not insert")
	}

	// A smaller insert shouldn't include.
	info4 := newInfo("a.d", 0)
	if group.shouldInclude(info4) || group.AddInfo(info4) {
		t.Error("shouldn't have been able to insert")
	}
}

// Insert same key into group and verify earlier timestamps are
// ignored and later timestamps always replace.
func TestSameKeyInserts(t *testing.T) {
	group := newGroup("a", 1, MIN_GROUP, t)
	info1 := newInfo("a.a", 1)
	if !group.AddInfo(info1) {
		t.Error("could not insert")
	}

	// Smaller timestamp should be ignored.
	info2 := newInfo("a.a", 1)
	info2.Timestamp = info1.Timestamp - 1
	if group.AddInfo(info2) {
		t.Error("should not allow insert")
	}

	// Two successively larger timestamps always win.
	info3 := newInfo("a.a", 1)
	info3.Timestamp = info1.Timestamp + 1
	if !group.AddInfo(info3) {
		t.Error("could not insert")
	}
	info4 := newInfo("a.a", 1)
	info4.Timestamp = info1.Timestamp + 2
	if !group.AddInfo(info4) {
		t.Error("could not insert")
	}
}

// Verify group compaction after TTL by waiting and verifying a full
// group can be inserted into again.
func TestGroupCompactAfterTTL(t *testing.T) {
	group := newGroup("a", 2, MIN_GROUP, t)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	info1.TTLStamp = info1.Timestamp + int64(time.Millisecond)
	if !group.AddInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	info2.TTLStamp = info2.Timestamp + int64(time.Millisecond)
	if !group.AddInfo(info2) {
		t.Error("could not insert")
	}

	// A larger insert shouldn't yet insert as we haven't surprassed TTL.
	info3 := newInfo("a.c", 3)
	if group.AddInfo(info3) {
		t.Error("shouldn't be able to insert")
	}

	// Now, wait a millisecond and try again.
	time.Sleep(time.Millisecond)
	if !group.AddInfo(info3) {
		t.Error("could not insert")
	}

	// Next value should also insert.
	info4 := newInfo("a.d", 4)
	if !group.AddInfo(info4) {
		t.Error("could not insert")
	}
}

// Insert random values into group; returns array of info objects.
func insertRandomInfos(group *Group, count int) InfoArray {
	infos := make(InfoArray, count)

	for i := 0; i < count; i++ {
		infos[i] = newInfo(fmt.Sprintf("a.%d", i), rand.Float64())
		group.AddInfo(infos[i])
	}

	return infos
}

// Verify behavior of MIN_GROUP and MAX_GROUP with limit of 100 keys
// after inserting 1000.
func TestGroups100Keys(t *testing.T) {
	// Start by adding random infos to min group.
	minGroup := newGroup("a", 100, MIN_GROUP, t)
	infos := insertRandomInfos(minGroup, 1000)

	// Insert same infos into the max group.
	maxGroup := newGroup("a", 100, MAX_GROUP, t)
	for _, info := range infos {
		maxGroup.AddInfo(info)
	}
	sort.Sort(infos)

	minInfos := minGroup.InfosAsArray()
	sort.Sort(minInfos)

	maxInfos := maxGroup.InfosAsArray()
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

// Verify that adding two infos with identical timestamps don't update
// group. This is a common occurence when updating gossip info from
// multiple independent peers passing along overlapping information.
// We don't want each new update with overlap to generate unnecessary
// delta info.
func TestSameKeySameTimestamp(t *testing.T) {
	group := newGroup("a", 2, MIN_GROUP, t)
	info1 := newInfo("a.a", 1.0)
	info2 := newInfo("a.a", 1.0)
	info2.Timestamp = info1.Timestamp
	if !group.AddInfo(info1) {
		t.Error("failed first insert")
	}
	if group.AddInfo(info2) {
		t.Error("second insert with identical key & timestamp should have failed")
	}
}

// Verify that adding two infos with the same key and different Hops
// values preserves the lower Hops count.
func TestSameKeyDifferentHops(t *testing.T) {
	info1 := newInfo("a.a", 1.0)
	info2 := newInfo("a.a", 1.0)
	info1.Hops = 1
	info2.Hops = 2

	// Add info1 first, then info2.
	group1 := newGroup("a", 1, MIN_GROUP, t)
	if !group1.AddInfo(info1) || !group1.AddInfo(info2) {
		t.Error("failed insertions", info1, info2)
	}
	if info := group1.GetInfo("a.a"); info == nil || info.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", info)
	}

	// Add info1 first, then info2.
	group2 := newGroup("a", 1, MIN_GROUP, t)
	if !group2.AddInfo(info1) || !group2.AddInfo(info2) {
		t.Error("failed insertions")
	}
	if info := group2.GetInfo("a.a"); info == nil || info.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", info)
	}
}

// Verify info selection by key.
func TestGroupGetInfo(t *testing.T) {
	group := newGroup("a", 10, MIN_GROUP, t)
	infos := insertRandomInfos(group, 10)
	for _, info := range infos {
		if info != group.GetInfo(info.Key) {
			t.Error("could not fetch info", info)
		}
	}

	// Test non-existent key.
	if group.GetInfo("b.a") != nil {
		t.Error("fetched something for non-existing key \"b.a\"")
	}
}

// Check delta groups based on info sequence numbers.
func TestGroupDelta(t *testing.T) {
	group := newGroup("a", 10, MIN_GROUP, t)

	// Insert keys with sequence numbers 1..10.
	for i := 0; i < 10; i++ {
		info := newInfo(fmt.Sprintf("a.%d", i), float64(i))
		info.Seq = int64(i + 1)
		group.AddInfo(info)
	}

	// Verify deltas with successive sequence numbers.
	for i := 0; i < 10; i++ {
		delta, err := group.Delta(int64(i))
		if err != nil {
			t.Error("delta failed at sequence number", i)
		}
		infos := delta.InfosAsArray()
		if len(infos) != 10-i {
			t.Errorf("expected %d infos, not %d", 10-i, len(infos))
		}
		sort.Sort(infos)
		for j := 0; j < 10-i; j++ {
			expKey := fmt.Sprintf("a.%d", j+i)
			if infos[j].Key != expKey {
				t.Errorf("run %d: key mismatch at index %d: %s != %s", i, j, infos[j].Key, expKey)
			}
		}
	}

	if _, err := group.Delta(int64(10)); err == nil {
		t.Error("fetching delta of group at maximum sequence number should return error")
	}
}
