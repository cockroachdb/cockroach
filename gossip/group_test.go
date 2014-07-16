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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

func newTestInfo(key string, val interface{}) *info {
	now := monotonicUnixNano()
	ttl := now + int64(time.Minute)
	return &info{
		Key:       key,
		Val:       val,
		Timestamp: now,
		TTLStamp:  ttl,
	}
}

// TestMinGroupShouldInclude tests MinGroup type groups
// and group.shouldInclude() behavior.
func TestMinGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MinGroup)

	// First two inserts work fine.
	info1 := newTestInfo("a.a", int64(1))
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}
	info2 := newTestInfo("a.b", int64(2))
	if err := group.addInfo(info2); err != nil {
		t.Error(err)
	}

	// A smaller insert should include fine.
	info3 := newTestInfo("a.c", int64(0))
	if !group.shouldInclude(info3) || group.addInfo(info3) != nil {
		t.Error("could not insert")
	}

	// A larger insert shouldn't include.
	info4 := newTestInfo("a.d", int64(3))
	if group.shouldInclude(info4) || group.addInfo(info4) == nil {
		t.Error("shouldn't have been able to insert")
	}
}

// TestMaxGroupShouldInclude tests MaxGroup type groups and
// group.shouldInclude() behavior.
func TestMaxGroupShouldInclude(t *testing.T) {
	group := newGroup("a", 2, MaxGroup)

	// First two inserts work fine.
	info1 := newTestInfo("a.a", int64(1))
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}
	info2 := newTestInfo("a.b", int64(2))
	if err := group.addInfo(info2); err != nil {
		t.Error(err)
	}

	// A larger insert should include fine.
	info3 := newTestInfo("a.c", int64(3))
	if !group.shouldInclude(info3) || group.addInfo(info3) != nil {
		t.Errorf("could not insert")
	}

	// A smaller insert shouldn't include.
	info4 := newTestInfo("a.d", int64(0))
	if group.shouldInclude(info4) || group.addInfo(info4) == nil {
		t.Error("shouldn't have been able to insert")
	}
}

// TestTypeMismatch inserts two infos of different types into a group
// and verifies error response.
func TestTypeMismatch(t *testing.T) {
	group := newGroup("a", 1, MinGroup)
	info1 := newTestInfo("a.a", int64(1))
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}
	info2 := &info{
		Key:       "a.b",
		Val:       "foo",
		Timestamp: info1.Timestamp,
		TTLStamp:  info1.TTLStamp,
	}
	if err := group.addInfo(info2); err == nil {
		t.Error("expected error inserting string info into float64 group")
	}
}

// TestSameKeyInserts inserts the same key into group and verifies
// earlier timestamps are ignored and later timestamps always replace it.
func TestSameKeyInserts(t *testing.T) {
	group := newGroup("a", 1, MinGroup)
	info1 := newTestInfo("a.a", int64(1))
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}

	// Smaller timestamp should be ignored.
	info2 := newTestInfo("a.a", int64(1))
	info2.Timestamp = info1.Timestamp - 1
	if err := group.addInfo(info2); err == nil {
		t.Error("should not allow insert")
	}

	// Two successively larger timestamps always win.
	info3 := newTestInfo("a.a", int64(1))
	info3.Timestamp = info1.Timestamp + 1
	if err := group.addInfo(info3); err != nil {
		t.Error(err)
	}
	info4 := newTestInfo("a.a", int64(1))
	info4.Timestamp = info1.Timestamp + 2
	if err := group.addInfo(info4); err != nil {
		t.Error(err)
	}
}

// TestGroupCompactAfterTTL verifies group compaction after TTL by
// waiting and verifying a full group can be inserted into again.
func TestGroupCompactAfterTTL(t *testing.T) {
	group := newGroup("a", 2, MinGroup)

	// First two inserts work fine.
	info1 := newTestInfo("a.a", int64(1))
	info1.TTLStamp = info1.Timestamp + int64(time.Millisecond)
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}
	info2 := newTestInfo("a.b", int64(2))
	info2.TTLStamp = info2.Timestamp + int64(time.Millisecond)
	if err := group.addInfo(info2); err != nil {
		t.Error(err)
	}

	// A larger insert shouldn't yet insert as we haven't surprassed TTL.
	info3 := newTestInfo("a.c", int64(3))
	if err := group.addInfo(info3); err == nil {
		t.Error("shouldn't be able to insert")
	}

	// Now, wait a millisecond and try again.
	time.Sleep(time.Millisecond)
	if err := group.addInfo(info3); err != nil {
		t.Error(err)
	}

	// Next value should also insert.
	info4 := newTestInfo("a.d", int64(4))
	if err := group.addInfo(info4); err != nil {
		t.Error(err)
	}
}

// insertRandomInfos inserts random values into group and returns
// a slice of info objects.
func insertRandomInfos(g *group, count int) infoSlice {
	infos := make(infoSlice, count)

	for i := 0; i < count; i++ {
		infos[i] = newTestInfo(fmt.Sprintf("a.%d", i), rand.Float64())
		g.addInfo(infos[i])
	}

	return infos
}

// TestGroups100Keys verifies behavior of MinGroup and MaxGroup with a
// limit of 100 keys after inserting 1000.
func TestGroups100Keys(t *testing.T) {
	// Start by adding random infos to min group.
	minGroup := newGroup("a", 100, MinGroup)
	infos := insertRandomInfos(minGroup, 1000)

	// Insert same infos into the max group.
	maxGroup := newGroup("a", 100, MaxGroup)
	for _, i := range infos {
		maxGroup.addInfo(i)
	}
	sort.Sort(infos)

	minInfos := minGroup.infosAsSlice()
	sort.Sort(minInfos)

	maxInfos := maxGroup.infosAsSlice()
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
	group := newGroup("a", 2, MinGroup)
	info1 := newTestInfo("a.a", float64(1.0))
	info2 := newTestInfo("a.a", float64(1.0))
	info2.Timestamp = info1.Timestamp
	if err := group.addInfo(info1); err != nil {
		t.Error(err)
	}
	if err := group.addInfo(info2); err == nil {
		t.Error("second insert with identical key & timestamp should have failed")
	}
}

// TestSameKeyDifferentHops verifies that adding two infos with the
// same key and different Hops values preserves the lower Hops count.
func TestSameKeyDifferentHops(t *testing.T) {
	info1 := newTestInfo("a.a", float64(1.0))
	info2 := newTestInfo("a.a", float64(1.0))
	info1.Hops = 1
	info2.Timestamp = info1.Timestamp
	info2.Hops = 2

	// Add info1 first, then info2.
	group1 := newGroup("a", 1, MinGroup)
	if err := group1.addInfo(info1); err != nil {
		t.Errorf("failed insert: %s", err)
	}
	if err := group1.addInfo(info2); err == nil {
		t.Errorf("shouldn't have inserted info 2: %s", err)
	}

	if i := group1.getInfo("a.a"); i == nil || i.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", i)
	}

	// Add info2 first, then info1.
	group2 := newGroup("a", 1, MinGroup)
	if err1, err2 := group2.addInfo(info2), group2.addInfo(info1); err1 != nil || err2 != nil {
		t.Errorf("failed insertions: %s, %s", err1, err2)
	}
	if i := group2.getInfo("a.a"); i == nil || i.Hops != 1 {
		t.Error("info nil or info.Hops != 1:", i)
	}
}

// TestGroupGetInfo verifies info selection by key.
func TestGroupGetInfo(t *testing.T) {
	g := newGroup("a", 10, MinGroup)
	infos := insertRandomInfos(g, 10)
	for _, i := range infos {
		if i != g.getInfo(i.Key) {
			t.Error("could not fetch info", i)
		}
	}

	// Test non-existent key.
	if g.getInfo("b.a") != nil {
		t.Error("fetched something for non-existing key \"b.a\"")
	}
}

// TestGroupGetInfoTTL verifies GetInfo with a short TTL.
func TestGroupGetInfoTTL(t *testing.T) {
	g := newGroup("a", 10, MinGroup)
	i := newTestInfo("a.a", int64(1))
	i.TTLStamp = i.Timestamp + int64(time.Nanosecond)
	g.addInfo(i)
	time.Sleep(time.Nanosecond)
	if g.getInfo(i.Key) != nil {
		t.Error("shouldn't have been able to fetch key with short TTL")
	}

	// Try 2 infos, one with short TTL and one with long TTL and
	// verify operation of infosAsSlice.
	info1 := newTestInfo("a.1", int64(1))
	info2 := newTestInfo("a.2", int64(2))
	info2.TTLStamp = i.Timestamp + int64(time.Nanosecond)
	g.addInfo(info1)
	g.addInfo(info2)

	time.Sleep(time.Nanosecond)
	infos := g.infosAsSlice()
	if len(infos) != 1 || infos[0].Val != info1.Val {
		t.Error("only one info should be returned", infos)
	}
}

type testValue struct {
	intVal    int64
	stringVal string
}

func (t *testValue) Less(o util.Ordered) bool {
	return t.intVal < o.(*testValue).intVal
}

// TestGroupWithStructVal verifies group operation with a value which
// is not a basic supported type.
func TestGroupWithStructVal(t *testing.T) {
	g := newGroup("a", 10, MinGroup)
	i1 := newTestInfo("a.a", &testValue{3, "a"})
	i2 := newTestInfo("a.b", &testValue{1, "b"})
	i3 := newTestInfo("a.c", &testValue{2, "c"})
	g.addInfo(i1)
	g.addInfo(i2)
	g.addInfo(i3)

	infos := g.infosAsSlice()
	if infos[0].Val != i2.Val || infos[1].Val != i3.Val || infos[2].Val != i1.Val {
		t.Error("Ordered interface not working properly with groups")
	}
}
