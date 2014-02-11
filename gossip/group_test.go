package gossip

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func newInfo(key string, val float64) *Info {
	now := time.Now().UnixNano()
	ttl := now + int64(time.Minute)
	return &Info{key, Float64Value(val), now, ttl, 0, "", 0}
}

// MIN_GROUP type groups and group.shouldInclude() behavior.
func TestMinGroupShouldInclude(t *testing.T) {
	group := NewGroup("a", 2, MIN_GROUP)

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
	group := NewGroup("a", 2, MAX_GROUP)

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
	group := NewGroup("a", 1, MIN_GROUP)
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
	group := NewGroup("a", 2, MIN_GROUP)

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
	minGroup := NewGroup("a", 100, MIN_GROUP)
	infos := insertRandomInfos(minGroup, 1000)

	// Insert same infos into the max group.
	maxGroup := NewGroup("a", 100, MAX_GROUP)
	for _, info := range infos {
		maxGroup.AddInfo(info)
	}
	sort.Sort(infos)

	minInfos := minGroup.Infos()
	sort.Sort(minInfos)

	maxInfos := maxGroup.Infos()
	sort.Sort(maxInfos)

	for i := 0; i < 100; i++ {
		if infos[i].Key != minInfos[i].Key {
			t.Errorf("key %d (%f != %f)", i, infos[i].Val, minInfos[i].Val)
		}
		if infos[1000-100+i].Key != maxInfos[i].Key {
			t.Errorf("key %d (%f != %f)", i, infos[1000-100+i].Val, maxInfos[i].Val)
		}
	}
}

// Verify info selection by key.
func TestGetInfo(t *testing.T) {
	group := NewGroup("a", 10, MIN_GROUP)
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
