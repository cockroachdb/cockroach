package gossip

import (
	"testing"
	"time"
)

func newInfo(key string, val float64) *Info {
	now := time.Now().UnixNano()
	ttl := now + int64(time.Minute)
	return &Info{key, Float64Value(val), now, ttl, 0, "", 0}
}

func TestMinGroupShouldInclude(t *testing.T) {
	group := NewGroup("a", 2, MIN_GROUP)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.shouldInclude(info1) || !group.AddInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.shouldInclude(info2) || !group.AddInfo(info2) {
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

func TestMaxGroupShouldInclude(t *testing.T) {
	group := NewGroup("a", 2, MAX_GROUP)

	// First two inserts work fine.
	info1 := newInfo("a.a", 1)
	if !group.shouldInclude(info1) || !group.AddInfo(info1) {
		t.Error("could not insert")
	}
	info2 := newInfo("a.b", 2)
	if !group.shouldInclude(info2) || !group.AddInfo(info2) {
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
