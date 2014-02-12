package gossip

import (
	"testing"
	"time"
)

// newGroup and newInfo are defined in group_test.go

// Register two groups and verify operation of belongsToGroup.
func TestRegisterGroup(t *testing.T) {
	is := NewInfoStore()

	groupA := newGroup("a", 1, MIN_GROUP, t)
	if is.RegisterGroup(groupA) != nil {
		t.Error("could not register group A")
	}
	groupB := newGroup("b", 1, MIN_GROUP, t)
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
