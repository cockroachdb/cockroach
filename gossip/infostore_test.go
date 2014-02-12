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

// Register groups, add and fetch group infos from min/max groups and
// verify ordering. Add an additional non-group info and fetch that as
// well.
func TestAddGroupInfos(t *testing.T) {
	is := NewInfoStore()

	group := newGroup("a", 10, MIN_GROUP, t)
	if is.RegisterGroup(group) != nil {
		t.Error("could not register group")
	}

	info1 := is.NewInfo("a.a", Float64Value(1), time.Second)
	info2 := is.NewInfo("a.b", Float64Value(2), time.Second)
	if !is.AddInfo(info1) || !is.AddInfo(info2) {
		t.Error("unable to add info1 or info2")
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
	maxGroup := newGroup("b", 10, MAX_GROUP, t)
	if is.RegisterGroup(maxGroup) != nil {
		t.Error("could not register group")
	}
	info3 := is.NewInfo("b.a", Float64Value(1), time.Second)
	info4 := is.NewInfo("b.b", Float64Value(2), time.Second)
	if !is.AddInfo(info3) || !is.AddInfo(info4) {
		t.Error("unable to add info1 or info2")
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
	if is.MaxSeq != info5.Seq {
		t.Errorf("store max seq info5 seq %d != %d", is.MaxSeq, info5.Seq)
	}
}

// Verify infostore combination with overlapping group and non-group
// infos.
func TestCombine(t *testing.T) {
	is1 := NewInfoStore()

	group1 := newGroup("a", 10, MIN_GROUP, t)
	group1Overlap := newGroup("b", 10, MIN_GROUP, t)
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

	group2 := newGroup("c", 10, MIN_GROUP, t)
	group2Overlap := newGroup("b", 10, MIN_GROUP, t)
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
