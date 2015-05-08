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
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// GroupType indicates the bounds of the values encountered within the group.
type GroupType int

const (
	// MinGroup maintains minimum values for keys matching group prefix.
	MinGroup GroupType = iota
	// MaxGroup maintains maximum values for keys matching group prefix.
	MaxGroup
)

// group organizes a collection of Info objects sharing a common key
// prefix (prefix is defined up to the last period character '.').
// Groups maintain a limited-size set of Info objects with set
// inclusion determined by group type. Two types are implemented here:
//
// MinGroup, MaxGroup: maintain only minimum/maximum values added
// to group respectively.
type group struct {
	Prefix      string    // Key prefix for Info items in group
	Limit       int       // Maximum number of keys in group
	TypeOf      GroupType // Minimums or maximums of all values encountered
	Infos       infoMap   // Map of infos in group
	minTTLStamp int64     // Minimum of all infos' TTLs (Unix nanos)
	gatekeeper  *info     // Minimum or maximum value in infos map, depending on type
}

// groupMap is a map of group prefixes => *group.
type groupMap map[string]*group

// newGroup allocates and returns a new group with prefix, limit and type.
func newGroup(prefix string, limit int, typeOf GroupType) *group {
	if limit <= 0 {
		log.Fatalf("group size limit must be a positive number (%d <= 0)", limit)
	}
	return &group{
		Prefix:      prefix,
		Limit:       limit,
		TypeOf:      typeOf,
		minTTLStamp: math.MaxInt64,
		Infos:       make(infoMap),
	}
}

// shouldInclude returns true if the specified info should belong
// in the group according to the group type and the value.
func (g *group) shouldInclude(i *info) bool {
	if g.gatekeeper == nil {
		return true
	}
	switch g.TypeOf {
	case MinGroup:
		return i.less(g.gatekeeper)
	case MaxGroup:
		return !i.less(g.gatekeeper)
	default:
		log.Fatalf("unknown group type %d", g.TypeOf)
		return false
	}
}

// updateIncremental incrementally updates group stats based on a
// single info. Stats include minimum time-to-live (used to decide
// when compaction is possible), and updates gatekeeper (used to
// decide when to add to group).
func (g *group) updateIncremental(i *info) {
	if i.TTLStamp < g.minTTLStamp {
		g.minTTLStamp = i.TTLStamp
	}
	// Update gatekeeper if it's currently nil --or-- if info shouldn't
	// be included in group (i.e. it's the most extreme min/max).
	if g.gatekeeper == nil || !g.shouldInclude(i) {
		g.gatekeeper = i
	}
}

// update iterates through group infos and updates stats.
func (g *group) update() {
	g.minTTLStamp = math.MaxInt64
	g.gatekeeper = nil

	for _, i := range g.Infos {
		g.updateIncremental(i)
	}
}

// compact compacts the group infos slice by removing expired info objects.
// Returns true if compaction occurred and space is free.
func (g *group) compact() bool {
	now := time.Now().UnixNano()
	if g.minTTLStamp > now {
		return false
	}

	// Delete expired entries && update group stats.
	g.minTTLStamp = math.MaxInt64
	g.gatekeeper = nil
	for _, i := range g.Infos {
		if i.expired(now) {
			g.removeInternal(i)
		} else {
			g.updateIncremental(i)
		}
	}

	return len(g.Infos) < g.Limit
}

// addInternal adds info to group, incrementally updating group stats.
func (g *group) addInternal(i *info) {
	g.Infos[i.Key] = i
	g.updateIncremental(i)
}

// removeInternal removes info from group, updating group stats wholesale if necessary.
func (g *group) removeInternal(i *info) {
	delete(g.Infos, i.Key)
	if g.gatekeeper == i || g.minTTLStamp == i.TTLStamp {
		g.update()
	}
}

// getInfo returns an info by key.
func (g *group) getInfo(key string) *info {
	if i, ok := g.Infos[key]; ok {
		// Check TTL and discard if too old.
		now := time.Now().UnixNano()
		if i.expired(now) {
			g.removeInternal(i)
			return nil
		}
		return i
	}
	return nil
}

// infosAsSlice returns an slice of infos from group, sorted by value;
// sort order is dependent on group type (MinGroup: ascending,
// MaxGroup: descending).
func (g *group) infosAsSlice() infoSlice {
	now := time.Now().UnixNano()
	infos := make(infoSlice, 0, len(g.Infos))
	for _, i := range g.Infos {
		// Check TTL and discard if too old.
		if i.expired(now) {
			g.removeInternal(i)
		} else {
			infos = append(infos, i)
		}
	}
	switch g.TypeOf {
	case MinGroup:
		sort.Sort(infos)
	case MaxGroup:
		sort.Sort(sort.Reverse(infos))
	}
	return infos
}

// addInfo adds the info to the group if there is sufficient space
// or if the info has a value which guarantees it a spot within the
// group according to the group type.
//
// Returns contentsChanged bool and an error. contentsChanged is true
// if this info is new or updates the contents of a preexisting info
// with the same key; if just the timestamp or number of hops changes,
// contentsChanged is false. An error is returned if the info types
// don't match an existing group or the info was older than what we
// currently have.
func (g *group) addInfo(i *info) (contentsChanged bool, err error) {
	// First, see if info is already in the group. If so, and this
	// info timestamp is newer, remove existing info. If the
	// timestamps are equal (i.e. this is the same info), but hops
	// value of prospective info is lower, take the minimum of the two
	// Hops values.
	if existingInfo, ok := g.Infos[i.Key]; ok {
		if i.Timestamp < existingInfo.Timestamp ||
			(i.Timestamp == existingInfo.Timestamp && i.Hops >= existingInfo.Hops) {
			err = util.Errorf("info %+v older than current info %+v", i, existingInfo)
			return
		}
		g.removeInternal(existingInfo)
		contentsChanged = !reflect.DeepEqual(existingInfo.Val, i.Val)
	} else {
		// No preexisting info means contentsChanged is true.
		contentsChanged = true
	}

	// If the group is not empty, verify types match by comparing to
	// gatekeeper.
	if g.gatekeeper != nil {
		t1 := reflect.TypeOf(i.Val).Kind()
		t2 := reflect.TypeOf(g.gatekeeper.Val).Kind()
		if t1 != t2 {
			err = util.Errorf("info %+v has type %s whereas group has type %s", i, t1, t2)
			return
		}
	}

	// If there's free space or we successfully compacted, add info.
	if len(g.Infos) < g.Limit || g.compact() {
		g.addInternal(i)
		return
	}

	// Group limit is reached. Check gatekeeper; if we should include,
	// it means we toss current gatekeeper and add info.
	if g.shouldInclude(i) {
		g.removeInternal(g.gatekeeper)
		g.addInternal(i)
		return
	}

	err = util.Errorf("info %+v not added to group", i)
	return
}
