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
	"math"
	"time"
)

type GroupType int

const (
	MIN_GROUP GroupType = iota
	MAX_GROUP
)

// Groups organize a collection of Info objects sharing a common key
// prefix (prefix is defined up to the last period character '.').
// Groups maintain a limited-size set of Info objects with set
// inclusion determined by group type. Two types are implemented here:
//
// MIN_GROUP, MAX_GROUP: maintain only minimum/maximum values added
// to group respectively.
type Group struct {
	Prefix      string    // Key prefix for Info items in group
	Limit       int       // Maximum number of keys in group
	TypeOf      GroupType // Minimums or maximums of all values encountered
	MinTTLStamp int64     // Minimum of all infos' TTLs (Unix nanos)
	Gatekeeper  *Info     // Minimum or maximum value in infos map, depending on type
	Infos       InfoMap   // Map of infos in group
}
type GroupMap map[string]*Group // Group map is from group prefix => *Group

// Returns true if the specified info should belong in the group
// according to the group type and the value.
func (group *Group) shouldInclude(info *Info) bool {
	if group.Gatekeeper == nil {
		return true
	}
	switch group.TypeOf {
	case MIN_GROUP:
		return info.Val.Less(group.Gatekeeper.Val)
	case MAX_GROUP:
		return !info.Val.Less(group.Gatekeeper.Val)
	default:
		panic(fmt.Errorf("unknown group type %d", group.TypeOf))
	}
}

// Incrementally updates group stats based on a single info. Stats
// include minimum time-to-live (used to decide when compaction is
// possible), and updates gatekeeper (used to decide when to add to
// group).
func (group *Group) updateIncremental(info *Info) {
	if info.TTLStamp < group.MinTTLStamp {
		group.MinTTLStamp = info.TTLStamp
	}
	// Update gatekeeper if it's currently nil --or-- if info shouldn't
	// be included in group (i.e. it's the most extreme min/max).
	if group.Gatekeeper == nil || !group.shouldInclude(info) {
		group.Gatekeeper = info
	}
}

// Iterates through group infos and updates stats.
func (group *Group) update() {
	group.MinTTLStamp = math.MaxInt64
	group.Gatekeeper = nil

	for _, info := range group.Infos {
		group.updateIncremental(info)
	}
}

// Compacts the group infos array by removing expired info objects.
// Returns true if compaction occurred and space is free.
func (group *Group) compact() bool {
	now := time.Now().UnixNano()
	if group.MinTTLStamp > now {
		return false
	}

	// Delete expired entries && update group stats.
	group.MinTTLStamp = math.MaxInt64
	group.Gatekeeper = nil
	for key, info := range group.Infos {
		if info.TTLStamp <= now {
			delete(group.Infos, key)
		} else {
			group.updateIncremental(info)
		}
	}

	return len(group.Infos) < group.Limit
}

// Adds info to group, incrementally updating group stats.
func (group *Group) addInternal(info *Info) {
	group.Infos[info.Key] = info
	group.updateIncremental(info)
}

// Removes info from group, updating group stats wholesale if necessary.
func (group *Group) removeInternal(info *Info) {
	delete(group.Infos, info.Key)
	if group.Gatekeeper == info || group.MinTTLStamp == info.TTLStamp {
		group.update()
	}
}

// Creates a new group with prefix, limit and type.
func NewGroup(prefix string, limit int, typeOf GroupType) (*Group, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("group size limit must be a positive number (%d <= 0)", limit)
	}
	return &Group{prefix, limit, typeOf, math.MaxInt64, nil, make(InfoMap)}, nil
}

// Returns an info by key.
func (group *Group) GetInfo(key string) *Info {
	if info, ok := group.Infos[key]; ok {
		return info
	}
	return nil
}

// Returns array of infos from group.
func (group *Group) InfosAsArray() InfoArray {
	infos := make(InfoArray, 0, len(group.Infos))
	for _, info := range group.Infos {
		infos = append(infos, info)
	}
	return infos
}

// Adds the info to the group if there is sufficient space or
// if the info has a value which guarantees it a spot within the
// group according to the group type.
//
// Returns true if the info was added; false otherwise.
func (group *Group) AddInfo(info *Info) bool {
	// First, see if info is already in the group. If so, and this
	// info timestamp is newer, remove existing info.
	if existingInfo, ok := group.Infos[info.Key]; ok {
		if existingInfo.Timestamp < info.Timestamp {
			// Take the minimum of the two Hops values; see comments
			// in InfoStore.AddInfo.
			if existingInfo.Hops < info.Hops {
				info.Hops = existingInfo.Hops
			}
			group.removeInternal(info)
		} else {
			return false // The info being added is older than what we have; skip
		}
	}

	// If there's free space or we successfully compacted, add info.
	if len(group.Infos) < group.Limit || group.compact() {
		group.addInternal(info)
		return true // Successfully appended to group
	}

	// Group limit is reached. Check gatekeeper; if we should include,
	// it means we toss current gatekeeper and add info.
	if group.shouldInclude(info) {
		group.removeInternal(group.Gatekeeper)
		group.addInternal(info)
		return true
	}

	return false
}

// Returns a delta of the group since the specified sequence number.
// Returns an error if the delta is empty.
func (group *Group) Delta(seq int64) (*Group, error) {
	delta, err := NewGroup(group.Prefix, group.Limit, group.TypeOf)
	if err != nil {
		return nil, err
	}
	for _, info := range group.Infos {
		if info.Seq > seq {
			delta.addInternal(info)
		}
	}
	if len(delta.Infos) == 0 {
		return nil, fmt.Errorf("no deltas to group since sequence number %d", seq)
	}
	return delta, nil
}
