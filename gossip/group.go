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

// GroupType indicates the bounds of the values encountered within the group.
type GroupType int

const (
	// MinGroup maintains minimum values for keys matching group prefix.
	MinGroup GroupType = iota
	// MaxGroup maintains maximum values for keys matching group prefix.
	MaxGroup
)

// Group organizes a collection of Info objects sharing a common key
// prefix (prefix is defined up to the last period character '.').
// Groups maintain a limited-size set of Info objects with set
// inclusion determined by group type. Two types are implemented here:
//
// MinGroup, MaxGroup: maintain only minimum/maximum values added
// to group respectively.
type Group struct {
	Prefix      string    // Key prefix for Info items in group
	Limit       int       // Maximum number of keys in group
	TypeOf      GroupType // Minimums or maximums of all values encountered
	MinTTLStamp int64     // Minimum of all infos' TTLs (Unix nanos)
	Gatekeeper  *Info     // Minimum or maximum value in infos map, depending on type
	Infos       InfoMap   // Map of infos in group
}

// GroupMap is a map of group prefixes => *Group.
type GroupMap map[string]*Group

// shouldInclude returns true if the specified info should belong
// in the group according to the group type and the value.
func (g *Group) shouldInclude(info *Info) bool {
	if g.Gatekeeper == nil {
		return true
	}
	switch g.TypeOf {
	case MinGroup:
		return info.Val.Less(g.Gatekeeper.Val)
	case MaxGroup:
		return !info.Val.Less(g.Gatekeeper.Val)
	default:
		panic(fmt.Errorf("unknown group type %d", g.TypeOf))
	}
}

// updateIncremental incrementally updates group stats based on a
// single info. Stats include minimum time-to-live (used to decide
// when compaction is possible), and updates gatekeeper (used to
// decide when to add to group).
func (g *Group) updateIncremental(info *Info) {
	if info.TTLStamp < g.MinTTLStamp {
		g.MinTTLStamp = info.TTLStamp
	}
	// Update gatekeeper if it's currently nil --or-- if info shouldn't
	// be included in group (i.e. it's the most extreme min/max).
	if g.Gatekeeper == nil || !g.shouldInclude(info) {
		g.Gatekeeper = info
	}
}

// update iterates through group infos and updates stats.
func (g *Group) update() {
	g.MinTTLStamp = math.MaxInt64
	g.Gatekeeper = nil

	for _, info := range g.Infos {
		g.updateIncremental(info)
	}
}

// compact compacts the group infos array by removing expired info objects.
// Returns true if compaction occurred and space is free.
func (g *Group) compact() bool {
	now := time.Now().UnixNano()
	if g.MinTTLStamp > now {
		return false
	}

	// Delete expired entries && update group stats.
	g.MinTTLStamp = math.MaxInt64
	g.Gatekeeper = nil
	for key, info := range g.Infos {
		if info.TTLStamp <= now {
			delete(g.Infos, key)
		} else {
			g.updateIncremental(info)
		}
	}

	return len(g.Infos) < g.Limit
}

// addInternal adds info to group, incrementally updating group stats.
func (g *Group) addInternal(info *Info) {
	g.Infos[info.Key] = info
	g.updateIncremental(info)
}

// removeInternal removes info from group, updating group stats wholesale if necessary.
func (g *Group) removeInternal(info *Info) {
	delete(g.Infos, info.Key)
	if g.Gatekeeper == info || g.MinTTLStamp == info.TTLStamp {
		g.update()
	}
}

// NewGroup allocates and returns a new group with prefix, limit and type.
func NewGroup(prefix string, limit int, typeOf GroupType) (*Group, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("group size limit must be a positive number (%d <= 0)", limit)
	}
	return &Group{
		Prefix:      prefix,
		Limit:       limit,
		TypeOf:      typeOf,
		MinTTLStamp: math.MaxInt64,
		Infos:       make(InfoMap),
	}, nil
}

// GetInfo returns an info by key.
func (g *Group) GetInfo(key string) *Info {
	if info, ok := g.Infos[key]; ok {
		// Check TTL and discard if too old.
		now := time.Now().UnixNano()
		if info.TTLStamp <= now {
			delete(g.Infos, key)
			return nil
		}
		return info
	}
	return nil
}

// InfosAsArray returns an array of infos from group.
func (g *Group) InfosAsArray() InfoArray {
	now := time.Now().UnixNano()
	infos := make(InfoArray, 0, len(g.Infos))
	for _, info := range g.Infos {
		// Check TTL and discard if too old.
		if info.TTLStamp <= now {
			delete(g.Infos, info.Key)
		} else {
			infos = append(infos, info)
		}
	}
	return infos
}

// AddInfo adds the info to the group if there is sufficient space
// or if the info has a value which guarantees it a spot within the
// group according to the group type.
//
// Returns true if the info was added; false otherwise.
func (g *Group) AddInfo(info *Info) bool {
	// First, see if info is already in the group. If so, and this
	// info timestamp is newer, remove existing info.
	if existingInfo, ok := g.Infos[info.Key]; ok {
		if existingInfo.Timestamp < info.Timestamp {
			// Take the minimum of the two Hops values; see comments
			// in InfoStore.AddInfo.
			if existingInfo.Hops < info.Hops {
				info.Hops = existingInfo.Hops
			}
			g.removeInternal(info)
		} else {
			return false // The info being added is older than what we have; skip
		}
	}

	// If there's free space or we successfully compacted, add info.
	if len(g.Infos) < g.Limit || g.compact() {
		g.addInternal(info)
		return true // Successfully appended to group
	}

	// Group limit is reached. Check gatekeeper; if we should include,
	// it means we toss current gatekeeper and add info.
	if g.shouldInclude(info) {
		g.removeInternal(g.Gatekeeper)
		g.addInternal(info)
		return true
	}

	return false
}
