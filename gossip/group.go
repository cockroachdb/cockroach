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
	prefix      string    // Key prefix for Info items in group
	limit       int       // Maximum number of keys in group
	typeOf      GroupType // Minimums or maximums of all values encountered
	minTTLStamp int64     // Minimum of all infos' TTLs (Unix nanos)
	gatekeeper  *Info     // Minimum or maximum value in infos map, depending on type
	infos       InfoMap   // Map of infos in group
}
type GroupMap map[string]*Group // Group map is from group prefix => *Group

// Returns true if the specified info should belong in the group
// according to the group type and the value.
func (group *Group) shouldInclude(info *Info) bool {
	if group.gatekeeper == nil {
		return true
	}
	switch group.typeOf {
	case MIN_GROUP:
		return info.Val.Less(group.gatekeeper.Val)
	case MAX_GROUP:
		return !info.Val.Less(group.gatekeeper.Val)
	default:
		panic(fmt.Errorf("unknown group type %d", group.typeOf))
	}
}

// Incrementally updates group stats based on a single info. Stats
// include minimum time-to-live (used to decide when compaction is
// possible), and updates gatekeeper (used to decide when to add to
// group).
func (group *Group) updateIncremental(info *Info) {
	if info.TTLStamp < group.minTTLStamp {
		group.minTTLStamp = info.TTLStamp
	}
	// Update gatekeeper if it's currently nil --or-- if info shouldn't
	// be included in group (i.e. it's the most extreme min/max).
	if group.gatekeeper == nil || !group.shouldInclude(info) {
		group.gatekeeper = info
	}
}

// Iterates through group infos and updates stats.
func (group *Group) update() {
	group.minTTLStamp = math.MaxInt64
	group.gatekeeper = nil

	for _, info := range group.infos {
		group.updateIncremental(info)
	}
}

// Compacts the group infos array by removing expired info objects.
// Returns true if compaction occurred and space is free.
func (group *Group) compact() bool {
	now := time.Now().UnixNano()
	if group.minTTLStamp > now {
		return false
	}

	// Delete expired entries && update group stats.
	group.minTTLStamp = math.MaxInt64
	group.gatekeeper = nil
	for key, info := range group.infos {
		if info.TTLStamp <= now {
			delete(group.infos, key)
		} else {
			group.updateIncremental(info)
		}
	}

	return len(group.infos) < group.limit
}

// Adds info to group, incrementally updating group stats.
func (group *Group) addInternal(info *Info) {
	group.infos[info.Key] = info
	group.updateIncremental(info)
}

// Removes info from group, updating group stats wholesale if necessary.
func (group *Group) removeInternal(info *Info) {
	delete(group.infos, info.Key)
	if group.gatekeeper == info || group.minTTLStamp == info.TTLStamp {
		group.update()
	}
}

// Creates a new group with prefix, limit and type.
func NewGroup(prefix string, limit int, typeOf GroupType) *Group {
	return &Group{prefix, limit, typeOf, math.MaxInt64, nil, make(InfoMap)}
}

// Returns an info by key.
func (group *Group) GetInfo(key string) *Info {
	if info, ok := group.infos[key]; ok {
		return info
	}
	return nil
}

// Returns array of infos from group.
func (group *Group) Infos() InfoArray {
	infos := make(InfoArray, 0, len(group.infos))
	for _, info := range group.infos {
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
	if existingInfo, ok := group.infos[info.Key]; ok {
		if existingInfo.Timestamp <= info.Timestamp {
			group.removeInternal(info)
		} else {
			return false // The info being added is older than what we have; skip
		}
	}

	// If there's free space or we successfully compacted, add info.
	if len(group.infos) < group.limit || group.compact() {
		group.addInternal(info)
		return true // Successfully appended to group
	}

	// Group limit is reached. Check gatekeeper; if we should include,
	// it means we toss current gatekeeper and add info.
	if group.shouldInclude(info) {
		group.removeInternal(group.gatekeeper)
		group.addInternal(info)
		return true
	}

	return false
}

// Returns a delta of the group since the specified sequence number.
// Returns an error if the delta is empty.
func (group *Group) Delta(seq int32) (*Group, error) {
	delta := NewGroup(group.prefix, group.limit, group.typeOf)
	for _, info := range group.infos {
		if info.Seq > seq {
			delta.addInternal(info)
		}
	}
	if len(delta.infos) == 0 {
		return nil, fmt.Errorf("no deltas to group since sequence number %d", seq)
	}
	return delta, nil
}
