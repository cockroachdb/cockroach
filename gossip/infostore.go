package gossip

import (
	"fmt"
	"sort"
	"time"
)

// InfoStore objects manage maps of Info and maps of Info Group
// objects. They maintain a sequence number generator which they use
// to allocate new info objects.
//
// InfoStores can be queried for incremental updates occurring since a
// specified sequence number.
//
// InfoStores can be combined using deltas from peer nodes.
type InfoStore struct {
	Infos  InfoMap  // Map from key to info
	Groups GroupMap // Map from key prefix to groups of infos
	MaxSeq int32    // Maximum sequence number inserted
	SeqGen int32    // Sequence generator incremented each time info is added
}

// Returns true if the info belongs to a group registered with
// the info store; false otherwise.
func (is *InfoStore) belongsToGroup(key string) *Group {
	if group, ok := is.Groups[InfoPrefix(key)]; ok {
		return group
	}
	return nil
}

// Create a new InfoStore.
func NewInfoStore() *InfoStore {
	return &InfoStore{make(InfoMap), make(GroupMap), 0, 0}
}

// Returns a new info object using specified key, value, and
// time-to-live.
func (is *InfoStore) NewInfo(key string, val Value, ttl time.Duration) *Info {
	is.SeqGen++
	now := time.Now().UnixNano()
	node := "localhost" // TODO(spencer): fix this
	return &Info{key, val, now, now + int64(ttl), is.SeqGen, node, 0}
}

// Returns an Info object by key or nil if it doesn't exist.
func (is *InfoStore) GetInfo(key string) *Info {
	if group := is.belongsToGroup(key); group != nil {
		return group.GetInfo(key)
	}
	if info, ok := is.Infos[key]; ok {
		return info
	}
	return nil
}

// Returns an array of info objects from specified group, sorted by
// value; sort order is dependent on group type (MIN_GROUP: ascending,
// MAX_GROUP: descending). Returns nil if group is not registered.
func (is *InfoStore) GetGroupInfos(prefix string) InfoArray {
	if group, ok := is.Groups[prefix]; ok {
		infos := group.InfosAsArray()
		switch group.TypeOf {
		case MIN_GROUP:
			sort.Sort(infos)
		case MAX_GROUP:
			sort.Sort(sort.Reverse(infos))
		}
		return infos
	}
	return nil
}

// Registers a new group with info store. Subsequent additions of
// infos will first check whether the info prefix matches a group. On
// match, the info will be added to the group instead of to the info
// map.
//
// REQUIRES: group.prefix is not already in the info store's groups map.
func (is *InfoStore) RegisterGroup(group *Group) error {
	if _, ok := is.Groups[group.Prefix]; ok {
		return fmt.Errorf("group \"%s\" already in group map", group.Prefix)
	}
	is.Groups[group.Prefix] = group
	return nil
}

// Adds or updates an info in the infos or groups maps. If the prefix
// of the info is a key of the info store's groups map, then the info
// is added to that group (prefix is defined by prefix of string up
// until last period '.'). Otherwise, the info is added to the infos
// map.
//
// Returns true if info was added; false otherwise.
func (is *InfoStore) AddInfo(info *Info) bool {
	// If the prefix matches a group, add to group.
	if group := is.belongsToGroup(info.Key); group != nil {
		if group.AddInfo(info) {
			if info.Seq > is.MaxSeq {
				is.MaxSeq = info.Seq
			}
			return true
		}
		return false
	}
	// Only replace an existing info if new timestamp is greater.
	if existingInfo, ok := is.Infos[info.Key]; ok {
		if info.Timestamp <= existingInfo.Timestamp {
			return false // Skip update, we already have newer value
		}
		// Take the minimum of the two Hops values, as that represents
		// our closest connection to the source of the info and we should
		// get credit for that when comparing peers.
		if existingInfo.Hops < info.Hops {
			info.Hops = existingInfo.Hops
		}
	}
	// Update info map.
	is.Infos[info.Key] = info
	if info.Seq > is.MaxSeq {
		is.MaxSeq = info.Seq
	}
	return true
}

// Combines an incremental delta with the current info store.
// The sequence numbers on all info objects are reset using the
// info store's sequence generator. All hop distances on infos
// are incremented to indicate they've arrived from an external
// source.
func (is *InfoStore) Combine(delta *InfoStore) {
	// Combine group info. We extract the infos from the group
	// and combine them one-by-one using AddInfo. This accounts
	// for any potential differences in info store configurations
	// between nodes.
	for _, group := range delta.Groups {
		for _, info := range group.Infos {
			is.SeqGen++
			info.Seq = is.SeqGen
			info.Hops++
			is.AddInfo(info)
		}
	}

	// Combine non-group info.
	for _, info := range delta.Infos {
		is.SeqGen++
		info.Seq = is.SeqGen
		info.Hops++
		is.AddInfo(info)
	}
}

// Returns an incremental delta of infos added to the info store since
// (not including) the specified sequence number. These deltas are
// intended for efficiently updating peer nodes.
//
// Returns an error if there are no deltas.
func (is *InfoStore) Delta(seq int32) (*InfoStore, error) {
	if seq >= is.MaxSeq {
		return nil, fmt.Errorf("no deltas to info store since sequence number %d", seq)
	}

	delta := new(InfoStore)
	delta.SeqGen = is.SeqGen

	// Delta of group maps.
	for _, group := range is.Groups {
		if deltaGroup, err := group.Delta(seq); err != nil {
			delta.RegisterGroup(deltaGroup)
		}
	}

	// Delta of info map.
	for _, info := range is.Infos {
		if info.Seq > seq {
			delta.Infos[info.Key] = info
		}
	}

	return delta, nil
}
