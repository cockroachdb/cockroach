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
	"bytes"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/util"
)

// infoStore objects manage maps of Info and maps of Info Group
// objects. They maintain a sequence number generator which they use
// to allocate new info objects.
//
// infoStores can be queried for incremental updates occurring since a
// specified sequence number.
//
// infoStores can be combined using deltas from peer nodes.
//
// infoStores are not thread safe.
type infoStore struct {
	Infos    infoMap  // Map from key to info
	Groups   groupMap // Map from key prefix to groups of infos
	NodeAddr net.Addr // Address of node owning this info store: "host:port"
	MaxSeq   int64    // Maximum sequence number inserted
	seqGen   int64    // Sequence generator incremented each time info is added
}

// monotonicUnixNano returns a monotonically increasing value for
// nanoseconds in Unix time. Since equal times are ignored with
// updates to infos, we're careful to avoid incorrectly ignoring a
// newly created value in the event one is created within the same
// nanosecond. Really unlikely except for the case of unittests, but
// better safe than sorry.
func monotonicUnixNano() int64 {
	monoTimeMu.Lock()
	defer monoTimeMu.Unlock()

	now := time.Now().UnixNano()
	if now == lastTime {
		now = lastTime + 1
	}
	lastTime = now
	return now
}

// String returns a string representation of an infostore.
func (is *infoStore) String() string {
	buf := bytes.Buffer{}
	prepend := ""
	if count := is.infoCount(); count > 0 {
		buf.WriteString(fmt.Sprintf("infostore contains %d info(s)", count))
	} else {
		buf.WriteString("infostore is empty")
	}
	// Compute delta of groups and infos.
	is.visitInfos(func(g *group) error {
		buf.WriteString(prepend)
		prepend = ", "
		buf.WriteString(fmt.Sprintf("group %q", g.Prefix))
		return nil
	}, func(i *info) error {
		buf.WriteString(prepend)
		prepend = ", "
		buf.WriteString(fmt.Sprintf("info %q: %+v", i.Key, i.Val))
		return nil
	})
	return buf.String()
}

// Returns true if the info belongs to a group registered with
// the info store; false otherwise.
func (is *infoStore) belongsToGroup(key string) *group {
	if g, ok := is.Groups[infoPrefix(key)]; ok {
		return g
	}
	return nil
}

var (
	monoTimeMu sync.Mutex
	lastTime   int64
)

// newInfoStore allocates and returns a new infoStore.
// "NodeAddr" is the address of the node owning the infostore
// in "host:port" format.
func newInfoStore(nodeAddr net.Addr) *infoStore {
	return &infoStore{
		Infos:    infoMap{},
		Groups:   groupMap{},
		NodeAddr: nodeAddr,
	}
}

// newInfo allocates and returns a new info object using specified key,
// value, and time-to-live.
func (is *infoStore) newInfo(key string, val interface{}, ttl time.Duration) *info {
	is.seqGen++
	now := monotonicUnixNano()
	ttlStamp := now + int64(ttl)
	if ttl == 0*time.Second {
		ttlStamp = math.MaxInt64
	}
	return &info{
		Key:       key,
		Val:       val,
		Timestamp: now,
		TTLStamp:  ttlStamp,
		NodeAddr:  is.NodeAddr,
		peerAddr:  is.NodeAddr,
		seq:       is.seqGen,
	}
}

// getInfo returns an info object by key or nil if it doesn't exist.
func (is *infoStore) getInfo(key string) *info {
	if group := is.belongsToGroup(key); group != nil {
		return group.getInfo(key)
	}
	if info, ok := is.Infos[key]; ok {
		// Check TTL and discard if too old.
		if info.expired(time.Now().UnixNano()) {
			delete(is.Infos, key)
			return nil
		}
		return info
	}
	return nil
}

// getGroupInfos returns an array of info objects from specified group.
// Returns nil if group is not registered.
func (is *infoStore) getGroupInfos(prefix string) infoSlice {
	if group, ok := is.Groups[prefix]; ok {
		return group.infosAsSlice()
	}
	return nil
}

// registerGroup registers a new group with info store. Subsequent
// additions of infos will first check whether the info prefix matches
// a group. On match, the info will be added to the group instead of
// to the info map.
//
// REQUIRES: group.prefix is not already in the info store's groups map.
func (is *infoStore) registerGroup(g *group) error {
	if g2, ok := is.Groups[g.Prefix]; ok {
		if g.Prefix != g2.Prefix || g.Limit != g2.Limit || g.TypeOf != g2.TypeOf {
			return util.Errorf("group %q already in group map with different settings %v vs. %v",
				g.Prefix, g, g2)
		}
		return nil
	}
	is.Groups[g.Prefix] = g
	return nil
}

// addInfo adds or updates an info in the infos or groups maps. If the
// prefix of the info is a key of the info store's groups map, then the
// info is added to that group (prefix is defined by prefix of string up
// until last period '.'). Otherwise, the info is added to the infos map.
//
// Returns nil if info was added; error otherwise.
func (is *infoStore) addInfo(i *info) error {
	// If the prefix matches a group, add to group.
	if group := is.belongsToGroup(i.Key); group != nil {
		if err := group.addInfo(i); err != nil {
			return err
		}
		if i.seq > is.MaxSeq {
			is.MaxSeq = i.seq
		}
		return nil
	}
	// Only replace an existing info if new timestamp is greater, or if
	// timestamps are equal, but new hops is smaller.
	if existingInfo, ok := is.Infos[i.Key]; ok {
		if i.Timestamp < existingInfo.Timestamp ||
			(i.Timestamp == existingInfo.Timestamp && i.Hops >= existingInfo.Hops) {
			return util.Errorf("info %+v older than current group info %+v", i, existingInfo)
		}
	}
	// Update info map.
	is.Infos[i.Key] = i
	if i.seq > is.MaxSeq {
		is.MaxSeq = i.seq
	}
	return nil
}

// infoCount returns the count of infos stored in groups and the
// non-group infos map. This is really just an approximation as
// we don't check whether infos are expired.
func (is *infoStore) infoCount() uint32 {
	count := uint32(len(is.Infos))
	for _, group := range is.Groups {
		count += uint32(len(group.Infos))
	}
	return count
}

// maxHops returns the maximum hops across all infos in the store.
// This is the maximum number of gossip exchanges between any
// originator and this node.
func (is *infoStore) maxHops() uint32 {
	var maxHops uint32
	is.visitInfos(nil, func(i *info) error {
		if i.Hops > maxHops {
			maxHops = i.Hops
		}
		return nil
	})
	return maxHops
}

// visitInfos implements a visitor pattern to run two methods in the
// course of visiting all groups, all group infos, and all non-group
// infos. The visitGroup function is run against each group in
// turn. After each group is visited, the visitInfo function is run
// against each of its infos.  Finally, after all groups have been
// visitied, the visitInfo function is run against each non-group info
// in turn. Be sure to skip over any expired infos.
func (is *infoStore) visitInfos(visitGroup func(*group) error, visitInfo func(*info) error) error {
	now := time.Now().UnixNano()
	for _, g := range is.Groups {
		if visitGroup != nil {
			if err := visitGroup(g); err != nil {
				return err
			}
		}
		if visitInfo != nil {
			for _, i := range g.Infos {
				if i.expired(now) {
					delete(g.Infos, i.Key)
					continue
				}
				if err := visitInfo(i); err != nil {
					return err
				}
			}
		}
	}

	if visitInfo != nil {
		for _, i := range is.Infos {
			if i.expired(now) {
				delete(is.Infos, i.Key)
				continue
			}
			if err := visitInfo(i); err != nil {
				return err
			}
		}
	}

	return nil
}

// combine combines an incremental delta with the current infoStore.
// The sequence numbers on all info objects are reset using the info
// store's sequence generator. All hop distances on infos are
// incremented to indicate they've arrived from an external source.
// Returns the count of "fresh" infos in the provided delta.
func (is *infoStore) combine(delta *infoStore) int {
	// combine group info. If the group doesn't yet exist, register
	// it. Extract the infos from the group and combine them
	// one-by-one using addInfo.
	var freshCount int
	delta.visitInfos(func(g *group) error {
		if _, ok := is.Groups[g.Prefix]; !ok {
			// Make a copy of the group.
			gCopy := newGroup(g.Prefix, g.Limit, g.TypeOf)
			is.registerGroup(gCopy)
		}
		return nil
	}, func(i *info) error {
		is.seqGen++
		i.seq = is.seqGen
		i.Hops++
		i.peerAddr = delta.NodeAddr
		if is.addInfo(i) == nil {
			freshCount++
		}
		return nil
	})
	return freshCount
}

// delta returns an incremental delta of infos added to the info store
// since (not including) the specified sequence number. These deltas
// are intended for efficiently updating peer nodes. Any infos passed
// from node requesting delta are ignored.
//
// Returns nil if there are no deltas.
func (is *infoStore) delta(addr net.Addr, seq int64) *infoStore {
	if seq >= is.MaxSeq {
		return nil
	}

	delta := newInfoStore(is.NodeAddr)

	// Compute delta of groups and infos.
	is.visitInfos(func(g *group) error {
		gDelta := newGroup(g.Prefix, g.Limit, g.TypeOf)
		delta.registerGroup(gDelta)
		return nil
	}, func(i *info) error {
		if i.isFresh(addr, seq) {
			delta.addInfo(i)
		}
		return nil
	})

	delta.MaxSeq = is.MaxSeq
	return delta
}

// distant returns an addrSet of node addresses for gossip peers which
// originated infos with info.Hops > maxHops.
func (is *infoStore) distant(maxHops uint32) *addrSet {
	addrMap := make(map[string]net.Addr)
	is.visitInfos(nil, func(i *info) error {
		if i.Hops > maxHops {
			addrMap[i.NodeAddr.String()] = i.NodeAddr
		}
		return nil
	})

	// Build and return array of addresses.
	addrs := newAddrSet(len(addrMap))
	for _, addr := range addrMap {
		addrs.addAddr(addr)
	}
	return addrs
}

// leastUseful determines which node from amongst the node addresses
// listed in addrs is currently contributing the least. Returns nil
// if addrs is empty.
func (is *infoStore) leastUseful(addrs *addrSet) net.Addr {
	contrib := make(map[string]int, addrs.len())
	for key := range addrs.addrs {
		contrib[key] = 0
	}
	is.visitInfos(nil, func(i *info) error {
		if addrs.hasAddr(i.peerAddr) {
			contrib[i.peerAddr.String()]++
		}
		return nil
	})

	least := math.MaxInt32
	var leastKey string
	for key, count := range contrib {
		if count < least {
			least = count
			leastKey = key
		}
	}

	if least == math.MaxInt32 {
		return nil
	}
	return addrs.addrs[leastKey]
}
