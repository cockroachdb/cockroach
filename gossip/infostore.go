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
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// callback holds regexp pattern match and GossipCallback method.
type callback struct {
	pattern *regexp.Regexp
	method  Callback
}

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
	Infos     infoMap             `json:"infos,omitempty"`  // Map from key to info
	Groups    groupMap            `json:"groups,omitempty"` // Map from key prefix to groups of infos
	NodeID    proto.NodeID        `json:"-"`                // Owning node's ID
	NodeAddr  util.UnresolvedAddr `json:"-"`                // Address of node owning this info store: "host:port"
	MaxSeq    int64               `json:"-"`                // Maximum sequence number inserted
	seqGen    int64               // Sequence generator incremented each time info is added
	callbacks []callback
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
	if now <= lastTime {
		now = lastTime + 1
	}
	lastTime = now
	return now
}

// String returns a string representation of an infostore.
func (is *infoStore) String() string {
	buf := bytes.Buffer{}
	if count := is.infoCount(); count > 0 {
		buf.WriteString(fmt.Sprintf("infostore with %d info(s): ", count))
	} else {
		return "infostore (empty)"
	}

	prepend := ""

	// Compute delta of groups and infos.
	if err := is.visitInfos(func(g *group) error {
		str := fmt.Sprintf("%sgroup %q", prepend, g.G.Prefix)
		prepend = ", "
		_, err := buf.WriteString(str)
		return err
	}, func(i *Info) error {
		str := fmt.Sprintf("%sinfo %q: %+v", prepend, i.Key, i.Val)
		prepend = ", "
		_, err := buf.WriteString(str)
		return err
	}); err != nil {
		log.Errorf("failed to properly construct string representation of infoStore: %s", err)
	}
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
func newInfoStore(nodeID proto.NodeID, nodeAddr util.UnresolvedAddr) *infoStore {
	return &infoStore{
		Infos:    infoMap{},
		Groups:   groupMap{},
		NodeID:   nodeID,
		NodeAddr: nodeAddr,
	}
}

// newInfo allocates and returns a new info object using specified key,
// value, and time-to-live.
func (is *infoStore) newInfo(key string, val interface{}, ttl time.Duration) *Info {
	is.seqGen++
	now := monotonicUnixNano()
	ttlStamp := now + int64(ttl)
	if ttl == 0 {
		ttlStamp = math.MaxInt64
	}
	i := &Info{
		Key:       key,
		Timestamp: now,
		TTLStamp:  ttlStamp,
		NodeID:    is.NodeID,
		PeerID:    is.NodeID,
		Seq:       is.seqGen,
	}
	i.setValue(val)
	return i
}

// getInfo returns an info object by key or nil if it doesn't exist.
func (is *infoStore) getInfo(key string) *Info {
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
	if g2, ok := is.Groups[g.G.Prefix]; ok {
		if g.G.Prefix != g2.G.Prefix || g.G.Limit != g2.G.Limit || g.G.TypeOf != g2.G.TypeOf {
			return util.Errorf("group %q already in group map with different settings %v vs. %v",
				g.G.Prefix, g, g2)
		}
		return nil
	}
	is.Groups[g.G.Prefix] = g
	return nil
}

// addInfo adds or updates an info in the infos or groups maps. If the
// prefix of the info is a key of the info store's groups map, then the
// info is added to that group (prefix is defined by prefix of string up
// until last period '.'). Otherwise, the info is added to the infos map.
//
// Returns nil if info was added; error otherwise.
func (is *infoStore) addInfo(i *Info) error {
	// If the prefix matches a group, add to group.
	if group := is.belongsToGroup(i.Key); group != nil {
		contentsChanged, err := group.addInfo(i)
		if err != nil {
			return err
		}
		if i.Seq > is.MaxSeq {
			is.MaxSeq = i.Seq
		}
		is.processCallbacks(i.Key, contentsChanged)
		return nil
	}
	// Only replace an existing info if new timestamp is greater, or if
	// timestamps are equal, but new hops is smaller.
	var contentsChanged bool
	if existingInfo, ok := is.Infos[i.Key]; ok {
		if i.Timestamp < existingInfo.Timestamp ||
			(i.Timestamp == existingInfo.Timestamp && i.Hops >= existingInfo.Hops) {
			return util.Errorf("info %+v older than current info %+v", i, existingInfo)
		}
		contentsChanged = !reflect.DeepEqual(existingInfo.Val, i.Val)
	} else {
		// No preexisting info means contentsChanged is true.
		contentsChanged = true
	}
	// Update info map.
	is.Infos[i.Key] = i
	if i.Seq > is.MaxSeq {
		is.MaxSeq = i.Seq
	}
	is.processCallbacks(i.Key, contentsChanged)
	return nil
}

// infoCount returns the count of infos stored in groups and the
// non-group infos map. This is really just an approximation as
// we don't check whether infos are expired.
func (is *infoStore) infoCount() uint32 {
	count := uint32(len(is.Infos))
	for _, group := range is.Groups {
		count += uint32(len(group.G.Infos))
	}
	return count
}

// maxHops returns the maximum hops across all infos in the store.
// This is the maximum number of gossip exchanges between any
// originator and this node.
func (is *infoStore) maxHops() uint32 {
	var maxHops uint32
	// will never error because `return nil` below
	_ = is.visitInfos(nil, func(i *Info) error {
		if i.Hops > maxHops {
			maxHops = i.Hops
		}
		return nil
	})
	return maxHops
}

// registerCallback compiles a regexp for pattern and adds it to
// the callbacks slice.
func (is *infoStore) registerCallback(pattern string, method Callback) {
	re := regexp.MustCompile(pattern)
	is.callbacks = append(is.callbacks, callback{pattern: re, method: method})
	var infos []*Info
	if err := is.visitInfos(nil, func(i *Info) error {
		if re.MatchString(i.Key) {
			infos = append(infos, i)
		}
		return nil
	}); err != nil {
		log.Errorf("failed to properly run registered callback while visiting pre-existing info: %s", err)
	}
	// Run callbacks in a goroutine to avoid mutex reentry.
	go func() {
		for _, i := range infos {
			method(i.Key, true /* contentsChanged */)
		}
	}()
}

// processCallbacks processes callbacks for the specified key by
// matching callback regular expression against the key and invoking
// the corresponding callback method on a match.
func (is *infoStore) processCallbacks(key string, contentsChanged bool) {
	var matches []callback
	for _, cb := range is.callbacks {
		if cb.pattern.MatchString(key) {
			matches = append(matches, cb)
		}
	}
	// Run callbacks in a goroutine to avoid mutex reentry.
	go func() {
		for _, cb := range matches {
			cb.method(key, contentsChanged)
		}
	}()
}

// visitInfos implements a visitor pattern to run two methods in the
// course of visiting all groups, all group infos, and all non-group
// infos. The visitGroup function is run against each group in
// turn. After each group is visited, the visitInfo function is run
// against each of its infos. Finally, after all groups have been
// visitied, the visitInfo function is run against each non-group info
// in turn. Be sure to skip over any expired infos.
func (is *infoStore) visitInfos(visitGroup func(*group) error, visitInfo func(*Info) error) error {
	now := time.Now().UnixNano()
	for _, g := range is.Groups {
		if visitGroup != nil {
			if err := visitGroup(g); err != nil {
				return err
			}
		}
		if visitInfo != nil {
			for _, i := range g.G.Infos {
				if i.expired(now) {
					delete(g.G.Infos, i.Key)
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
	if err := delta.visitInfos(func(g *group) error {
		if _, ok := is.Groups[g.G.Prefix]; !ok {
			// Make a copy of the group.
			gCopy := newGroup(g.G.Prefix, g.G.Limit, g.G.TypeOf)
			return is.registerGroup(gCopy)
		}
		return nil
	}, func(i *Info) error {
		is.seqGen++
		i.Seq = is.seqGen
		i.Hops++
		i.PeerID = delta.NodeID
		// Errors from addInfo here are not a problem; they simply
		// indicate that the data in *is is newer than in *delta.
		if err := is.addInfo(i); err == nil {
			freshCount++
		}
		return nil
	}); err != nil {
		log.Errorf("failed to properly combine infoStores: %s", err)
	}
	return freshCount
}

// delta returns an incremental delta of infos added to the info store
// since (not including) the specified sequence number. These deltas
// are intended for efficiently updating peer nodes. Any infos passed
// from node requesting delta are ignored.
//
// Returns nil if there are no deltas.
func (is *infoStore) delta(nodeID proto.NodeID, seq int64) *infoStore {
	if seq >= is.MaxSeq {
		return nil
	}

	delta := newInfoStore(is.NodeID, is.NodeAddr)

	// Compute delta of groups and infos.
	if err := is.visitInfos(func(g *group) error {
		gDelta := newGroup(g.G.Prefix, g.G.Limit, g.G.TypeOf)
		return delta.registerGroup(gDelta)
	}, func(i *Info) error {
		if i.isFresh(nodeID, seq) {
			return delta.addInfo(i)
		}
		return nil
	}); err != nil {
		log.Errorf("failed to properly create delta infoStore: %s", err)
	}

	delta.MaxSeq = is.MaxSeq
	return delta
}

// distant returns a nodeSet for gossip peers which originated infos
// with info.Hops > maxHops.
func (is *infoStore) distant(maxHops uint32) *nodeSet {
	ns := newNodeSet(0)
	// will never error because `return nil` below
	_ = is.visitInfos(nil, func(i *Info) error {
		if i.Hops > maxHops {
			ns.addNode(i.NodeID)
		}
		return nil
	})
	return ns
}

// leastUseful determines which node ID from amongst the set is
// currently contributing the least. Returns 0 if nodes is empty.
func (is *infoStore) leastUseful(nodes *nodeSet) proto.NodeID {
	contrib := make(map[proto.NodeID]int, nodes.len())
	for node := range nodes.nodes {
		contrib[node] = 0
	}
	// will never error because `return nil` below
	_ = is.visitInfos(nil, func(i *Info) error {
		contrib[i.PeerID]++
		return nil
	})

	least := math.MaxInt32
	var leastNode proto.NodeID
	for id, count := range contrib {
		if nodes.hasNode(id) {
			if count < least {
				least = count
				leastNode = id
			}
		}
	}
	return leastNode
}
