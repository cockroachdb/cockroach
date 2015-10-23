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
	"regexp"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// callback holds regexp pattern match and GossipCallback method.
type callback struct {
	pattern *regexp.Regexp
	method  Callback
}

// infoStore objects manage maps of Info objects. They maintain a
// sequence number generator which they use to allocate new info
// objects.
//
// infoStores can be queried for incremental updates occurring since a
// specified sequence number.
//
// infoStores can be combined using deltas from peer nodes.
//
// infoStores are not thread safe.
type infoStore struct {
	Infos     infoMap             `json:"infos,omitempty"` // Map from key to info
	NodeID    roachpb.NodeID      `json:"-"`               // Owning node's ID
	NodeAddr  util.UnresolvedAddr `json:"-"`               // Address of node owning this info store: "host:port"
	MaxSeq    int64               `json:"-"`               // Maximum sequence number inserted
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
	if infoCount := len(is.Infos); infoCount > 0 {
		fmt.Fprintf(&buf, "infostore with %d info(s): ", infoCount)
	} else {
		return "infostore (empty)"
	}

	prepend := ""

	if err := is.visitInfos(func(key string, i *info) error {
		fmt.Fprintf(&buf, "%sinfo %q: %+v", prepend, key, i.Value)
		prepend = ", "
		return nil
	}); err != nil {
		log.Errorf("failed to properly construct string representation of infoStore: %s", err)
	}
	return buf.String()
}

var (
	monoTimeMu sync.Mutex
	lastTime   int64
)

// newInfoStore allocates and returns a new infoStore.
func newInfoStore(nodeID roachpb.NodeID, nodeAddr util.UnresolvedAddr) infoStore {
	return infoStore{
		Infos:    make(infoMap),
		NodeID:   nodeID,
		NodeAddr: nodeAddr,
	}
}

// newInfo allocates and returns a new info object using specified key,
// value, and time-to-live.
func (is *infoStore) newInfo(val []byte, ttl time.Duration) *info {
	is.seqGen++
	now := monotonicUnixNano()
	ttlStamp := now + int64(ttl)
	if ttl == 0 {
		ttlStamp = math.MaxInt64
	}
	v := roachpb.MakeValueFromBytesAndTimestamp(val, roachpb.Timestamp{WallTime: now})
	return &info{
		Info: Info{
			Value:    v,
			TTLStamp: ttlStamp,
			NodeID:   is.NodeID,
		},
		peerID: is.NodeID,
		seq:    is.seqGen,
	}
}

// getInfo returns the Info at key. Returns nil when key is not present
// in the infoStore.
func (is *infoStore) getInfo(key string) *info {
	if info, ok := is.Infos[key]; ok {
		// Check TTL and discard if too old.
		if info.expired(time.Now().UnixNano()) {
			delete(is.Infos, key)
		} else {
			return info
		}
	}
	return nil
}

// addInfo adds or updates an info in the infos map.
//
// Returns nil if info was added; error otherwise.
func (is *infoStore) addInfo(key string, i *info) error {
	// Only replace an existing info if new timestamp is greater, or if
	// timestamps are equal, but new hops is smaller.
	if existingInfo, ok := is.Infos[key]; ok {
		iNanos := i.Value.Timestamp.WallTime
		existingNanos := existingInfo.Value.Timestamp.WallTime
		if iNanos < existingNanos || (iNanos == existingNanos && i.Hops >= existingInfo.Hops) {
			return util.Errorf("info %+v older than current info %+v", i, existingInfo)
		}
	}

	i.Value.InitChecksum([]byte(key))

	// Update info map.
	is.Infos[key] = i
	if i.seq > is.MaxSeq {
		is.MaxSeq = i.seq
	}
	is.processCallbacks(key, i.Value.GetRawBytes())
	return nil
}

// maxHops returns the maximum hops across all infos in the store.
// This is the maximum number of gossip exchanges between any
// originator and this node.
func (is *infoStore) maxHops() uint32 {
	var maxHops uint32
	if err := is.visitInfos(func(key string, i *info) error {
		if i.Hops > maxHops {
			maxHops = i.Hops
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return maxHops
}

// registerCallback compiles a regexp for pattern and adds it to
// the callbacks slice.
func (is *infoStore) registerCallback(pattern string, method Callback) {
	re := regexp.MustCompile(pattern)
	is.callbacks = append(is.callbacks, callback{pattern: re, method: method})
	infos := make(infoMap)
	if err := is.visitInfos(func(key string, i *info) error {
		if re.MatchString(key) {
			infos[key] = i
		}
		return nil
	}); err != nil {
		panic(err)
	}
	// Run callbacks in a goroutine to avoid mutex reentry.
	go func() {
		for key, i := range infos {
			method(key, i.Value.GetRawBytes())
		}
	}()
}

// processCallbacks processes callbacks for the specified key by
// matching callback regular expression against the key and invoking
// the corresponding callback method on a match.
func (is *infoStore) processCallbacks(key string, content []byte) {
	var matches []callback
	for _, cb := range is.callbacks {
		if cb.pattern.MatchString(key) {
			matches = append(matches, cb)
		}
	}
	// Run callbacks in a goroutine to avoid mutex reentry.
	go func() {
		for _, cb := range matches {
			cb.method(key, content)
		}
	}()
}

// visitInfos implements a visitor pattern to run the visitInfo
// function against each info in turn. Be sure to skip over any expired
// infos.
func (is *infoStore) visitInfos(visitInfo func(string, *info) error) error {
	now := time.Now().UnixNano()

	if visitInfo != nil {
		for k, i := range is.Infos {
			if i.expired(now) {
				delete(is.Infos, k)
				continue
			}
			if err := visitInfo(k, i); err != nil {
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
func (is *infoStore) combine(infos map[string]*Info, nodeID roachpb.NodeID) int {
	var freshCount int
	for key, infoProto := range infos {
		i := &info{
			Info: *infoProto,
		}
		is.seqGen++
		i.seq = is.seqGen
		i.Hops++
		i.peerID = nodeID
		// Errors from addInfo here are not a problem; they simply
		// indicate that the data in *is is newer than in *delta.
		if err := is.addInfo(key, i); err == nil {
			freshCount++
		}
	}
	return freshCount
}

// delta returns an incremental delta of infos added to the info store
// since (not including) the specified sequence number. These deltas
// are intended for efficiently updating peer nodes. Any infos passed
// from node requesting delta are ignored.
//
// Returns nil if there are no deltas.
func (is *infoStore) delta(nodeID roachpb.NodeID, seq int64) map[string]*Info {
	infos := make(map[string]*Info)

	if seq < is.MaxSeq {
		// Compute delta of infos.
		if err := is.visitInfos(func(key string, i *info) error {
			if i.isFresh(nodeID, seq) {
				infos[key] = &i.Info
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}

	return infos
}

// distant returns a nodeSet for gossip peers which originated infos
// with info.Hops > maxHops.
func (is *infoStore) distant(maxHops uint32) nodeSet {
	ns := makeNodeSet(0)
	if err := is.visitInfos(func(key string, i *info) error {
		if i.Hops > maxHops {
			ns.addNode(i.NodeID)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return ns
}

// leastUseful determines which node ID from amongst the set is
// currently contributing the least. Returns 0 if nodes is empty.
func (is *infoStore) leastUseful(nodes nodeSet) roachpb.NodeID {
	contrib := make(map[roachpb.NodeID]int, nodes.len())
	for node := range nodes.nodes {
		contrib[node] = 0
	}
	if err := is.visitInfos(func(key string, i *info) error {
		contrib[i.peerID]++
		return nil
	}); err != nil {
		panic(err)
	}

	least := math.MaxInt32
	var leastNode roachpb.NodeID
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
