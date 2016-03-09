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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

type stringMatcher interface {
	MatchString(string) bool
}

type allMatcher struct{}

func (allMatcher) MatchString(string) bool {
	return true
}

// callback holds regexp pattern match and GossipCallback method.
type callback struct {
	matcher stringMatcher
	method  Callback
}

// infoStore objects manage maps of Info objects. They maintain a
// sequence number generator which they use to allocate new info
// objects.
//
// infoStores can be queried for incremental updates occurring since a
// specified map of peer node high water timestamps.
//
// infoStores can be combined using deltas from peer nodes.
//
// infoStores are not thread safe.
type infoStore struct {
	stopper *stop.Stopper

	Infos           infoMap                  `json:"infos,omitempty"` // Map from key to info
	NodeID          roachpb.NodeID           `json:"-"`               // Owning node's ID
	NodeAddr        util.UnresolvedAddr      `json:"-"`               // Address of node owning this info store: "host:port"
	highWaterStamps map[roachpb.NodeID]int64 // Per-node information for gossip peers
	callbacks       []*callback

	callbackMu     sync.Mutex // Serializes callbacks
	callbackWorkMu sync.Mutex // Protects callbackWork
	callbackWork   []func()
}

var monoTime struct {
	sync.Mutex
	last int64
}

var errNotFresh = errors.New("info not fresh")

// monotonicUnixNano returns a monotonically increasing value for
// nanoseconds in Unix time. Since equal times are ignored with
// updates to infos, we're careful to avoid incorrectly ignoring a
// newly created value in the event one is created within the same
// nanosecond. Really unlikely except for the case of unittests, but
// better safe than sorry.
func monotonicUnixNano() int64 {
	monoTime.Lock()
	defer monoTime.Unlock()

	now := timeutil.Now().UnixNano()
	if now <= monoTime.last {
		now = monoTime.last + 1
	}
	monoTime.last = now
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

	if err := is.visitInfos(func(key string, i *Info) error {
		fmt.Fprintf(&buf, "%sinfo %q: %+v", prepend, key, i.Value)
		prepend = ", "
		return nil
	}); err != nil {
		log.Errorf("failed to properly construct string representation of infoStore: %s", err)
	}
	return buf.String()
}

// newInfoStore allocates and returns a new infoStore.
func newInfoStore(nodeID roachpb.NodeID, nodeAddr util.UnresolvedAddr, stopper *stop.Stopper) *infoStore {
	return &infoStore{
		stopper:         stopper,
		Infos:           make(infoMap),
		NodeID:          nodeID,
		NodeAddr:        nodeAddr,
		highWaterStamps: map[roachpb.NodeID]int64{},
	}
}

// newInfo allocates and returns a new info object using specified key,
// value, and time-to-live.
func (is *infoStore) newInfo(val []byte, ttl time.Duration) *Info {
	if is.NodeID == 0 {
		panic("gossip infostore's NodeID is 0")
	}
	now := monotonicUnixNano()
	ttlStamp := now + int64(ttl)
	if ttl == 0 {
		ttlStamp = math.MaxInt64
	}
	v := roachpb.MakeValueFromBytesAndTimestamp(val, roachpb.Timestamp{WallTime: now})
	return &Info{
		Value:    v,
		TTLStamp: ttlStamp,
		NodeID:   is.NodeID,
	}
}

// getInfo returns the Info at key. Returns nil when key is not present
// in the infoStore.
func (is *infoStore) getInfo(key string) *Info {
	if info, ok := is.Infos[key]; ok {
		// Check TTL and discard if too old.
		if info.expired(timeutil.Now().UnixNano()) {
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
func (is *infoStore) addInfo(key string, i *Info) error {
	if i.NodeID == 0 {
		panic("gossip info's NodeID is 0")
	}
	// Only replace an existing info if new timestamp is greater, or if
	// timestamps are equal, but new hops is smaller.
	if existingInfo, ok := is.Infos[key]; ok {
		iNanos := i.Value.Timestamp.WallTime
		existingNanos := existingInfo.Value.Timestamp.WallTime
		if iNanos < existingNanos || (iNanos == existingNanos && i.Hops >= existingInfo.Hops) {
			return errNotFresh
		}
	}
	if i.OrigStamp == 0 {
		i.Value.InitChecksum([]byte(key))
		i.OrigStamp = monotonicUnixNano()
		if highWaterStamp, ok := is.highWaterStamps[i.NodeID]; ok && highWaterStamp >= i.OrigStamp {
			panic(util.Errorf("high water stamp %d >= %d", highWaterStamp, i.OrigStamp))
		}
	}
	// Update info map.
	is.Infos[key] = i
	// Update the high water timestamp & min hops for the originating node.
	if nID := i.NodeID; nID != 0 {
		if hws := is.highWaterStamps[nID]; hws < i.OrigStamp {
			is.highWaterStamps[nID] = i.OrigStamp
		}
	}
	is.processCallbacks(key, i.Value)
	return nil
}

// getHighWaterStamps returns a copy of the high water stamps map of
// gossip peer info maintained by this infostore.
func (is *infoStore) getHighWaterStamps() map[roachpb.NodeID]int64 {
	copy := make(map[roachpb.NodeID]int64, len(is.highWaterStamps))
	for k, hws := range is.highWaterStamps {
		copy[k] = hws
	}
	return copy
}

// registerCallback registers a callback for a key pattern to be
// invoked whenever new info for a gossip key matching pattern is
// received. The callback method is invoked with the info key which
// matched pattern. Returns a function to unregister the callback.
// Note: the callback may fire after being unregistered.
func (is *infoStore) registerCallback(pattern string, method Callback) func() {
	var matcher stringMatcher
	if pattern == ".*" {
		matcher = allMatcher{}
	} else {
		matcher = regexp.MustCompile(pattern)
	}
	cb := &callback{matcher: matcher, method: method}
	is.callbacks = append(is.callbacks, cb)
	if err := is.visitInfos(func(key string, i *Info) error {
		if matcher.MatchString(key) {
			is.runCallbacks(key, i.Value, method)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	return func() {
		for i, targetCB := range is.callbacks {
			if targetCB == cb {
				numCBs := len(is.callbacks)
				is.callbacks[i] = is.callbacks[numCBs-1]
				is.callbacks = is.callbacks[:numCBs-1]
				break
			}
		}
	}
}

// processCallbacks processes callbacks for the specified key by
// matching callback regular expression against the key and invoking
// the corresponding callback method on a match.
func (is *infoStore) processCallbacks(key string, content roachpb.Value) {
	var matches []Callback
	for _, cb := range is.callbacks {
		if cb.matcher.MatchString(key) {
			matches = append(matches, cb.method)
		}
	}
	is.runCallbacks(key, content, matches...)
}

func (is *infoStore) runCallbacks(key string, content roachpb.Value, callbacks ...Callback) {
	// Add the callbacks to the callback work list.
	f := func() {
		for _, method := range callbacks {
			method(key, content)
		}
	}
	is.callbackWorkMu.Lock()
	is.callbackWork = append(is.callbackWork, f)
	is.callbackWorkMu.Unlock()

	// Run callbacks in a goroutine to avoid mutex reentry. We also guarantee
	// callbacks are run in order such that if a key is updated twice in
	// succession, the second callback will never be run before the first.
	is.stopper.RunAsyncTask(func() {
		// Grab the callback mutex to serialize execution of the callbacks.
		is.callbackMu.Lock()
		defer is.callbackMu.Unlock()

		// Grab and execute the list of work.
		is.callbackWorkMu.Lock()
		work := is.callbackWork
		is.callbackWork = nil
		is.callbackWorkMu.Unlock()

		for _, w := range work {
			w()
		}
	})
}

// visitInfos implements a visitor pattern to run the visitInfo
// function against each info in turn. Be sure to skip over any expired
// infos.
func (is *infoStore) visitInfos(visitInfo func(string, *Info) error) error {
	now := timeutil.Now().UnixNano()

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
// All hop distances on infos are incremented to indicate they've
// arrived from an external source. Returns the count of "fresh"
// infos in the provided delta.
func (is *infoStore) combine(infos map[string]*Info, nodeID roachpb.NodeID) (freshCount int, err error) {
	for key, i := range infos {
		infoCopy := *i
		infoCopy.Hops++
		infoCopy.PeerID = nodeID
		// Errors from addInfo here are not a problem; they simply
		// indicate that the data in *is is newer than in *delta.
		if infoCopy.OrigStamp == 0 {
			panic(util.Errorf("combining info from node %d with 0 original timestamp", nodeID))
		}
		if addErr := is.addInfo(key, &infoCopy); addErr == nil {
			freshCount++
		} else if addErr != errNotFresh {
			err = addErr
		}
	}
	return
}

// delta returns a map of infos which have originating timestamps
// newer than the high water timestamps indicated by the supplied
// map (which is taken from the perspective of the peer node we're
// taking this delta for).
func (is *infoStore) delta(highWaterTimestamps map[roachpb.NodeID]int64) map[string]*Info {
	infos := make(map[string]*Info)
	// Compute delta of infos.
	if err := is.visitInfos(func(key string, i *Info) error {
		if i.isFresh(highWaterTimestamps[i.NodeID]) {
			infos[key] = i
		}
		return nil
	}); err != nil {
		panic(err)
	}

	return infos
}

// mostDistant returns the most distant gossip node known to the
// store as well as the number of hops to reach it.
func (is *infoStore) mostDistant() (roachpb.NodeID, uint32) {
	var nodeID roachpb.NodeID
	var maxHops uint32
	if err := is.visitInfos(func(key string, i *Info) error {
		if i.Hops > maxHops {
			maxHops = i.Hops
			nodeID = i.NodeID
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return nodeID, maxHops
}

// leastUseful determines which node ID from amongst the set is
// currently contributing the least. Returns the node ID. If nodes is
// empty, returns 0.
func (is *infoStore) leastUseful(nodes nodeSet) roachpb.NodeID {
	contrib := make(map[roachpb.NodeID]map[roachpb.NodeID]struct{}, nodes.len())
	for node := range nodes.nodes {
		contrib[node] = map[roachpb.NodeID]struct{}{}
	}
	if err := is.visitInfos(func(key string, i *Info) error {
		if _, ok := contrib[i.PeerID]; !ok {
			contrib[i.PeerID] = map[roachpb.NodeID]struct{}{}
		}
		contrib[i.PeerID][i.NodeID] = struct{}{}
		return nil
	}); err != nil {
		panic(err)
	}

	least := math.MaxInt32
	var leastNode roachpb.NodeID
	for id, m := range contrib {
		count := len(m)
		if nodes.hasNode(id) {
			if count < least {
				least = count
				leastNode = id
			}
		}
	}
	return leastNode
}
