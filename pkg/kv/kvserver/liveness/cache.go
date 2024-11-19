// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type UpdateInfo struct {
	lastUpdateTime      hlc.Timestamp
	lastUnavailableTime hlc.Timestamp
}

// Gossip is the subset of *gossip.Gossip used by liveness.
type Gossip interface {
	RegisterCallback(pattern string, method gossip.Callback, opts ...gossip.CallbackOption) func()
	GetNodeID() roachpb.NodeID
}

var livenessRegex = gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
var storeRegex = gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix)

// Cache stores updates to both Liveness records and the store descriptor map.
// It doesn't store the entire StoreDescriptor, only the time when it is
// updated. The StoreDescriptor is sent directly from nodes so doesn't require
// the liveness leaseholder to be available.
// TODO(baptist): Currently liveness does not take into account the store
// descriptor timestamps. Once all code has changed over to not directly
// checking liveness on the liveness record, then the isLive method should
// change to take this into account. Only epoch leases will use the liveness
// timestamp directly.
type Cache struct {
	gossip     Gossip
	clock      *hlc.Clock
	nodeDialer *nodedialer.Dialer
	st         *cluster.Settings
	mu         struct {
		syncutil.RWMutex
		// lastNodeUpdate stores timestamps of StoreDescriptor updates in Gossip.
		// This is tracking based on NodeID, so any store that is updated on this
		// node will update teh lastNodeUpdate. We don't have the ability to handle
		// "1 stalled store" on a node from a liveness perspective.
		lastNodeUpdate map[roachpb.NodeID]UpdateInfo
		// nodes stores liveness records read from Gossip
		nodes                 map[roachpb.NodeID]Record
		notifyLivenessChanged func(old, new livenesspb.Liveness)
	}
}

// NewCache creates a liveness cache. The cache will receive updates from Gossip
// or from direct updates to liveness made through this node.
func NewCache(
	g Gossip, clock *hlc.Clock, st *cluster.Settings, nodeDialer *nodedialer.Dialer,
) *Cache {
	c := Cache{
		gossip:     g,
		clock:      clock,
		nodeDialer: nodeDialer,
		st:         st,
	}
	c.mu.nodes = make(map[roachpb.NodeID]Record)
	c.mu.lastNodeUpdate = make(map[roachpb.NodeID]UpdateInfo)

	// Listen for gossip updates to both liveness and store descriptors.
	// Liveness updates are only published by the liveness leaseholder, but
	// store updates are published directly by the node. Different parts of the
	// system have different criteria for what they consider alive.
	c.gossip.RegisterCallback(livenessRegex, c.livenessGossipUpdate)

	// Enable redundant callbacks for the store keys because we use these
	// callbacks as a clock to determine when a store was last updated even if
	// it hasn't otherwise changed.
	c.gossip.RegisterCallback(storeRegex, c.storeGossipUpdate, gossip.Redundant)

	return &c
}

// setLivenessChangedFn sets the liveness function after the Cache has been
// created. This prevents a circular dependency between the Cache and the
// NodeLiveness objects.
func (c *Cache) setLivenessChangedFn(cbFn func(old, new livenesspb.Liveness)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.notifyLivenessChanged = cbFn
}

// selfID returns the ID for this node according to Gossip. This will be 0
// until the node has joined the cluster.
func (c *Cache) selfID() roachpb.NodeID {
	return c.gossip.GetNodeID()
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (c *Cache) livenessGossipUpdate(_ string, content roachpb.Value) {
	ctx := context.TODO()
	var liveness livenesspb.Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Errorf(ctx, "%v", err)
		return
	}

	c.maybeUpdate(ctx, Record{Liveness: liveness, raw: content.TagAndDataBytes()})
}

// storeGossipUpdate is the Gossip callback used to keep the nodeDescMap up to date.
func (c *Cache) storeGossipUpdate(_ string, content roachpb.Value) {
	ctx := context.TODO()
	var storeDesc roachpb.StoreDescriptor
	if err := content.GetProto(&storeDesc); err != nil {
		log.Errorf(ctx, "%v", err)
		return
	}
	nodeID := storeDesc.Node.NodeID
	if nodeID == 0 {
		log.Errorf(ctx, "unexpected update for node 0, %v", storeDesc)
		return
	}
	c.mu.Lock()
	previousRec, found := c.mu.lastNodeUpdate[nodeID]
	if !found {
		previousRec = UpdateInfo{}
	}
	previousRec.lastUpdateTime = c.clock.Now()
	c.mu.lastNodeUpdate[nodeID] = previousRec
	c.mu.Unlock()
}

// maybeUpdate replaces the liveness (if it appears newer) and invokes the
// registered callbacks if the node became live in the process.
func (c *Cache) maybeUpdate(ctx context.Context, newLivenessRec Record) {
	if newLivenessRec.Liveness == (livenesspb.Liveness{}) {
		log.Fatal(ctx, "invalid new liveness record; found to be empty")
	}

	if newLivenessRec.NodeID == 0 {
		log.Fatal(ctx, "attempt to cache liveness record with nid 0")
	}

	shouldReplace := true
	c.mu.Lock()

	// NB: shouldReplace will always be true right after a node restarts since the
	// `nodes` map will be empty. This means that the callbacks called below will
	// always be invoked at least once after node restarts.
	oldLivenessRec, ok := c.mu.nodes[newLivenessRec.NodeID]
	if ok {
		shouldReplace = livenessChanged(oldLivenessRec, newLivenessRec)
	}

	if shouldReplace {
		c.mu.nodes[newLivenessRec.NodeID] = newLivenessRec
	}
	notifyFn := c.mu.notifyLivenessChanged
	c.mu.Unlock()

	if shouldReplace {
		notifyFn(oldLivenessRec.Liveness, newLivenessRec.Liveness)
	}
}

// livenessChanged checks to see if the new liveness is in fact newer
// than the old liveness.
func livenessChanged(old, new Record) bool {
	oldL, newL := old.Liveness, new.Liveness

	// Compare liveness information. If oldL < newL, replace.
	if cmp := oldL.Compare(newL); cmp != 0 {
		return cmp < 0
	}

	// If Epoch and Expiration are unchanged, assume that the update is newer
	// when its draining or decommissioning field changed.
	//
	// Similarly, assume that the update is newer if the raw encoding is changed
	// when all the fields are the same. This ensures that the CPut performed
	// by updateLivenessAttempt will eventually succeed even if the proto
	// encoding changes.
	//
	// This has false positives (in which case we're clobbering the liveness). A
	// better way to handle liveness updates in general is to add a sequence
	// number.
	//
	// See #18219.
	return oldL.Draining != newL.Draining ||
		oldL.Membership != newL.Membership ||
		(oldL.Equal(newL) && !bytes.Equal(old.raw, new.raw))
}

// self returns the raw, encoded value that the database has for this liveness
// record in addition to the decoded liveness proto.
func (c *Cache) self() (_ Record, ok bool) {
	return c.getLiveness(c.selfID())
}

// getLiveness returns the liveness record for the specified nodeID. If the
// liveness record is not found (due to gossip propagation delays or due to the
// node not existing), we surface that to the caller. The record returned also
// includes the raw, encoded value that the database has for this liveness
// record in addition to the decoded liveness proto.
func (c *Cache) getLiveness(nodeID roachpb.NodeID) (_ Record, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if l, ok := c.mu.nodes[nodeID]; ok {
		return l, true
	}
	return Record{}, false
}

// GetNodeVitality returns the vitality record for the specified nodeID. If the
// vitality record is not found (due to gossip propagation delays or due to the
// node not existing), we surface that to the caller by returning a record where
// NodeVitality.IsValid return false.
func (c *Cache) GetNodeVitality(nodeID roachpb.NodeID) livenesspb.NodeVitality {
	if l, ok := c.getLiveness(nodeID); ok {
		return c.convertToNodeVitality(l.Liveness)
	}
	// If we don't have a liveness record, we can't create a full NodeVitality.
	// This is a little unfortunate as we may have a NodeDescriptor.
	return livenesspb.NodeVitality{}
}

// convertToNodeVitality is used if you already have a Liveness record received
// externally.
func (c *Cache) convertToNodeVitality(l livenesspb.Liveness) livenesspb.NodeVitality {
	// The store is considered dead if it hasn't been updated via gossip
	// within the liveness threshold. Note that LastUpdatedTime is set
	// when the store detail is created and will have a non-zero value
	// even before the first gossip arrives for a store.

	// NB: nodeDialer is nil in some tests.
	connected := c.nodeDialer == nil || c.nodeDialer.ConnHealth(l.NodeID, rpc.SystemClass) == nil
	lastDescUpdate := c.lastDescriptorUpdate(l.NodeID)

	return l.CreateNodeVitality(
		c.clock.Now(),
		lastDescUpdate.lastUpdateTime,
		lastDescUpdate.lastUnavailableTime,
		connected,
		TimeUntilNodeDead.Get(&c.st.SV),
		TimeAfterNodeSuspect.Get(&c.st.SV),
	)
}

// getIsLiveMap returns a map of nodeID to boolean liveness status of
// each node. This excludes nodes that were removed completely (dead +
// decommissioned)
func (c *Cache) getIsLiveMap() livenesspb.IsLiveMap {
	lMap := livenesspb.IsLiveMap{}
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.clock.Now()
	for nID, l := range c.mu.nodes {
		isLive := l.IsLive(now)
		if l.Membership.Decommissioned() {
			// This is a node that was completely removed. Skip over it.
			continue
		}
		lMap[nID] = livenesspb.IsLiveMapEntry{
			Liveness: l.Liveness,
			IsLive:   isLive,
		}
	}
	return lMap
}

// getAllLivenessEntries returns a copy of all the entries currently in the
// liveness Cache. Most places should avoid calling this method and instead just
// get the entry they need. In a few places in the code it is more efficient to
// get the entries once and keep the map in memory for later iteration.
func (c *Cache) getAllLivenessEntries() []livenesspb.Liveness {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cpy := make([]livenesspb.Liveness, 0, len(c.mu.nodes))
	for _, l := range c.mu.nodes {
		cpy = append(cpy, l.Liveness)
	}
	return cpy
}

// ScanNodeVitalityFromCache returns a map of nodeID to boolean liveness status
// of each node from the cache. This excludes nodes that were decommissioned.
// Decommissioned nodes are kept in the KV store and the cache forever, but are
// typically not referenced in normal usage. The method ScanNodeVitalityFromKV
// does return decommissioned nodes.
func (c *Cache) ScanNodeVitalityFromCache() livenesspb.NodeVitalityMap {
	entries := c.getAllLivenessEntries()
	statusMap := make(map[roachpb.NodeID]livenesspb.NodeVitality, len(entries))
	for _, l := range entries {
		if l.Membership.Decommissioned() {
			// This is a node that was completely removed. Skip over it.
			continue
		}
		statusMap[l.NodeID] = c.convertToNodeVitality(l)
	}
	return statusMap
}

// lastDescriptorUpdate returns when this node last had an update.
func (c *Cache) lastDescriptorUpdate(nodeID roachpb.NodeID) UpdateInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if l, ok := c.mu.lastNodeUpdate[nodeID]; ok {
		return l
	}
	// If there is no timestamp, use the "0" timestamp.
	return UpdateInfo{}
}

// checkForStaleEntries checks if any of the cached node updates have not been
// updated for longer than the interval. If they become stale, they remain stale
// for the suspect interval to prevent flapping nodes from impacting system
// stability.
func (c *Cache) checkForStaleEntries(interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	for _, l := range c.mu.lastNodeUpdate {
		if l.lastUpdateTime.AddDuration(interval).Less(now) {
			l.lastUnavailableTime = now
		}
	}
}
