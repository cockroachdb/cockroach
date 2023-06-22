// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package liveness

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type UpdateInfo struct {
	lastUpdateTime      hlc.Timestamp
	lastUnavailableTime hlc.Timestamp
}

// cache stores updates to both Liveness records and the store descriptor map.
// It doesn't store the entire StoreDescriptor, only the time when it is
// updated. The StoreDescriptor is sent directly from nodes so doesn't require
// the liveness leaseholder to be available.
// TODO(baptist): Currently liveness does not take into account the store
// descriptor timestamps. Once all code has changed over to not directly
// checking liveness on the liveness record, then the isLive method should
// change to take this into account. Only epoch leases will use the liveness
// timestamp directly.
type cache struct {
	gossip                *gossip.Gossip
	clock                 *hlc.Clock
	notifyLivenessChanged func(old, new livenesspb.Liveness)
	mu                    struct {
		syncutil.RWMutex
		// lastNodeUpdate stores timestamps of StoreDescriptor updates in Gossip.
		// This is tracking based on NodeID, so any store that is updated on this
		// node will update teh lastNodeUpdate. We don't have the ability to handle
		// "1 stalled store" on a node from a liveness perspective.
		lastNodeUpdate map[roachpb.NodeID]UpdateInfo
		// nodes stores liveness records read from Gossip
		nodes map[roachpb.NodeID]Record
	}
}

func newCache(
	g *gossip.Gossip, clock *hlc.Clock, cbFn func(livenesspb.Liveness, livenesspb.Liveness),
) *cache {
	c := cache{}
	c.gossip = g
	c.clock = clock
	c.mu.nodes = make(map[roachpb.NodeID]Record)
	c.mu.lastNodeUpdate = make(map[roachpb.NodeID]UpdateInfo)

	c.notifyLivenessChanged = cbFn

	// Gossip is nil in some tests.
	if c.gossip != nil {
		// NB: we should consider moving this registration to .Start() once we
		// have ensured that nobody uses the server's KV client (kv.DB) before
		// nl.Start() is invoked. At the time of writing this invariant does
		// not hold (which is a problem, since the node itself won't be live
		// at this point, and requests routed to it will hang).
		livenessRegex := gossip.MakePrefixPattern(gossip.KeyNodeLivenessPrefix)
		c.gossip.RegisterCallback(livenessRegex, c.livenessGossipUpdate)

		// Enable redundant callbacks for the store keys because we use these
		// callbacks as a clock to determine when a store was last updated even if it
		// hasn't otherwise changed.
		storeRegex := gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix)
		c.gossip.RegisterCallback(storeRegex, c.storeGossipUpdate, gossip.Redundant)
	}
	return &c
}

// selfID returns the ID for this node according to Gossip. This will be 0
// until the node has joined the cluster.
func (c *cache) selfID() roachpb.NodeID {
	return c.gossip.NodeID.Get()
}

// livenessGossipUpdate is the gossip callback used to keep the
// in-memory liveness info up to date.
func (c *cache) livenessGossipUpdate(_ string, content roachpb.Value) {
	ctx := context.TODO()
	var liveness livenesspb.Liveness
	if err := content.GetProto(&liveness); err != nil {
		log.Errorf(ctx, "%v", err)
		return
	}

	c.maybeUpdate(ctx, Record{Liveness: liveness, raw: content.TagAndDataBytes()})
}

// storeGossipUpdate is the Gossip callback used to keep the nodeDescMap up to date.
func (c *cache) storeGossipUpdate(_ string, content roachpb.Value) {
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
func (c *cache) maybeUpdate(ctx context.Context, newLivenessRec Record) {
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
	c.mu.Unlock()

	if shouldReplace {
		c.notifyLivenessChanged(oldLivenessRec.Liveness, newLivenessRec.Liveness)
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

// Self returns the raw, encoded value that the database has for this liveness
// record in addition to the decoded liveness proto.
func (c *cache) Self() (_ Record, ok bool) {
	return c.GetLiveness(c.selfID())
}

// GetLiveness returns the liveness record for the specified nodeID. If the
// liveness record is not found (due to gossip propagation delays or due to the
// node not existing), we surface that to the caller. The record returned also
// includes the raw, encoded value that the database has for this liveness
// record in addition to the decoded liveness proto.
func (c *cache) GetLiveness(nodeID roachpb.NodeID) (_ Record, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if l, ok := c.mu.nodes[nodeID]; ok {
		return l, true
	}
	return Record{}, false
}

// getAllLivenessEntries returns a copy of all the entries currently in the
// liveness cache. Most places should avoid calling this method and instead just
// get the entry they need. In a few places in the code it is more efficient to
// get the entries once and keep the map in memory for later iteration.
func (c *cache) getAllLivenessEntries() []livenesspb.Liveness {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cpy := make([]livenesspb.Liveness, 0, len(c.mu.nodes))
	for _, l := range c.mu.nodes {
		cpy = append(cpy, l.Liveness)
	}
	return cpy
}

// LastDescriptorUpdate returns when this node last had an update.
func (c *cache) LastDescriptorUpdate(nodeID roachpb.NodeID) (UpdateInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if l, ok := c.mu.lastNodeUpdate[nodeID]; ok {
		return l, true
	}
	// If there is no timestamp, use the "0" timestamp.
	return UpdateInfo{}, false
}

// CheckForStaleEntries checks if any of the cached node updates have not been
// updated for longer than the interval. If they become stale, they remain stale
// for the suspect interval to prevent flapping nodes from impacting system
// stability.
func (c *cache) CheckForStaleEntries(interval time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	for _, l := range c.mu.lastNodeUpdate {
		if l.lastUpdateTime.AddDuration(interval).Less(now) {
			l.lastUnavailableTime = now
		}
	}
}
