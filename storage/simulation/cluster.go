// Copyright 2015 The Cockroach Authors.
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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/gossiputil"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/stop"
)

// nodeIDSlice implements sort.Interface.
type nodeIDSlice []roachpb.NodeID

func (n nodeIDSlice) Len() int           { return len(n) }
func (n nodeIDSlice) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n nodeIDSlice) Less(i, j int) bool { return n[i] < n[j] }

// Cluster maintains a list of all nodes, stores and ranges as well as any
// shared resources.
type Cluster struct {
	stopper         *stop.Stopper
	clock           *hlc.Clock
	rpc             *rpc.Context
	gossip          *gossip.Gossip
	storePool       *storage.StorePool
	allocator       storage.Allocator
	storeGossiper   *gossiputil.StoreGossiper
	nodes           map[roachpb.NodeID]*Node
	stores          map[roachpb.StoreID]*Store
	storeIDs        roachpb.StoreIDSlice // sorted
	ranges          map[roachpb.RangeID]*Range
	rangeIDs        roachpb.RangeIDSlice // sorted
	rangeIDsByStore map[roachpb.StoreID]roachpb.RangeIDSlice
	rand            *rand.Rand
	epoch           int
	epochWriter     *tabwriter.Writer
	actionWriter    *tabwriter.Writer
	script          Script
}

// createCluster generates a new cluster using the provided stopper and the
// number of nodes supplied. Each node will have one store to start.
func createCluster(
	stopper *stop.Stopper,
	nodeCount int,
	epochWriter, actionWriter io.Writer,
	script Script,
	rand *rand.Rand,
) *Cluster {
	clock := hlc.NewClock(hlc.UnixNano)
	rpcContext := rpc.NewContext(nil, clock, stopper)
	g := gossip.New(rpcContext, nil, stopper)
	// NodeID is required for Gossip, so set it to -1 for the cluster Gossip
	// instance to prevent conflicts with real NodeIDs.
	g.SetNodeID(-1)
	storePool := storage.NewStorePool(g, clock, storage.TestTimeUntilStoreDeadOff, stopper)
	c := &Cluster{
		stopper:   stopper,
		clock:     clock,
		rpc:       rpcContext,
		gossip:    g,
		storePool: storePool,
		allocator: storage.MakeAllocator(storePool, storage.AllocatorOptions{
			AllowRebalance: true,
			Deterministic:  true,
		}),
		storeGossiper:   gossiputil.NewStoreGossiper(g),
		nodes:           make(map[roachpb.NodeID]*Node),
		stores:          make(map[roachpb.StoreID]*Store),
		ranges:          make(map[roachpb.RangeID]*Range),
		rangeIDsByStore: make(map[roachpb.StoreID]roachpb.RangeIDSlice),
		rand:            rand,
		epochWriter:     tabwriter.NewWriter(epochWriter, 8, 1, 2, ' ', 0),
		actionWriter:    tabwriter.NewWriter(actionWriter, 8, 1, 2, ' ', 0),
		script:          script,
		epoch:           -1,
	}

	// Add the nodes.
	for i := 0; i < nodeCount; i++ {
		c.addNewNodeWithStore()
	}

	// Add a single range and add to this first node's first store.
	firstRange := c.addRange()
	firstRange.addReplica(c.stores[0])

	c.calculateRangeIDsByStore()

	// Output the first epoch header.
	c.epoch = 0
	c.OutputEpochHeader()
	c.OutputEpoch()
	c.flush()

	return c
}

// addNewNodeWithStore adds new node with a single store.
func (c *Cluster) addNewNodeWithStore() {
	nodeID := roachpb.NodeID(len(c.nodes))
	c.nodes[nodeID] = newNode(nodeID, c.gossip)
	// Only output if we're running the simulation.
	if c.epoch >= 0 {
		fmt.Fprintf(c.actionWriter, "%d:\tNode %d added\n", c.epoch, nodeID)
	}
	c.addStore(nodeID)
}

// addStore adds a new store to the node with the provided nodeID.
func (c *Cluster) addStore(nodeID roachpb.NodeID) *Store {
	n := c.nodes[nodeID]
	s := n.addNewStore()
	c.stores[s.desc.StoreID] = s

	// Save a sorted array of store IDs to avoid having to calculate them
	// multiple times.
	c.storeIDs = append(c.storeIDs, s.desc.StoreID)
	sort.Sort(c.storeIDs)

	// Only output if we're running the simulation.
	if c.epoch >= 0 {
		fmt.Fprintf(c.actionWriter, "%d:\tStore %d added on Node %d\n", c.epoch, s.desc.StoreID, nodeID)
		c.OutputEpochHeader()
	}
	return s
}

// addRange adds a new range to the cluster but does not attach it to any
// store.
func (c *Cluster) addRange() *Range {
	rangeID := roachpb.RangeID(len(c.ranges))
	newRng := newRange(rangeID, c.allocator)
	c.ranges[rangeID] = newRng

	// Save a sorted array of range IDs to avoid having to calculate them
	// multiple times.
	c.rangeIDs = append(c.rangeIDs, rangeID)
	sort.Sort(c.rangeIDs)

	return newRng
}

// splitRangeRandom splits a random range from within the cluster.
func (c *Cluster) splitRangeRandom() {
	rangeID := roachpb.RangeID(c.rand.Int63n(int64(len(c.ranges))))
	c.splitRange(rangeID)
}

// splitRange "splits" a range. This split creates a new range with new
// replicas on the same stores as the passed in range. The new range has the
// same zone config as the original range.
func (c *Cluster) splitRange(rangeID roachpb.RangeID) {
	fmt.Fprintf(c.actionWriter, "%d:\tRange %d split\n", c.epoch, rangeID)
	newRange := c.addRange()
	originalRange := c.ranges[rangeID]
	newRange.splitRange(originalRange)
}

// runEpoch steps through a single instance of the simulator. Returns true if
// the last epoch has occurred.
// During every epoch the following steps occur:
// 1) All scripted cluster actions are executed.
// 2) The status of every store is gossiped so the store pool is up to date.
// 3) All replicas call the allocator to determine if there are any possible
//    actions for it to perform.
// 4) The replica on each range with the highest priority executes its action.
// 5) The rangesByStore map is recalculated.
// 6) The current status of the cluster is output.
// 7) The outputs are flushed.
func (c *Cluster) runEpoch() bool {
	c.epoch++

	// Perform all the requested scripted actions.
	lastEpoch := c.executeScriptedActions()

	// Gossip all the store updates.
	c.gossipStores()

	// Determine next operations for all ranges. The reason for doing this as
	// a distinct step from execution, is to have each range consider its
	// situation as it currently stands at each epoch.
	c.prepareActions()

	// Execute the determined operations.
	// TODO(bram): #4566 Add ability to end when stable.
	c.performActions()

	// Recalculate the ranges IDs by store map.
	c.calculateRangeIDsByStore()

	// Output the update.
	c.OutputEpoch()

	// Flush the output.
	c.flush()

	return lastEpoch
}

// executeScriptedActions performs all scripted actions to the cluster and
// return true if the exit action is encountered.
func (c *Cluster) executeScriptedActions() bool {
	var lastEpoch bool
	actions := c.script.getActions(c.epoch)
	for _, action := range actions {
		switch action.operation {
		case OpExit:
			{
				fmt.Fprintf(c.actionWriter, "%d:\tAction:Exit - this will be the last epoch.\n", c.epoch)
				lastEpoch = true
			}
		case OpSplitRange:
			{
				switch action.variant {
				case OpVarValue:
					fmt.Fprintf(c.actionWriter, "%d:\tAction:SplitRange - splitting the range %d.\n", c.epoch, action.value)
					c.splitRange(roachpb.RangeID(action.value))
				case OpVarRandom:
					fmt.Fprintf(c.actionWriter, "%d:\tAction:SplitRange - splitting a random range.\n", c.epoch)
					c.splitRangeRandom()
				case OpVarFirst:
					fmt.Fprintf(c.actionWriter, "%d:\tAction:SplitRange - splitting the first range.\n", c.epoch)
					c.splitRange(c.rangeIDs[0])
				case OpVarLast:
					fmt.Fprintf(c.actionWriter, "%d:\tAction:SplitRange - splitting the last range.\n", c.epoch)
					c.splitRange(c.rangeIDs[len(c.rangeIDs)-1])
				}
			}
		case OpAddNode:
			{
				fmt.Fprintf(c.actionWriter, "%d:\tAction:AddNode - Adding a new node with a new store.\n", c.epoch)
				c.addNewNodeWithStore()
			}
		}
	}
	return lastEpoch
}

// gossipStores gossips all the most recent status for all stores.
func (c *Cluster) gossipStores() {
	c.storeGossiper.GossipWithFunction(c.storeIDs, func() {
		for storeID, store := range c.stores {
			if err := store.gossipStore(len(c.rangeIDsByStore[storeID])); err != nil {
				fmt.Fprintf(c.actionWriter, "%d:\tError gossiping store %d: %s\n", c.epoch, storeID, err)
			}
		}
	})
}

// prepareActions walks through each replica and determines if any action is
// required using the allocator.
func (c *Cluster) prepareActions() {
	for _, r := range c.ranges {
		for storeID, rep := range r.replicas {
			rep.action, rep.priority = r.allocator.ComputeAction(r.zone, &r.desc)
			if rep.action == storage.AllocatorNoop {
				rep.rebalance = r.allocator.ShouldRebalance(storeID)
				// Set the priority to 1 so that rebalances will occur in
				// performActions.
				rep.priority = 1
			} else {
				rep.rebalance = false
			}
			r.replicas[storeID] = rep
		}
	}
}

// performActions performs a single action, if required, for each range. Returns
// true if no actions were performed.
func (c *Cluster) performActions() bool {
	// Once a store has started performing an action on a range, it "locks" the
	// range and any subsequent store that tries to perform another action
	// on the range will encounter a conflict and forfeit its action for the
	// epoch. This is designed to be similar to what will occur when two or
	// more stores try to perform actions against the same range descriptor. In
	// our case, the first store numerically will always be the one that
	// succeeds. In a real cluster, the transaction with the higher
	// transactional priority will succeed and the others will abort.
	usedRanges := make(map[roachpb.RangeID]roachpb.StoreID)
	stable := true
	// Each store can perform a single action per epoch.
	for _, storeID := range c.storeIDs {
		// Find the range with the highest priority action for the replica on
		// the store.
		var topRangeID roachpb.RangeID
		var topReplica replica
		for _, rangeID := range c.rangeIDsByStore[storeID] {
			rep := c.ranges[rangeID].replicas[storeID]
			if rep.priority > topReplica.priority {
				topRangeID = rangeID
				topReplica = rep
			}
		}

		if conflictStoreID, ok := usedRanges[topRangeID]; ok {
			switch topReplica.action {
			case storage.AllocatorAdd:
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tADD:conflict:%d\n",
					c.epoch, storeID, topRangeID, conflictStoreID)
			case storage.AllocatorRemove:
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREMOVE:conflict:%d\n",
					c.epoch, storeID, topRangeID, conflictStoreID)
			case storage.AllocatorRemoveDead:
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREPAIR:conflict:%d\n",
					c.epoch, storeID, topRangeID, conflictStoreID)
			case storage.AllocatorNoop:
				if topReplica.rebalance {
					fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREBALANCE:conflict:%d\n",
						c.epoch, storeID, topRangeID, conflictStoreID)
				}
			}
		} else {
			r := c.ranges[topRangeID]
			switch topReplica.action {
			case storage.AllocatorAdd:
				stable = false
				newStoreID, err := r.getAllocateTarget()
				if err != nil {
					fmt.Fprintf(c.actionWriter, "%d:\tError: %s\n", c.epoch, err)
					continue
				}
				r.addReplica(c.stores[newStoreID])
				usedRanges[topRangeID] = storeID
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tADD:%d\n",
					c.epoch, storeID, topRangeID, newStoreID)
			case storage.AllocatorRemoveDead:
				stable = false
				// TODO(bram): #4566 implement this.
				usedRanges[topRangeID] = storeID
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREPAIR\n", c.epoch, storeID, topRangeID)
			case storage.AllocatorRemove:
				stable = false
				removeStoreID, err := r.getRemoveTarget()
				if err != nil {
					fmt.Fprintf(c.actionWriter, "%d:\tError: %s\n", c.epoch, err)
					continue
				}
				r.removeReplica(c.stores[removeStoreID])
				usedRanges[topRangeID] = storeID
				fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREMOVE:%d\n",
					c.epoch, storeID, topRangeID, removeStoreID)
			case storage.AllocatorNoop:
				if topReplica.rebalance {
					rebalanceStoreID, found := r.getRebalanceTarget(topReplica.store.desc.StoreID)
					if !found {
						continue
					}
					stable = false
					r.addReplica(c.stores[rebalanceStoreID])
					usedRanges[topRangeID] = storeID
					fmt.Fprintf(c.actionWriter, "%d:\tStore:%d\tRange:%d\tREBALANCE:%d\n",
						c.epoch, storeID, topRangeID, rebalanceStoreID)
				}
			}
		}
	}
	return stable
}

// calculateRangeIDsByStore fills in the list of range ids mapped to each
// store. This map is used for determining which operation to run for each store
// and outputs.  It should only be run once at the end of each epoch.
func (c *Cluster) calculateRangeIDsByStore() {
	c.rangeIDsByStore = make(map[roachpb.StoreID]roachpb.RangeIDSlice)
	for rangeID, r := range c.ranges {
		for _, storeID := range r.getStoreIDs() {
			c.rangeIDsByStore[storeID] = append(c.rangeIDsByStore[storeID], rangeID)
		}
	}
	for storeID := range c.rangeIDsByStore {
		sort.Sort(c.rangeIDsByStore[storeID])
	}
}

// String prints out the current status of the cluster.
func (c *Cluster) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "Cluster Info:\tEpoch - %d\n", c.epoch)

	var nodeIDs nodeIDSlice
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Sort(nodeIDs)

	buf.WriteString("Node Info:\n")
	for _, nodeID := range nodeIDs {
		n := c.nodes[nodeID]
		buf.WriteString(n.String())
		buf.WriteString("\n")
	}

	buf.WriteString("Store Info:\n")
	for _, storeID := range c.storeIDs {
		s := c.stores[storeID]
		buf.WriteString(s.String(len(c.rangeIDsByStore[storeID])))
		buf.WriteString("\n")
	}

	var rangeIDs roachpb.RangeIDSlice
	for rangeID := range c.ranges {
		rangeIDs = append(rangeIDs, rangeID)
	}
	sort.Sort(rangeIDs)

	buf.WriteString("Range Info:\n")
	for _, rangeID := range rangeIDs {
		r := c.ranges[rangeID]
		fmt.Fprintf(&buf, "%s\n", r)
	}

	return buf.String()
}

// OutputEpochHeader outputs to the epoch writer the header for epoch outputs
// based on all of the current stores.
func (c *Cluster) OutputEpochHeader() {
	fmt.Fprintf(c.epochWriter, "Store:\t")
	for _, storeID := range c.storeIDs {
		fmt.Fprintf(c.epochWriter, "%d\t", storeID)
	}
	fmt.Fprintf(c.epochWriter, "\n")
}

// OutputEpoch writes to the epochWRiter the current free capacity for all
// stores.
func (c *Cluster) OutputEpoch() {
	fmt.Fprintf(c.epochWriter, "%d:\t", c.epoch)

	for _, storeID := range c.storeIDs {
		store := c.stores[roachpb.StoreID(storeID)]
		capacity := store.getCapacity(len(c.rangeIDsByStore[storeID]))
		fmt.Fprintf(c.epochWriter, "%d/%.0f%%\t", len(c.rangeIDsByStore[storeID]), float64(capacity.Available)/float64(capacity.Capacity)*100)
	}
	fmt.Fprintf(c.epochWriter, "\n")
}

func (c *Cluster) flush() {
	_ = c.actionWriter.Flush()
	_ = c.epochWriter.Flush()
}
