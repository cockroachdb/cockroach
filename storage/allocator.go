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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/spf13/pflag"
)

const (
	// maxFractionUsedThreshold: if the fraction used of a store descriptor
	// capacity is greater than this value, it will never be used as a rebalance
	// target and it will always be eligible to rebalance replicas to other
	// stores.
	maxFractionUsedThreshold = 0.95
	// minFractionUsedThreshold: if the mean fraction used of a list of store
	// descriptors is less than this, then range count will be used to make
	// rebalancing decisions instead of the fraction of bytes used. This is
	// useful for distributing load evenly on nascent deployments.
	minFractionUsedThreshold = 0.02
	// rebalanceFromMean is used to declare a range above and below the average
	// used capacity of the cluster. If a store's usage is below this range, it
	// is a rebalancing target and can accept new replicas; if usage is above
	// this range, the store is eligible to rebalance replicas to other stores.
	rebalanceFromMean = 0.025 // 2.5%
	// rebalanceShouldRebalanceChance represents a chance that an individual
	// replica should attempt to rebalance. This helps introduce some
	// probabilistic "jitter" to shouldRebalance() function: the store will not
	// take every rebalancing opportunity available.
	rebalanceShouldRebalanceChance = 0.2

	// priorities for various repair operations.
	removeDeadReplicaPriority  float64 = 10000
	addMissingReplicaPriority  float64 = 1000
	removeExtraReplicaPriority float64 = 100
)

// AllocatorAction enumerates the various replication adjustments that may be
// recommended by the allocator.
type AllocatorAction int

// These are the possible allocator actions.
const (
	_ AllocatorAction = iota
	AllocatorNoop
	AllocatorRemove
	AllocatorAdd
	AllocatorRemoveDead
)

// A BalanceMode is a configurable mode which effects how the allocator makes
// rebalancing decisions.
type BalanceMode int

const (
	// BalanceModeUsage balances ranges between stores by primarily
	// considering disk space usage, but also considers range counts in nascent
	// clusters.
	BalanceModeUsage BalanceMode = iota
	// BalanceModeRangeCount balances ranges by considering the total range
	// count of each node.
	BalanceModeRangeCount
)

// balanceModeLookup is used to map BalanceMode values to strings, used for
// accepting command line options.
var balanceModeLookup = [...]string{
	BalanceModeUsage:      "usage",
	BalanceModeRangeCount: "rangecount",
}

// String is needed to implement the pflag.Value interface, allowing this to be
// set from the command line.
func (r *BalanceMode) String() string {
	idx := int(*r)
	if idx < 0 || idx >= len(balanceModeLookup) {
		return strconv.Itoa(idx)
	}
	return balanceModeLookup[idx]
}

// Set configures the given BalanceMode from a string provided from the
// command line. It returns an error if the provided string value is not
// recognized. Needed to implement pflag.Value.
func (r *BalanceMode) Set(value string) error {
	for i, s := range balanceModeLookup {
		if value == s {
			*r = BalanceMode(i)
			return nil
		}
	}
	return fmt.Errorf("%s is not a valid balance mode", value)
}

// Type is needed by the pflag.Value interface.
func (r *BalanceMode) Type() string {
	return "string"
}

var _ pflag.Value = new(BalanceMode)

// AllocatorOptions are configurable options which effect the way that the
// replicate queue will handle rebalancing opportunities.
type AllocatorOptions struct {
	// AllowRebalance allows this store to attempt to rebalance its own
	// replicas to other stores.
	AllowRebalance bool

	// Mode determines the strategy that will be used to locate stores for
	// allocation decisions in a way that balances load across the cluster.
	Mode BalanceMode

	// Deterministic makes allocation decisions deterministic, based on
	// current cluster statistics. If this flag is not set, allocation operations
	// will have random behavior. This flag is intended to be set for testing
	// purposes only.
	Deterministic bool
}

// Allocator makes allocation decisions based on available capacity
// in other stores which match the required attributes for a desired
// range replica.
//
// When choosing a new allocation target, three candidates from
// available stores meeting a max fraction of bytes used threshold
// (maxFractionUsedThreshold) are chosen at random and the least
// loaded of the three is selected in order to bias loading towards a
// more balanced cluster, while still spreading load over all
// available servers. "Load" is defined according to fraction of bytes
// used, if greater than minFractionUsedThreshold; otherwise it's
// defined according to range count.
//
// When choosing a rebalance target, a random store is selected from
// amongst the set of stores with fraction of bytes within
// rebalanceFromMean from the mean.
type Allocator struct {
	storePool *StorePool
	randGen   *rand.Rand
	options   AllocatorOptions
	balancer  balancer
}

// MakeAllocator creates a new allocator using the specified StorePool.
func MakeAllocator(storePool *StorePool, options AllocatorOptions) Allocator {
	var randSource rand.Source
	if options.Deterministic {
		randSource = rand.NewSource(777)
	} else {
		randSource = rand.NewSource(rand.Int63())
	}
	randGen := rand.New(randSource)
	a := Allocator{
		storePool: storePool,
		randGen:   randGen,
		options:   options,
	}

	// Instantiate balancer based on provided options.
	switch options.Mode {
	case BalanceModeUsage:
		a.balancer = usageBalancer{randGen}
	case BalanceModeRangeCount:
		a.balancer = rangeCountBalancer{randGen}
	default:
		panic(fmt.Sprintf("AllocatorOptions specified invalid BalanceMode %s", options.Mode))
	}

	return a
}

// ComputeAction determines the exact operation needed to repair the supplied
// range, as governed by the supplied zone configuration. It returns the
// required action that should be taken and a replica on which the action should
// be performed.
func (a *Allocator) ComputeAction(zone config.ZoneConfig, desc *roachpb.RangeDescriptor) (
	AllocatorAction, float64) {
	deadReplicas := a.storePool.deadReplicas(desc.Replicas)
	if len(deadReplicas) > 0 {
		// The range has dead replicas, which should be removed immediately.
		// Adjust the priority by the number of dead replicas the range has.
		quorum := computeQuorum(len(desc.Replicas))
		liveReplicas := len(desc.Replicas) - len(deadReplicas)
		return AllocatorRemoveDead, removeDeadReplicaPriority + float64(quorum-liveReplicas)
	}

	// TODO(mrtracy): Handle non-homogenous and mismatched attribute sets.
	need := len(zone.ReplicaAttrs)
	have := len(desc.Replicas)
	if have < need {
		// Range is under-replicated, and should add an additional replica.
		// Priority is adjusted by the difference between the current replica
		// count and the quorum of the desired replica count.
		neededQuorum := computeQuorum(need)
		return AllocatorAdd, addMissingReplicaPriority + float64(neededQuorum-have)
	}
	if have > need {
		// Range is over-replicated, and should remove a replica.
		// Ranges with an even number of replicas get extra priority because
		// they have a more fragile quorum.
		return AllocatorRemove, removeExtraReplicaPriority - float64(have%2)
	}

	// Nothing to do.
	return AllocatorNoop, 0
}

// AllocateTarget returns a suitable store for a new allocation with the
// required attributes. Nodes already accommodating existing replicas are ruled
// out as targets. If relaxConstraints is true, then the required attributes
// will be relaxed as necessary, from least specific to most specific, in order
// to allocate a target. If needed, a filter function can be added that further
// filter the results. The function will be passed the storeDesc and the used
// and new counts. It returns a bool indicating inclusion or exclusion from the
// set of stores being considered.
func (a *Allocator) AllocateTarget(required roachpb.Attributes, existing []roachpb.ReplicaDescriptor, relaxConstraints bool,
	filter func(storeDesc *roachpb.StoreDescriptor, count, used *stat) bool) (*roachpb.StoreDescriptor, error) {
	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}

	// Because more redundancy is better than less, if relaxConstraints, the
	// matching here is lenient, and tries to find a target by relaxing an
	// attribute constraint, from last attribute to first.
	for attrs := append([]string(nil), required.Attrs...); ; attrs = attrs[:len(attrs)-1] {
		sl := a.storePool.getStoreList(roachpb.Attributes{Attrs: attrs}, a.options.Deterministic)
		if target := a.balancer.selectGood(sl, existingNodes); target != nil {
			return target, nil
		}
		if len(attrs) == 0 {
			return nil, util.Errorf("unable to allocate a target store; no candidates available")
		} else if !relaxConstraints {
			return nil, util.Errorf("unable to allocate a target store; no candidates available with attributes %s", required)
		}
	}
}

// RemoveTarget returns a suitable replica to remove from the provided replica
// set. It attempts to consider which of the provided replicas would be the best
// candidate for removal.
//
// TODO(mrtracy): removeTarget eventually needs to accept the attributes from
// the zone config associated with the provided replicas. This will allow it to
// make correct decisions in the case of ranges with heterogeneous replica
// requirements (i.e. multiple data centers).
func (a Allocator) RemoveTarget(existing []roachpb.ReplicaDescriptor) (roachpb.ReplicaDescriptor, error) {
	if len(existing) == 0 {
		return roachpb.ReplicaDescriptor{}, util.Errorf("must supply at least one replica to allocator.RemoveTarget()")
	}

	// Retrieve store descriptors for the provided replicas from the StorePool.
	sl := StoreList{}
	for i := range existing {
		desc := a.storePool.getStoreDescriptor(existing[i].StoreID)
		if desc == nil {
			continue
		}
		sl.add(desc)
	}

	if bad := a.balancer.selectBad(sl); bad != nil {
		for i := range existing {
			if existing[i].StoreID == bad.StoreID {
				return existing[i], nil
			}
		}
	}
	return roachpb.ReplicaDescriptor{}, util.Errorf("RemoveTarget() could not select an appropriate replica to be remove")
}

// RebalanceTarget returns a suitable store for a rebalance target
// with required attributes. Rebalance targets are selected via the
// same mechanism as AllocateTarget(), except the chosen target must
// follow some additional criteria. Namely, if chosen, it must further
// the goal of balancing the cluster.
//
// The supplied parameters are the StoreID of the replica being rebalanced, the
// required attributes for the replica being rebalanced, and a list of the
// existing replicas of the range (which must include the replica being
// rebalanced).
//
// Simply ignoring a rebalance opportunity in the event that the
// target chosen by AllocateTarget() doesn't fit balancing criteria
// is perfectly fine, as other stores in the cluster will also be
// doing their probabilistic best to rebalance. This helps prevent
// a stampeding herd targeting an abnormally under-utilized store.
func (a Allocator) RebalanceTarget(storeID roachpb.StoreID, required roachpb.Attributes, existing []roachpb.ReplicaDescriptor) *roachpb.StoreDescriptor {
	if !a.options.AllowRebalance {
		return nil
	}
	existingNodes := make(nodeIDSet, len(existing))
	for _, repl := range existing {
		existingNodes[repl.NodeID] = struct{}{}
	}
	storeDesc := a.storePool.getStoreDescriptor(storeID)
	sl := a.storePool.getStoreList(required, a.options.Deterministic)
	if replacement := a.balancer.improve(storeDesc, sl, existingNodes); replacement != nil {
		return replacement
	}
	return nil
}

// ShouldRebalance returns whether the specified store should attempt to
// rebalance a replica to another store.
func (a Allocator) ShouldRebalance(storeID roachpb.StoreID) bool {
	if !a.options.AllowRebalance {
		return false
	}
	// In production, add some random jitter to shouldRebalance.
	if !a.options.Deterministic && a.randGen.Float32() > rebalanceShouldRebalanceChance {
		return false
	}
	if log.V(2) {
		log.Infof("ShouldRebalance from store %d", storeID)
	}
	storeDesc := a.storePool.getStoreDescriptor(storeID)
	if storeDesc == nil {
		if log.V(2) {
			log.Warningf(
				"ShouldRebalance couldn't find store with id %d in StorePool",
				storeID)
		}
		return false
	}

	sl := a.storePool.getStoreList(*storeDesc.CombinedAttrs(), a.options.Deterministic)

	// ShouldRebalance is true if a suitable replacement can be found.
	return a.balancer.improve(storeDesc, sl, makeNodeIDSet(storeDesc.Node.NodeID)) != nil
}

// computeQuorum computes the quorum value for the given number of nodes.
func computeQuorum(nodes int) int {
	return (nodes / 2) + 1
}
