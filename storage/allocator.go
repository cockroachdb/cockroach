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
// Author: Levon Lloyd (levon.lloyd@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// StoreFinder finds the disks in a datacenter with the most available capacity.
type StoreFinder func(engine.Attributes) ([]*StoreDescriptor, error)

// allocator makes allocation decisions based on a zone configuration,
// existing range metadata and available stores. Configuration
// settings and range metadata information is stored directly in the
// engine-backed range they describe. Information on suitability and
// availability of servers is gleaned from the gossip network.
type allocator struct {
	storeFinder StoreFinder
	rand        rand.Rand
}

// allocate returns a suitable store based on the supplied
// attributes list. If none are available / suitable, returns an
// error. It uses the allocator's StoreFinder to select the set of
// available stores matching attributes for missing replicas and picks
// using randomly weighted selection based on available capacities.
func (a *allocator) allocate(required engine.Attributes, existingReplicas []proto.Replica) (
	*StoreDescriptor, error) {
	// Get a set of current nodes -- we never want to allocate on an existing node.
	usedNodes := make(map[int32]struct{})
	for _, replica := range existingReplicas {
		usedNodes[replica.NodeID] = struct{}{}
	}

	stores, err := a.storeFinder(required)
	if err != nil {
		return nil, err
	}

	// Randomly pick a node weighted by capacity.
	var candidates []*StoreDescriptor
	var capacityTotal float64
	for _, s := range stores {
		if _, ok := usedNodes[s.Node.NodeID]; !ok {
			candidates = append(candidates, s)
			capacityTotal += s.Capacity.PercentAvail()
		}
	}

	var capacitySeen float64
	targetCapacity := a.rand.Float64() * capacityTotal

	// Walk through candidates, stopping when
	// we've passed the capacity target.
	for _, c := range candidates {
		capacitySeen += c.Capacity.PercentAvail()
		if capacitySeen >= targetCapacity {
			return c, nil
		}
	}
	return nil, util.Errorf("unable to find an appropriate store for requested replica attributes")
}
