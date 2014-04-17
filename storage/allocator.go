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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Levon Lloyd (levon.lloyd@gmail.com)

package storage

import (
	"math/rand"
)

// StoreFinder finds the disks in a datacenter with the most available capacity.
type StoreFinder func(string) ([]StoreAttributes, error)

// allocator makes allocation decisions based on a zone configuration,
// existing range metadata and available stores. Configuration
// settings and range metadata information is stored directly in the
// engine-backed range they describe.  Information on suitability and
// availability of servers is gleaned from the gossip network.
type allocator struct {
	storeFinder StoreFinder
	rand        rand.Rand
}

// allocate returns a suitable Replica for the range and zone. If none
// are available / suitable, returns an error. It looks up the zone
// config for the block to determine where it needs to send data, then
// uses the gossip network to pick a random set of nodes in each data
// center based on the available capacity of the node.
// TODO(levon): Handle Racks/Power Units.
// TODO(levon): throw error if not enough suitable replicas can be found.
// TODO(spencer): only need the delta between existing replicas and zone config's specifications.
func (a *allocator) allocate(config *ZoneConfig, existingReplicas []Replica) ([]Replica, error) {
	usedHosts := make(map[int32]struct{})
	for _, replica := range existingReplicas {
		usedHosts[replica.NodeID] = struct{}{}
	}
	var results []Replica = nil
	for dc, diskTypes := range config.Replicas {
		// For each replica to be placed in this data center.
		for _, diskType := range diskTypes {
			// Randomly pick a node weighted by capacity.
			var candidates []StoreAttributes = nil
			var capacityTotal float64
			stores, err := a.storeFinder(dc)
			if err != nil {
				return nil, err
			}
			for _, s := range stores {
				_, alreadyUsed := usedHosts[s.Attributes.NodeID]
				if s.Capacity.DiskType == diskType && !alreadyUsed {
					candidates = append(candidates, s)
					capacityTotal += s.Capacity.PercentAvail()
				}
			}

			var capacitySeen float64
			targetCapacity := rand.Float64() * capacityTotal

			// Walk through candidates in random order, stopping when
			// we've passed the capacity target.
			for _, c := range candidates {
				capacitySeen += c.Capacity.PercentAvail()
				if capacitySeen >= targetCapacity {
					replica := Replica{
						NodeID:  c.Attributes.NodeID,
						StoreID: c.StoreID,
						// RangeID is filled in later, when range is created.
					}
					results = append(results, replica)
					usedHosts[c.Attributes.NodeID] = struct{}{}
					break
				}
			}
		}
	}
	return results, nil
}

/*func findZoneConfig(key string) (ZoneConfig, error) {

}*/
