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
	"fmt"
	"math/rand"
	"sort"
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
// are available / suitable, returns an error. It looks at the zone
// config for the block to determine where it needs to send data, then
// uses the StoreFinder to pick a random set of nodes in each data
// center based on the available capacity of the node.
// TODO(levon): Handle Racks/Power Units.
func (a *allocator) allocate(config *ZoneConfig, existingReplicas map[string][]Replica) ([]Replica, error) {
	var neededReplicas int
	var results []Replica

	// Create a sorted list of datacenters to fix the order in which
	// replicas are assigned.
	dcs := make([]string, len(config.Replicas), len(config.Replicas))
	for dc := range config.Replicas {
		dcs = append(dcs, dc)
	}
	sort.Strings(dcs)

	for _, dc := range dcs {
		diskTypes := config.Replicas[dc]
		usedHosts := make(map[int32]struct{})
		for _, replica := range existingReplicas[dc] {
			usedHosts[replica.NodeID] = struct{}{}
		}

		neededReplicas += len(diskTypes)
		stores, err := a.storeFinder(dc)
		if err != nil {
			return nil, err
		}

		// Compute how many of each DiskType we need in this Data Center.
		// We store the maximal disk type as we need it below.
		maxDiskType := DiskType(0)
		neededDiskTypes := make(map[DiskType]int)
		for _, strDiskType := range diskTypes {
			diskType := StringToDiskType(strDiskType)
			if diskType > maxDiskType {
				maxDiskType = diskType
			}
			neededDiskTypes[diskType]++
		}

		for _, replica := range existingReplicas[dc] {
			neededDiskTypes[replica.DiskType]--
		}

		// For each disk type to be placed in this data center.
		// We avoid ranging over the map directly as this introduces
		// unwanted randomness.
		for diskType := DiskType(0); diskType <= maxDiskType; diskType++ {
			count := neededDiskTypes[diskType]
			for i := 0; i < count; i++ {
				// Randomly pick a node weighted by capacity.
				var candidates []StoreAttributes
				var capacityTotal float64
				for _, s := range stores {
					_, alreadyUsed := usedHosts[s.Attributes.NodeID]
					if s.Capacity.DiskType == diskType && !alreadyUsed {
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
						replica := Replica{
							NodeID:     c.Attributes.NodeID,
							StoreID:    c.StoreID,
							Datacenter: dc,
							DiskType:   diskType,
							// RangeID is filled in later, when range is created.
						}
						results = append(results, replica)
						usedHosts[c.Attributes.NodeID] = struct{}{}
						break
					}
				}
			}
		}
	}
	var err error
	if len(existingReplicas)+len(results) < neededReplicas {
		err = fmt.Errorf("unable to place all %d replicas, Only %d found", neededReplicas, len(results))
	}
	return results, err
}

/*func findZoneConfig(key string) (ZoneConfig, error) {

}*/
