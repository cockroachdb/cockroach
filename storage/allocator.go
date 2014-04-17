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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"math/rand"
)

// ZoneConfigFinder looks up a ZoneConfig for a range that starts with
// a given key.
type ZoneConfigFinder func(string) (ZoneConfig, error)

// AvailableDiskFinder finds the disks in a datacenter with the most available capacity.
type AvailableDiskFinder func(string) ([]AvailableDiskConfig, error)

// allocator makes allocation decisions based on a zone
// configuration, existing range metadata and available servers &
// disks. Configuration settings and range metadata information is
// stored directly in the engine-backed range they describe.
// Information on suitability and availability of servers is
// gleaned from the gossip network.
type allocator struct {
	diskFinder AvailableDiskFinder
	zcFinder   ZoneConfigFinder
	rand       rand.Rand
}

var maxCapacityPrefix = "max-free-capacity-"

// allocate returns a suitable Replica for the range and zone. If none
// are available / suitable, returns an error. It looks up the zone config for
// the block to determine where it needs to send data, then uses the gossip
// network to pick a random set of nodes in each data center based on the
// available capacity of the node.
// TODO(levon) Handle Racks/Power Units.
// TODO(levon) throw error if not enough suitable replicas can be found

func (a *allocator) allocate(start string, existingReplicas []Replica) ([]Replica, error) {
	usedHosts := make(map[string]bool)
	for _, replica := range existingReplicas {
		usedHosts[replica.Addr] = true
	}
	// Find the Zone Config that applies to this range.
	zoneConfig, err := a.zcFinder(start)
	if err != nil {
		return nil, err
	}
	result := make([]Replica, 0, 1)
	for dc, diskTypes := range zoneConfig.Replicas {
		// For each replica to be placed in this data center.
		for _, diskType := range diskTypes {
			// Randomly pick a node weighted by capacity.
			candidates := make([]AvailableDiskConfig, len(zoneConfig.Replicas))
			var candidateCapacityTotal float64
			disks, err := a.diskFinder(dc)
			if err != nil {
				return nil, err
			}
			for _, c := range disks {
				if c.DiskType == diskType && !usedHosts[c.Node.Address] {
					candidates = append(candidates, c)
					candidateCapacityTotal += c.DiskCapacity.PercentAvail()
				}
			}

			var capacitySeen float64
			targetCapacity := rand.Float64()

			// Walk through candidates in random order, stopping when
			// we've passed the capacity target.
			for _, c := range candidates {
				capacitySeen += (c.DiskCapacity.PercentAvail() / candidateCapacityTotal)
				if capacitySeen >= targetCapacity {
					replica := Replica{
						Addr: c.Node.Address,
						Disk: c.Disk,
					}
					result = append(result, replica)
					usedHosts[c.Node.Address] = true
					break
				}
			}
		}
	}
	return result, nil
}

/*func findZoneConfig(key string) (ZoneConfig, error) {

}*/
