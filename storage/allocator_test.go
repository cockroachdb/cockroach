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
	"reflect"
	"testing"
)

var simpleZoneConfig = ZoneConfig{
	Replicas: map[string][]DiskType{
		"a": []DiskType{SSD},
	},
}

var multiDisksConfig = ZoneConfig{
	Replicas: map[string][]DiskType{
		"a": []DiskType{SSD, HDD, MEM},
	},
}

var multiDCConfig = ZoneConfig{
	Replicas: map[string][]DiskType{
		"a": []DiskType{SSD},
		"b": []DiskType{SSD},
	},
}

var singleStore = func(x string) ([]StoreAttributes, error) {
	return []StoreAttributes{
		StoreAttributes{
			StoreID: 1,
			Attributes: NodeAttributes{
				NodeID:     1,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  SSD,
			},
		},
	}, nil
}

var sameDCStores = func(x string) ([]StoreAttributes, error) {
	return []StoreAttributes{
		StoreAttributes{
			StoreID: 1,
			Attributes: NodeAttributes{
				NodeID:     1,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  SSD,
			},
		},
		StoreAttributes{
			StoreID: 2,
			Attributes: NodeAttributes{
				NodeID:     2,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  SSD,
			},
		},
		StoreAttributes{
			StoreID: 3,
			Attributes: NodeAttributes{
				NodeID:     2,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  HDD,
			},
		},
		StoreAttributes{
			StoreID: 4,
			Attributes: NodeAttributes{
				NodeID:     3,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  HDD,
			},
		},
		StoreAttributes{
			StoreID: 5,
			Attributes: NodeAttributes{
				NodeID:     4,
				Datacenter: "a",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  MEM,
			},
		},
	}, nil
}

var multiDCStores = func(dc string) ([]StoreAttributes, error) {
	if dc == "a" {
		return []StoreAttributes{
			StoreAttributes{
				StoreID: 1,
				Attributes: NodeAttributes{
					NodeID:     1,
					Datacenter: "a",
					PDU:        "a",
					Rack:       "a",
				},
				Capacity: StoreCapacity{
					Capacity:  100,
					Available: 100,
					DiskType:  SSD,
				},
			},
		}, nil
	}
	return []StoreAttributes{
		StoreAttributes{
			StoreID: 2,
			Attributes: NodeAttributes{
				NodeID:     2,
				Datacenter: "b",
				PDU:        "a",
				Rack:       "a",
			},
			Capacity: StoreCapacity{
				Capacity:  100,
				Available: 100,
				DiskType:  SSD,
			},
		},
	}, nil
}

var noStores = func(x string) ([]StoreAttributes, error) {
	return []StoreAttributes{}, nil
}

func TestSimpleRetrieval(t *testing.T) {
	var a = allocator{
		storeFinder: singleStore,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(&simpleZoneConfig, map[string][]Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	var expected = []Replica{
		Replica{
			NodeID:     1,
			StoreID:    1,
			DiskType:   SSD,
			Datacenter: "a",
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Expected: %v\nGot: %v", expected, result)
	}
}

func TestNoAvailableDisks(t *testing.T) {
	var a = allocator{
		storeFinder: noStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(&simpleZoneConfig, map[string][]Replica{})
	if err == nil {
		t.Fatalf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestThreeDisksSameDC(t *testing.T) {
	var a = allocator{
		storeFinder: sameDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(&multiDisksConfig, map[string][]Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Expected: 3 replicas, Got: %v", len(result))
	}
	if result[0].DiskType == result[1].DiskType {
		t.Fatal("Expected different disk types, got both the same.")
	}
}

func TestTwoDataCenters(t *testing.T) {
	var a = allocator{
		storeFinder: multiDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(&multiDCConfig, map[string][]Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	var expected = []Replica{
		Replica{
			NodeID:     1,
			StoreID:    1,
			Datacenter: "a",
			DiskType:   SSD,
		},
		Replica{
			NodeID:     2,
			StoreID:    2,
			Datacenter: "b",
			DiskType:   SSD,
		},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("Expected: %v\nGot: %v", expected, result)
	}
}

func TestExistingReplica(t *testing.T) {
	var a = allocator{
		storeFinder: sameDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(&multiDisksConfig, map[string][]Replica{
		"a": []Replica{
			Replica{
				NodeID:   1,
				StoreID:  1,
				DiskType: SSD,
			},
		},
	})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	var expected = []Replica{
		Replica{
			NodeID:   2,
			StoreID:  3,
			DiskType: HDD,
		},
	}
	if result[0].DiskType != HDD {
		t.Fatalf("Expected placement on an HDD, Got: %v", result[0].DiskType)
	}
	if result[0].NodeID == 1 {
		t.Fatal("Expected placement on a node other than 1")
	}
	if !reflect.DeepEqual(expected, result) {
	}
}
