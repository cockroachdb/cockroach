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

package storage

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

var simpleZoneConfig = proto.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
	},
}

var multiDisksConfig = proto.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"a", "hdd"}},
		{Attrs: []string{"a", "mem"}},
	},
}

var multiDCConfig = proto.ZoneConfig{
	ReplicaAttrs: []proto.Attributes{
		{Attrs: []string{"a", "ssd"}},
		{Attrs: []string{"b", "ssd"}},
	},
}

// filterStores returns just the store descriptors in the supplied
// stores slice which contain all the specified attributes.
func filterStores(a proto.Attributes, stores []*StoreDescriptor) ([]*StoreDescriptor, error) {
	var filtered []*StoreDescriptor
	for _, s := range stores {
		sAttrs := s.CombinedAttrs()
		if a.IsSubset(*sAttrs) {
			filtered = append(filtered, s)
		}
	}
	return filtered, nil
}

var singleStore = func(a proto.Attributes) ([]*StoreDescriptor, error) {
	return filterStores(a, []*StoreDescriptor{
		{
			StoreID: 1,
			Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
			Node: proto.NodeDescriptor{
				NodeID: 1,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
	})
}

var sameDCStores = func(a proto.Attributes) ([]*StoreDescriptor, error) {
	return filterStores(a, []*StoreDescriptor{
		{
			StoreID: 1,
			Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
			Node: proto.NodeDescriptor{
				NodeID: 1,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
		{
			StoreID: 2,
			Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
			Node: proto.NodeDescriptor{
				NodeID: 2,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
		{
			StoreID: 3,
			Attrs:   proto.Attributes{Attrs: []string{"hdd"}},
			Node: proto.NodeDescriptor{
				NodeID: 2,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
		{
			StoreID: 4,
			Attrs:   proto.Attributes{Attrs: []string{"hdd"}},
			Node: proto.NodeDescriptor{
				NodeID: 3,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
		{
			StoreID: 5,
			Attrs:   proto.Attributes{Attrs: []string{"mem"}},
			Node: proto.NodeDescriptor{
				NodeID: 4,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
	})
}

var multiDCStores = func(a proto.Attributes) ([]*StoreDescriptor, error) {
	return filterStores(a, []*StoreDescriptor{
		{
			StoreID: 1,
			Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
			Node: proto.NodeDescriptor{
				NodeID: 1,
				Attrs:  proto.Attributes{Attrs: []string{"a"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
		{
			StoreID: 2,
			Attrs:   proto.Attributes{Attrs: []string{"ssd"}},
			Node: proto.NodeDescriptor{
				NodeID: 2,
				Attrs:  proto.Attributes{Attrs: []string{"b"}},
			},
			Capacity: engine.StoreCapacity{
				Capacity:  100,
				Available: 100,
			},
		},
	})
}

var noStores = func(a proto.Attributes) ([]*StoreDescriptor, error) {
	return filterStores(a, []*StoreDescriptor{})
}

func TestSimpleRetrieval(t *testing.T) {
	defer leaktest.AfterTest(t)
	var a = allocator{
		storeFinder: singleStore,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 1 || result.StoreID != 1 {
		t.Errorf("expected NodeID 1 and StoreID 1: %+v", result)
	}
}

func TestNoAvailableDisks(t *testing.T) {
	defer leaktest.AfterTest(t)
	var a = allocator{
		storeFinder: noStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(simpleZoneConfig.ReplicaAttrs[0], []proto.Replica{})
	if result != nil {
		t.Errorf("expected nil result: %+v", result)
	}
	if err == nil {
		t.Errorf("allocation succeeded despite there being no available disks: %v", result)
	}
}

func TestThreeDisksSameDC(t *testing.T) {
	defer leaktest.AfterTest(t)
	var a = allocator{
		storeFinder: sameDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result1, err := a.allocate(multiDisksConfig.ReplicaAttrs[0], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.StoreID != 2 {
		t.Errorf("Expected store 2; got %+v", result1)
	}
	exReplicas := []proto.Replica{
		{
			NodeID:  result1.Node.NodeID,
			StoreID: result1.StoreID,
			Attrs:   multiDisksConfig.ReplicaAttrs[0],
		},
	}
	result2, err := a.allocate(multiDisksConfig.ReplicaAttrs[1], exReplicas)
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result2.StoreID != 4 {
		t.Errorf("Expected store 4; got %+v", result2)
	}
	if result1.Node.NodeID == result2.Node.NodeID {
		t.Errorf("Expected node ids to be different %+v vs %+v", result1, result2)
	}
	result3, err := a.allocate(multiDisksConfig.ReplicaAttrs[2], []proto.Replica{})
	if err != nil {
		t.Errorf("Unable to perform allocation: %v", err)
	}
	if result3.Node.NodeID != 4 || result3.StoreID != 5 {
		t.Errorf("Expected node 4, store 5; got %+v", result3)
	}
}

func TestTwoDatacenters(t *testing.T) {
	defer leaktest.AfterTest(t)
	var a = allocator{
		storeFinder: multiDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result1, err := a.allocate(multiDCConfig.ReplicaAttrs[0], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	result2, err := a.allocate(multiDCConfig.ReplicaAttrs[1], []proto.Replica{})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result1.Node.NodeID != 1 || result2.Node.NodeID != 2 {
		t.Errorf("Expected nodes 1 & 2: %+v vs %+v", result1.Node, result2.Node)
	}
	// Verify that no result is forthcoming if we already have a replica.
	_, err = a.allocate(multiDCConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  result2.Node.NodeID,
			StoreID: result2.StoreID,
			Attrs:   multiDCConfig.ReplicaAttrs[1],
		},
	})
	if err == nil {
		t.Errorf("expected error on allocation without available stores")
	}
}

func TestExistingReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
	var a = allocator{
		storeFinder: sameDCStores,
		rand:        *rand.New(rand.NewSource(0)),
	}
	result, err := a.allocate(multiDisksConfig.ReplicaAttrs[1], []proto.Replica{
		{
			NodeID:  1,
			StoreID: 1,
			Attrs:   multiDisksConfig.ReplicaAttrs[0],
		},
	})
	if err != nil {
		t.Fatalf("Unable to perform allocation: %v", err)
	}
	if result.Node.NodeID != 3 || result.StoreID != 4 {
		t.Errorf("expected result to have node 3 and store 4: %+v", result)
	}
}
