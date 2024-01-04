// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"context"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func makeServer(
	nodeID roachpb.NodeID, locality roachpb.Locality, nodeDescs *syncutil.IntMap,
) *server {
	ctx := context.Background()
	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	stopper := stop.NewStopper()
	stopper.Stop(ctx)
	registry := metric.NewRegistry()
	nodeIDContainer := base.NodeIDContainer{}
	nodeIDContainer.Set(ctx, nodeID)
	clusterIDContainer := base.ClusterIDContainer{}
	u := uuid.MakeV4()
	clusterIDContainer.Set(ctx, u)
	return newServer(ambientCtx, &clusterIDContainer, &nodeIDContainer, stopper, registry, nodeDescs)
}

// Tests the randomness of alternative id selection in getRandomNodeDescriptor
func TestRandomAlternativeNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodeDescs := syncutil.IntMap{}
	numDescriptors := 12
	var serverID roachpb.NodeID = 3
	var excludeID roachpb.NodeID = 7
	for i := 1; i <= numDescriptors; i++ {
		var desc roachpb.NodeDescriptor
		desc.NodeID = roachpb.NodeID(i)
		nodeDescs.Store(int64(desc.NodeID), unsafe.Pointer(&desc))
	}
	countSelected := make(map[roachpb.NodeID]int32, numDescriptors)
	server := makeServer(serverID, roachpb.Locality{}, &nodeDescs)
	for i := 0; i < 200; i++ {
		desc, err := server.getRandomNodeDescriptor(excludeID)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if desc.NodeID == serverID || desc.NodeID == excludeID {
			t.Fatalf("excluded id %v was selected", desc.NodeID)
		}
		countSelected[desc.NodeID]++
	}
	if len(countSelected) != numDescriptors-2 {
		t.Fatalf("some descriptors weren't selected: %v", countSelected)
	}
	for i, v := range countSelected {
		if v < 5 {
			t.Fatalf("%v was selected only %d times", i, v)
		}
	}
}

// Tests that getRandomNodeDescriptor returns an error when the server has no
// valid ids
func TestAlternativeNodeNotFound(t *testing.T) {
	defer leaktest.AfterTest(t)()
	nodeDescs := syncutil.IntMap{}
	numDescriptors := 2
	var serverID roachpb.NodeID = 1
	var excludeID roachpb.NodeID = 2
	for i := 1; i <= numDescriptors; i++ {
		var desc roachpb.NodeDescriptor
		desc.NodeID = roachpb.NodeID(i)
		nodeDescs.Store(int64(desc.NodeID), unsafe.Pointer(&desc))
	}
	server := makeServer(serverID, roachpb.Locality{}, &nodeDescs)
	desc, err := server.getRandomNodeDescriptor(excludeID)
	if err == nil || err.Error() != "no valid descriptors found" {
		t.Errorf("unexpected: desc, err = %v, %v", desc, err)
	}
}
