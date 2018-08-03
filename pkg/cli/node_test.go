// Copyright 2018 The Cockroach Authors.
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

package cli

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func makeNodeStatus(nodeID roachpb.NodeID) status.NodeStatus {
	return status.NodeStatus{
		Desc: roachpb.NodeDescriptor{NodeID: nodeID},
	}
}

func makeDecommissionStatus(
	nodeID roachpb.NodeID, isLive bool, decommissioning bool,
) serverpb.DecommissionStatusResponse_Status {
	return serverpb.DecommissionStatusResponse_Status{
		NodeID:          nodeID,
		IsLive:          isLive,
		Decommissioning: decommissioning,
	}
}

func TestHideDecommissioned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ns := []status.NodeStatus{
		makeNodeStatus(1),
		makeNodeStatus(2),
		makeNodeStatus(3),
		makeNodeStatus(4),
		// Below don't have decommission statuses
		makeNodeStatus(5),
		makeNodeStatus(6),
	}
	d := serverpb.DecommissionStatusResponse{
		Status: []serverpb.DecommissionStatusResponse_Status{
			makeDecommissionStatus(1, true, false),  // live
			makeDecommissionStatus(2, false, false), // dead but not decommissioning
			makeDecommissionStatus(3, true, true),   // live, decommissioning
			makeDecommissionStatus(4, false, true),  // dead and decommissioned --> hide
			// NodeIDs do not match those in NodeStatuses --> have no effect, ignored
			makeDecommissionStatus(15, true, false), // live
			makeDecommissionStatus(16, false, true), // dead and decommissioned
		},
	}

	ns = hideDecommissioned(ns, &d)

	expectedNs := []status.NodeStatus{
		makeNodeStatus(1),
		makeNodeStatus(2),
		makeNodeStatus(3),
		// missing NodeID 4 which is dead and decommissioned
		makeNodeStatus(5),
		makeNodeStatus(6),
	}
	expectedD := serverpb.DecommissionStatusResponse{
		Status: []serverpb.DecommissionStatusResponse_Status{
			makeDecommissionStatus(1, true, false),  // live
			makeDecommissionStatus(2, false, false), // dead but not decommissioning
			makeDecommissionStatus(3, true, true),   // live, decommissioning
		},
	}
	if !reflect.DeepEqual(ns, expectedNs) {
		var expectedIDs []roachpb.NodeID
		for _, n := range expectedNs {
			expectedIDs = append(expectedIDs, n.Desc.NodeID)
		}
		var IDs []roachpb.NodeID
		for _, n := range ns {
			IDs = append(IDs, n.Desc.NodeID)
		}
		t.Errorf("unexpected NodeStatuses: want %v, got %v\n", expectedIDs, IDs)
	}

	if !reflect.DeepEqual(d, expectedD) {
		var expectedIDs []roachpb.NodeID
		for _, n := range expectedD.Status {
			expectedIDs = append(expectedIDs, n.NodeID)
		}
		var IDs []roachpb.NodeID
		for _, n := range d.Status {
			IDs = append(IDs, n.NodeID)
		}
		t.Errorf("unexpected Decommission statuses: want %v, got %v\n", expectedIDs, IDs)
	}
}
