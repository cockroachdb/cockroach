// Copyright 2019 The Cockroach Authors.
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

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestReplicaUpdateLastReplicaAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := func(replicaIDs ...roachpb.ReplicaID) roachpb.RangeDescriptor {
		d := roachpb.RangeDescriptor{
			StartKey:         roachpb.RKey("a"),
			EndKey:           roachpb.RKey("b"),
			InternalReplicas: make([]roachpb.ReplicaDescriptor, len(replicaIDs)),
		}
		for i, id := range replicaIDs {
			d.InternalReplicas[i].ReplicaID = id
		}
		return d
	}

	testCases := []struct {
		oldDesc                  roachpb.RangeDescriptor
		newDesc                  roachpb.RangeDescriptor
		lastReplicaAdded         roachpb.ReplicaID
		expectedLastReplicaAdded roachpb.ReplicaID
	}{
		// Adding a replica. In normal operation, Replica IDs always increase.
		{desc(), desc(1), 0, 1},
		{desc(1), desc(1, 2), 0, 2},
		{desc(1, 2), desc(1, 2, 3), 0, 3},
		// Add a replica with an out-of-order ID (this shouldn't happen in practice).
		{desc(2, 3), desc(1, 2, 3), 0, 0},
		// Removing a replica has no-effect.
		{desc(1, 2, 3), desc(2, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 2), 3, 0},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var r Replica
			r.mu.state.Desc = &c.oldDesc
			r.mu.lastReplicaAdded = c.lastReplicaAdded
			r.setDesc(context.Background(), &c.newDesc)
			if c.expectedLastReplicaAdded != r.mu.lastReplicaAdded {
				t.Fatalf("expected %d, but found %d",
					c.expectedLastReplicaAdded, r.mu.lastReplicaAdded)
			}
		})
	}
}
