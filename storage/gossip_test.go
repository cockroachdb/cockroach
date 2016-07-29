// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package storage_test

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

func TestGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop()

	errors := make(chan error)
	descs := make(chan *roachpb.RangeDescriptor)
	tc.Servers[0].Gossip().RegisterCallback(gossip.KeyFirstRangeDescriptor,
		func(_ string, content roachpb.Value) {
			var desc roachpb.RangeDescriptor
			if err := content.GetProto(&desc); err != nil {
				errors <- err
			} else {
				descs <- &desc
			}
		})

	// Wait for the specified descriptor to be gossiped for the first range. We
	// loop because the timing of replica addition and lease transfer can cause
	// extra gossiping of the first range.
	waitForGossip := func(desc *roachpb.RangeDescriptor) {
		for {
			select {
			case err := <-errors:
				t.Fatal(err)
			case gossiped := <-descs:
				if reflect.DeepEqual(desc, gossiped) {
					return
				}
				log.Infof(context.TODO(), "expected\n%+v\nbut found\n%+v", desc, gossiped)
			}
		}
	}

	// Expect an initial callback of the first range descriptor.
	select {
	case err := <-errors:
		t.Fatal(err)
	case <-descs:
	}

	// Add two replicas. The first range descriptor should be gossiped after each
	// addition.
	var desc *roachpb.RangeDescriptor
	firstRangeKey := keys.MinKey
	for i := 1; i <= 2; i++ {
		var err error
		if desc, err = tc.AddReplicas(firstRangeKey, tc.Target(i)); err != nil {
			t.Fatal(err)
		}
		waitForGossip(desc)
	}

	// Transfer the lease to a new node. This should cause the first range to be
	// gossiped again.
	if err := tc.TransferRangeLease(desc, tc.Target(1)); err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// Remove a non-lease holder replica.
	desc, err := tc.RemoveReplicas(firstRangeKey, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}
	waitForGossip(desc)

	// TODO(peter): Re-enable or remove when we've resolved the discussion
	// about removing the lease-holder replica. See #7872.

	// // Remove the lease holder replica.
	// leaseHolder, err := tc.FindRangeLeaseHolder(desc, nil)
	// desc, err = tc.RemoveReplicas(firstRangeKey, leaseHolder)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// select {
	// case err := <-errors:
	// 	t.Fatal(err)
	// case gossiped := <-descs:
	// 	if !reflect.DeepEqual(desc, gossiped) {
	// 		t.Fatalf("expected\n%+v\nbut found\n%+v", desc, gossiped)
	// 	}
	// }
}
