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
// Author: Marc Berhault (marc@cockroachlabs.com)

package acceptance

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/acceptance/cluster"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func countRangeReplicas(db *client.DB) (int, error) {
	desc := &roachpb.RangeDescriptor{}
	if err := db.GetProto(keys.RangeDescriptorKey(roachpb.RKeyMin), desc); err != nil {
		return 0, err
	}
	return len(desc.Replicas), nil
}

func checkRangeReplication(t *testing.T, c cluster.Cluster, d time.Duration) {
	if c.NumNodes() < 1 {
		// Looks silly, but we actually start zero-node clusters in the
		// reference tests.
		t.Log("replication test is a no-op for empty cluster")
		return
	}

	wantedReplicas := 3
	if c.NumNodes() < 3 {
		wantedReplicas = c.NumNodes()
	}

	log.Infof(context.Background(), "waiting for first range to have %d replicas", wantedReplicas)

	util.SucceedsSoon(t, func() error {
		// Reconnect on every iteration; gRPC will eagerly tank the connection
		// on transport errors. Always talk to node 0 because it's guaranteed
		// to exist.
		client, dbStopper := c.NewClient(t, 0)
		defer dbStopper.Stop()

		select {
		case <-stopper:
			t.Fatalf("interrupted")
			return nil
		case <-time.After(1 * time.Second):
		}

		foundReplicas, err := countRangeReplicas(client)
		if err != nil {
			return err
		}

		if log.V(1) {
			log.Infof(context.Background(), "found %d replicas", foundReplicas)
		}
		if foundReplicas >= wantedReplicas {
			return nil
		}
		return fmt.Errorf("expected %d replicas, only found %d", wantedReplicas, foundReplicas)
	})

	log.Infof(context.Background(), "found %d replicas", wantedReplicas)
}
