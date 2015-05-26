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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func countRangeReplicas(client *client.DB) (int, error) {
	r, err := client.Scan(keys.Meta2Prefix, keys.Meta2Prefix.PrefixEnd(), 10)
	if err != nil {
		return 0, err
	}

	for _, row := range r.Rows {
		desc := &proto.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			return 0, err
		}
		if string(desc.StartKey) == "" {
			return len(desc.Replicas), nil
		}
	}
	return 0, util.Errorf("first range not found")
}

func checkRangeReplication(t *testing.T, cluster *localcluster.Cluster, d time.Duration) {
	// Always talk to node 0.
	client, err := makeDBClient(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}

	wantedReplicas := 3
	if len(cluster.Nodes) < 3 {
		wantedReplicas = len(cluster.Nodes)
	}

	log.Infof("waiting for first range to have %d replicas", wantedReplicas)

	util.SucceedsWithin(t, d, func() error {
		select {
		case <-stopper:
			t.Fatalf("interrupted")
			return nil
		case <-time.After(1 * time.Second):
		}

		found, err := countRangeReplicas(client)
		if err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "%d ", found)
		if found == wantedReplicas {
			fmt.Printf("... correct number of replicas found\n")
			return nil
		}
		return fmt.Errorf("not enough replicas")
	})
}

func TestRangeReplication(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	defer l.Stop()

	checkRangeReplication(t, l, 20*time.Second)
}
