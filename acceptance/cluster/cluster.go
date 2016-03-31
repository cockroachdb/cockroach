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
// Author: Tobias Schottdorf

package cluster

import (
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/util/stop"
)

// A Cluster is an abstraction away from a concrete cluster deployment (i.e.
// a local docker cluster, or an AWS-provisioned one). It exposes a shared
// set of methods for test-related manipulation.
type Cluster interface {
	// NumNodes returns the number of nodes in the cluster, running or not.
	NumNodes() int
	// NewClient returns a kv client for the given node.
	NewClient(*testing.T, int) (*client.DB, *stop.Stopper)
	// PGUrl returns a URL string for the given node postgres server.
	PGUrl(int) string
	// Assert verifies that the cluster state is as expected (i.e. no unexpected
	// restarts or node deaths occurred). Tests can call this periodically to
	// ascertain cluster health.
	Assert(*testing.T)
	// AssertAndStop performs the same test as Assert but then proceeds to
	// dismantle the cluster.
	AssertAndStop(*testing.T)
	// Kill terminates the cockroach process running on the given node number.
	// The given integer must be in the range [0,NumNodes()-1].
	Kill(int) error
	// Restart terminates the cockroach process running on the given node
	// number, unless it is already stopped, and restarts it.
	// The given integer must be in the range [0,NumNodes()-1].
	Restart(int) error
	// URL returns the HTTP(s) endpoint.
	URL(int) string
	// Addr returns the host and port from the node in the format HOST:PORT.
	Addr(int) string
}

// Consistent performs a replication consistency check on all the ranges
// in the cluster. It depends on a majority of the nodes being up.
func Consistent(t *testing.T, c Cluster) {
	if c.NumNodes() <= 0 {
		return
	}
	// Always connect to the first node in the cluster.
	kvClient, kvStopper := c.NewClient(t, 0)
	defer kvStopper.Stop()
	// Set withDiff to false because any failure results in a second consistency check
	// being called with withDiff=true.
	if pErr := kvClient.CheckConsistency(keys.LocalMax, keys.MaxKey, false /* withDiff*/); pErr != nil {
		t.Fatal(pErr)
	}
}
