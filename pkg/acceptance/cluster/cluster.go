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
	"net"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
)

// A Cluster is an abstraction away from a concrete cluster deployment (i.e.
// a local docker cluster, or an AWS-provisioned one). It exposes a shared
// set of methods for test-related manipulation.
type Cluster interface {
	// NumNodes returns the number of nodes in the cluster, running or not.
	NumNodes() int
	// NewClient returns a kv client for the given node.
	NewClient(context.Context, int) (*client.DB, error)
	// PGUrl returns a URL string for the given node postgres server.
	PGUrl(context.Context, int) string
	// InternalIP returns the address used for inter-node communication.
	InternalIP(ctx context.Context, i int) net.IP
	// Assert verifies that the cluster state is as expected (i.e. no unexpected
	// restarts or node deaths occurred). Tests can call this periodically to
	// ascertain cluster health.
	Assert(context.Context, testing.TB)
	// AssertAndStop performs the same test as Assert but then proceeds to
	// dismantle the cluster.
	AssertAndStop(context.Context, testing.TB)
	// ExecRoot executes the given command with super-user privileges.
	ExecRoot(ctx context.Context, i int, cmd []string) error
	// Kill terminates the cockroach process running on the given node number.
	// The given integer must be in the range [0,NumNodes()-1].
	Kill(context.Context, int) error
	// Restart terminates the cockroach process running on the given node
	// number, unless it is already stopped, and restarts it.
	// The given integer must be in the range [0,NumNodes()-1].
	Restart(context.Context, int) error
	// URL returns the HTTP(s) endpoint.
	URL(context.Context, int) string
	// Addr returns the host and port from the node in the format HOST:PORT.
	Addr(ctx context.Context, i int, port string) string
	// Hostname returns a node's hostname.
	Hostname(i int) string
}

// Consistent performs a replication consistency check on all the ranges
// in the cluster. It depends on a majority of the nodes being up, and does
// the check against the node at index i.
func Consistent(ctx context.Context, c Cluster, i int) error {
	kvClient, err := c.NewClient(ctx, i)
	if err != nil {
		return err
	}
	// Set withDiff to false because any failure results in a second consistency check
	// being called with withDiff=true.
	return kvClient.CheckConsistency(
		ctx,
		keys.LocalMax,
		keys.MaxKey,
		false, /* withDiff*/
	)
}
