// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	gosql "database/sql"
	"net"
	"testing"

	"github.com/cockroachdb/errors"
)

// A Cluster is an abstraction away from a concrete cluster deployment (i.e.
// a local docker cluster, or an AWS-provisioned one). It exposes a shared
// set of methods for test-related manipulation.
type Cluster interface {
	// NumNodes returns the number of nodes in the cluster, running or not.
	NumNodes() int
	// NewDB returns a sql.DB client for the given node.
	NewDB(context.Context, int) (*gosql.DB, error)
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
	// ExecCLI runs `./cockroach <args>`, while filling in required flags such as
	// --insecure, --certs-dir, --host.
	//
	// Returns stdout, stderr, and an error.
	ExecCLI(ctx context.Context, i int, args []string) (string, string, error)
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
	return errors.Errorf("Consistency checking is unimplmented and should be re-implemented using SQL")
}
