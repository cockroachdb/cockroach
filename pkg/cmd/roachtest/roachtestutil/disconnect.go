// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/stretchr/testify/require"
)

// Disconnect takes a set of nodes and each nodes internal ips. It disconnects
// each node from all the others in the list.
func Disconnect(t test.Test, c cluster.Cluster, ctx context.Context, nodes []int) {
	if c.IsLocal() {
		t.Status("skipping iptables disconnect on local cluster")
		return
	}

	ips, err := c.InternalIP(ctx, t.L(), nodes)
	require.NoError(t, err)

	// disconnect each node from every other passed in node.
	for n := 0; n < len(nodes); n++ {
		for ip := 0; ip < len(ips); ip++ {
			if n != ip {
				c.Run(ctx, c.Node(nodes[n]), `sudo iptables -A INPUT -s `+ips[ip]+` -j DROP`)
				c.Run(ctx, c.Node(nodes[n]), `sudo iptables -A OUTPUT -d `+ips[ip]+` -j DROP`)
			}
		}
	}
}

// Cleanup takes a set of nodes and each nodes internal ips. It disconnects
// each node from all the others in the list.
func Cleanup(t test.Test, c cluster.Cluster, ctx context.Context) {
	if c.IsLocal() {
		t.Status("skipping iptables cleanup on local cluster")
		return
	}
	c.Run(ctx, c.All(), `sudo iptables -F`)
}
