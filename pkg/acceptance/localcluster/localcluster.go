// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package localcluster

import (
	"bytes"
	"context"
	gosql "database/sql"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/errors"
)

// LocalCluster implements cluster.Cluster.
type LocalCluster struct {
	*Cluster
}

var _ cluster.Cluster = &LocalCluster{}

// Port implements cluster.Cluster.
func (b *LocalCluster) Port(ctx context.Context, i int) string {
	return b.RPCPort(i)
}

// NumNodes implements cluster.Cluster.
func (b *LocalCluster) NumNodes() int {
	return len(b.Nodes)
}

// NewDB implements the Cluster interface.
func (b *LocalCluster) NewDB(ctx context.Context, i int) (*gosql.DB, error) {
	return gosql.Open("postgres", b.PGUrl(ctx, i))
}

// PGUrl implements cluster.Cluster.
func (b *LocalCluster) PGUrl(ctx context.Context, i int) string {
	return b.Nodes[i].PGUrl()
}

// InternalIP implements cluster.Cluster.
func (b *LocalCluster) InternalIP(ctx context.Context, i int) net.IP {
	ips, err := net.LookupIP(b.IPAddr(i))
	if err != nil {
		panic(err)
	}
	return ips[0]
}

// Assert implements cluster.Cluster.
func (b *LocalCluster) Assert(ctx context.Context, t testing.TB) {
	// TODO(tschottdorf): actually implement this.
}

// AssertAndStop implements cluster.Cluster.
func (b *LocalCluster) AssertAndStop(ctx context.Context, t testing.TB) {
	b.Assert(ctx, t)
	b.Close()
}

// ExecCLI implements cluster.Cluster.
func (b *LocalCluster) ExecCLI(ctx context.Context, i int, cmd []string) (string, string, error) {
	cmd = append([]string{b.Cfg.Binary}, cmd...)
	cmd = append(cmd, "--insecure", "--host", ":"+b.Port(ctx, i))
	c := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	var o, e bytes.Buffer
	c.Stdout, c.Stderr = &o, &e
	err := c.Run()
	if err != nil {
		err = errors.Wrapf(err, "cmd: %v\nstderr:\n %s\nstdout:\n %s", cmd, o.String(), e.String())
	}
	return o.String(), e.String(), err
}

// Kill implements cluster.Cluster.
func (b *LocalCluster) Kill(ctx context.Context, i int) error {
	b.Nodes[i].Kill()
	return nil
}

// RestartAsync restarts the node. The returned channel receives an error or,
// once the node is successfully connected to the cluster and serving, nil.
func (b *LocalCluster) RestartAsync(ctx context.Context, i int) <-chan error {
	b.Nodes[i].Kill()
	joins := b.joins()
	ch := b.Nodes[i].StartAsync(ctx, joins...)
	if len(joins) == 0 && len(b.Nodes) > 1 {
		// This blocking loop in is counter-intuitive but is essential in allowing
		// restarts of whole clusters. Roughly the following happens:
		//
		// 1. The whole cluster gets killed.
		// 2. A node restarts.
		// 3. It will *block* here until it has written down the file which contains
		//    enough information to link other nodes.
		// 4. When restarting other nodes, and `.joins()` is passed in, these nodes
		//    can connect (at least) to the first node.
		// 5. the cluster can become healthy after restart.
		//
		// If we didn't block here, we'd start all nodes up with join addresses that
		// don't make any sense, and the cluster would likely not become connected.
		//
		// An additional difficulty is that older versions (pre 1.1) don't write
		// this file. That's why we let *every* node do this (you could try to make
		// only the first one wait, but if that one is 1.0, bad luck).
		// Short-circuiting the wait in the case that the listening URL file is
		// written makes restarts work with 1.0 servers for the most part.
		for {
			if gossipAddr := b.Nodes[i].AdvertiseAddr(); gossipAddr != "" {
				return ch
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return ch
}

// Restart implements cluster.Cluster.
func (b *LocalCluster) Restart(ctx context.Context, i int) error {
	return <-b.RestartAsync(ctx, i)
}

// URL implements cluster.Cluster.
func (b *LocalCluster) URL(ctx context.Context, i int) string {
	rest := b.Nodes[i].HTTPAddr()
	if rest == "" {
		return ""
	}
	return "http://" + rest
}

// Addr implements cluster.Cluster.
func (b *LocalCluster) Addr(ctx context.Context, i int, port string) string {
	return net.JoinHostPort(b.Nodes[i].AdvertiseAddr(), port)
}

// Hostname implements cluster.Cluster.
func (b *LocalCluster) Hostname(i int) string {
	return b.IPAddr(i)
}
