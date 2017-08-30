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

package localcluster

import (
	"bytes"
	"net"
	"os/exec"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/pkg/errors"
)

// LocalCluster implements cluster.Cluster.
type LocalCluster struct {
	*Cluster
}

// Port implements cluster.Cluster.
func (b *LocalCluster) Port(ctx context.Context, i int) string {
	return b.RPCPort(i)
}

// NumNodes implements cluster.Cluster.
func (b *LocalCluster) NumNodes() int {
	return len(b.Nodes)
}

// NewClient implements cluster.Cluster.
func (b *LocalCluster) NewClient(ctx context.Context, i int) (*client.DB, error) {
	return b.Client(i), nil
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
	cmd = append([]string{b.cfg.Binary}, cmd...)
	cmd = append(cmd, "--insecure", "--port", b.Port(ctx, i))
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
	return b.Nodes[i].StartAsync(ctx, b.joins()...)
}

// Restart implements cluster.Cluster.
func (b *LocalCluster) Restart(ctx context.Context, i int) error {
	return <-b.RestartAsync(ctx, i)
}

// URL implements cluster.Cluster.
func (b *LocalCluster) URL(ctx context.Context, i int) string {
	return "http://" + b.Addr(ctx, i, b.HTTPPort(i))
}

// Addr implements cluster.Cluster.
func (b *LocalCluster) Addr(ctx context.Context, i int, port string) string {
	return net.JoinHostPort(b.IPAddr(i), port)
}

// Hostname implements cluster.Cluster.
func (b *LocalCluster) Hostname(i int) string {
	return b.IPAddr(i)
}
