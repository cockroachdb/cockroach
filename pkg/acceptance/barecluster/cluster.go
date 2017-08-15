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

package barecluster

import (
	"bytes"
	"context"
	"net"
	"os/exec"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/pkg/errors"
)

// BareCluster .
type BareCluster struct {
	*Cluster
}

// Port .
func (b *BareCluster) Port(ctx context.Context, i int) string {
	return b.RPCPort(i)
}

// NumNodes .
func (b *BareCluster) NumNodes() int {
	return len(b.Nodes)
}

// NewClient .
func (b *BareCluster) NewClient(ctx context.Context, i int) (*client.DB, error) {
	return b.Clients[i], nil
}

// PGUrl .
func (b *BareCluster) PGUrl(ctx context.Context, i int) string {
	return b.Nodes[i].PGUrl()
}

// InternalIP .
func (b *BareCluster) InternalIP(ctx context.Context, i int) net.IP {
	ips, err := net.LookupIP(b.IPAddr(i))
	if err != nil {
		panic(err)
	}
	return ips[0]
}

// Assert .
func (b *BareCluster) Assert(ctx context.Context, t testing.TB) {}

// AssertAndStop .
func (b *BareCluster) AssertAndStop(ctx context.Context, t testing.TB) {
	b.Close()
}

// ExecCLI .
func (b *BareCluster) ExecCLI(ctx context.Context, i int, cmd []string) (string, string, error) {
	cmd = append([]string{"../../cockroach"}, cmd...)
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

// Kill .
func (b *BareCluster) Kill(ctx context.Context, i int) error {
	b.Nodes[i].Kill()
	return nil
}

// Restart .
func (b *BareCluster) Restart(ctx context.Context, i int) error {
	b.Nodes[i].Kill()
	b.Nodes[i].Start()
	return nil
}

// URL .
func (b *BareCluster) URL(ctx context.Context, i int) string {
	return "http://" + b.Addr(ctx, i, b.HTTPPort(i))
}

// Addr .
func (b *BareCluster) Addr(ctx context.Context, i int, port string) string {
	return net.JoinHostPort(b.IPAddr(i), port)
}

// Hostname .
func (b *BareCluster) Hostname(i int) string {
	return b.IPAddr(i)
}
