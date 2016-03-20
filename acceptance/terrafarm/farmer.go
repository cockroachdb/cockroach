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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package terrafarm

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
)

// A Farmer sets up and manipulates a test cluster via terraform.
type Farmer struct {
	Output         io.Writer
	Cwd, LogDir    string
	KeyName        string
	Stores         string
	nodes, writers []string
}

func (f *Farmer) refresh() {
	f.nodes = f.output("instances")
	f.writers = f.output("example_block_writer")
}

// FirstInstance returns the address of the first instance.
func (f *Farmer) FirstInstance() string {
	if len(f.nodes) == 0 {
		f.refresh()
	}
	return f.nodes[0]
}

// Nodes returns a (copied) slice of provisioned nodes' host names.
func (f *Farmer) Nodes() (hosts []string) {
	if len(f.nodes) == 0 {
		f.refresh()
	}
	return append(hosts, f.nodes...)
}

// Writers returns a (copied) slice of provisioned block writers' host names.
func (f *Farmer) Writers() (hosts []string) {
	if len(f.writers)+len(f.nodes) == 0 {
		f.refresh()
	}
	return append(hosts, f.writers...)
}

// NumNodes returns the number of nodes.
func (f *Farmer) NumNodes() int {
	return len(f.Nodes())
}

// NumWriters returns the number of block writers.
func (f *Farmer) NumWriters() int {
	return len(f.Writers())
}

// Add provisions the given number of nodes and block writers, respectively.
func (f *Farmer) Add(nodes, writers int) error {
	nodes += f.NumNodes()
	writers += f.NumWriters()
	args := []string{fmt.Sprintf("--var=num_instances=%d", nodes),
		fmt.Sprintf("--var=stores=%s", f.Stores),
		fmt.Sprintf("--var=example_block_writer_instances=%d", writers)}

	if nodes == 0 && writers == 0 {
		return f.runErr("terraform", f.appendDefaults(append([]string{"destroy", "--force"}, args...))...)
	}
	return f.apply(args...)
}

// Resize is the counterpart to Add which resizes a cluster given
// the desired number of nodes and writers.
func (f *Farmer) Resize(nodes, writers int) error {
	return f.Add(nodes-f.NumNodes(), writers-f.NumWriters())
}

// AbsLogDir returns the absolute log dir to which logs are written.
func (f *Farmer) AbsLogDir() string {
	if f.LogDir == "" || filepath.IsAbs(f.LogDir) {
		return f.LogDir
	}
	return filepath.Clean(filepath.Join(f.Cwd, f.LogDir))
}

// CollectLogs copies all possibly interesting files from all available peers
// if LogDir is not empty.
func (f *Farmer) CollectLogs() {
	if f.LogDir == "" {
		return
	}
	if err := os.MkdirAll(f.AbsLogDir(), 0777); err != nil {
		fmt.Fprint(os.Stderr, err)
		return
	}
	hosts := append(f.Nodes(), f.Writers()...)
	const src = "logs"
	for i, host := range hosts {
		var dest string
		if i < f.NumNodes() {
			dest = "node." + dest
		} else {
			i -= f.NumNodes()
			dest = "writer." + dest
		}
		dest += strconv.Itoa(i)
		if err := f.scp(host, f.defaultKeyFile(), src,
			filepath.Join(f.AbsLogDir(), dest)); err != nil {
			f.logf("error collecting %s from host %s: %s", src, host, err)
		}
	}
}

// Destroy collects the logs and tears down the cluster.
func (f *Farmer) Destroy() error {
	f.CollectLogs()
	if f.LogDir != "" {
		defer f.logf("logs copied to %s", f.AbsLogDir())
	}
	return f.Resize(0, 0)
}

// MustDestroy calls Destroy(), panicking on error.
func (f *Farmer) MustDestroy() {
	if err := f.Destroy(); err != nil {
		panic(err)
	}
}

// Exec executes the given command on the i-th node, returning (in that order)
// stdout, stderr and an error.
func (f *Farmer) Exec(i int, cmd string) error {
	stdout, stderr, err := f.ssh(f.Nodes()[i], f.defaultKeyFile(), cmd)
	if err != nil {
		return fmt.Errorf("failed: %s: %s\nstdout:\n%s\nstderr:\n%s", cmd, err, stdout, stderr)
	}
	return nil
}

// NewClient implements the Cluster interface.
func (f *Farmer) NewClient(t *testing.T, i int) (*client.DB, *stop.Stopper) {
	panic("unimplemented")
}

// PGUrl returns a URL string for the given node postgres server.
func (f *Farmer) PGUrl(i int) string {
	panic("unimplemented")
}

// WaitReady waits until the infrastructure is in a state that *should* allow
// for a healthy cluster. Currently, this means waiting for the load balancer
// to resolve from all nodes.
func (f *Farmer) WaitReady(d time.Duration) error {
	var rOpts = retry.Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Minute,
		Multiplier:     1.5,
	}
	var err error
	for r := retry.Start(rOpts); r.Next(); {
		instance := f.FirstInstance()
		if err != nil || instance == "" {
			err = fmt.Errorf("no nodes found: %v", err)
			continue
		}
		for i := range f.Nodes() {
			if err = f.Exec(i, "nslookup "+instance); err != nil {
				break
			}
		}
		if err == nil {
			return nil
		}
	}
	return err
}

// Assert verifies that the cluster state is as expected (i.e. no unexpected
// restarts or node deaths occurred). Tests can call this periodically to
// ascertain cluster health.
// TODO(tschottdorf): unimplemented when nodes are expected down.
func (f *Farmer) Assert(t *testing.T) {
	for _, item := range []struct {
		typ   string
		hosts []string
	}{
		{"cockroach", f.Nodes()},
		{"block_writer", f.Writers()},
	} {
		for i, host := range item.hosts {
			out, _, err := f.execSupervisor(host, "status "+item.typ)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, "RUNNING") {
				t.Fatalf("%s %d (%s) is down:\n%s", item.typ, i, host, out)
			}
		}
	}
}

// AssertAndStop performs the same test as Assert but then proceeds to
// dismantle the cluster.
func (f *Farmer) AssertAndStop(t *testing.T) {
	if err := f.Destroy(); err != nil {
		t.Fatal(err)
	}
}

// Kill terminates the cockroach process running on the given node number.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Kill(i int) error {
	return f.Exec(i, "pkill -9 cockroach")
}

// Restart terminates the cockroach process running on the given node
// number, unless it is already stopped, and restarts it.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Restart(i int) error {
	_ = f.Kill(i)
	// supervisorctl is horrible with exit codes (cockroachdb/cockroach-prod#59).
	_, _, err := f.execSupervisor(f.Nodes()[i], "start cockroach")
	return err
}

// URL returns the HTTP(s) endpoint.
func (f *Farmer) URL(i int) string {
	return "http://" + net.JoinHostPort(f.Nodes()[i], base.DefaultHTTPPort)
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (f *Farmer) Addr(i int) string {
	return net.JoinHostPort(f.Nodes()[i], base.DefaultHTTPPort)
}

func (f *Farmer) logf(format string, args ...interface{}) {
	if f.Output != nil {
		fmt.Fprintf(f.Output, format, args...)
	}
}
