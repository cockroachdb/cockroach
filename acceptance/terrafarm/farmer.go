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
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/pkg/errors"
)

// The constants below are the possible values of the KeepCluster field.
const (
	// KeepClusterAlways lets Farmer always keep the test cluster.
	KeepClusterAlways = "always"
	// KeepClusterFailed lets Farmer keep only failed test clusters.
	KeepClusterFailed = "failed"
	// KeepClusterNever lets Farmer always destroy the test cluster.
	KeepClusterNever = "never"
)

// A Farmer sets up and manipulates a test cluster via terraform.
type Farmer struct {
	Output      io.Writer
	Cwd, LogDir string
	KeyName     string
	Stores      string
	// Prefix will be prepended all names of resources created by Terraform.
	Prefix string
	// StateFile is the file (under `Cwd`) in which Terraform will stores its
	// state.
	StateFile string
	// AddVars are additional Terraform variables to be set during calls to Add.
	AddVars     map[string]string
	KeepCluster string
	nodes       []string
}

func (f *Farmer) refresh() {
	f.nodes = f.output("instances")
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

// NumNodes returns the number of nodes.
func (f *Farmer) NumNodes() int {
	return len(f.Nodes())
}

// Add provisions the given number of nodes.
func (f *Farmer) Add(nodes int) error {
	nodes += f.NumNodes()
	args := []string{
		fmt.Sprintf("-var=num_instances=%d", nodes),
		fmt.Sprintf("-var=stores=%s", f.Stores),
	}

	// Disable update checks for test clusters by setting the required environment
	// variable.
	const skipCheck = "COCKROACH_SKIP_UPDATE_CHECK=1"
	// This is a Terraform variable defined in acceptance/terraform/variables.tf
	// and passed through to the supervisor.conf file through
	// acceptance/terraform/main.tf.
	const envVar = "cockroach_env"
	if env := f.AddVars[envVar]; env == "" {
		f.AddVars[envVar] = skipCheck
	} else {
		f.AddVars[envVar] += "," + skipCheck
	}

	for v, val := range f.AddVars {
		args = append(args, fmt.Sprintf("-var=%s=%s", v, val))
	}

	if nodes == 0 {
		return f.runErr("terraform", f.appendDefaults(append([]string{"destroy", "--force"}, args...))...)
	}
	return f.apply(args...)
}

// Resize is the counterpart to Add which resizes a cluster given
// the desired number of nodes.
func (f *Farmer) Resize(nodes int) error {
	return f.Add(nodes - f.NumNodes())
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
	hosts := append(f.Nodes())
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
			f.logf("error collecting %s from host %s: %s\n", src, host, err)
		}
	}
}

// Destroy collects the logs and tears down the cluster.
func (f *Farmer) Destroy(t *testing.T) error {
	f.CollectLogs()
	if f.LogDir != "" {
		defer f.logf("logs copied to %s\n", f.AbsLogDir())
	}

	wd, err := os.Getwd()
	if err != nil {
		wd = "acceptance"
	}
	baseDir := filepath.Join(wd, f.Cwd)
	if (t.Failed() && f.KeepCluster == KeepClusterFailed) ||
		f.KeepCluster == KeepClusterAlways {

		t.Logf("not destroying; run:\n(cd %s && terraform destroy -force -state %s)",
			baseDir, f.StateFile)
		return nil
	}

	if err := f.Resize(0); err != nil {
		return err
	}
	return os.Remove(filepath.Join(baseDir, f.StateFile))
}

// MustDestroy calls Destroy(), fataling on error.
func (f *Farmer) MustDestroy(t *testing.T) {
	if err := f.Destroy(t); err != nil {
		t.Fatal(errors.Wrap(err, "cannot destroy cluster"))
	}
}

// Exec executes the given command on the i-th node.
func (f *Farmer) Exec(i int, cmd string) error {
	stdout, stderr, err := f.ssh(f.Nodes()[i], f.defaultKeyFile(), cmd)
	if err != nil {
		return fmt.Errorf("failed: %s: %s\nstdout:\n%s\nstderr:\n%s", cmd, err, stdout, stderr)
	}
	return nil
}

// NewClient implements the Cluster interface.
func (f *Farmer) NewClient(t *testing.T, i int) (*client.DB, *stop.Stopper) {
	stopper := stop.NewStopper()
	rpcContext := rpc.NewContext(&base.Context{
		Insecure: true,
		User:     security.NodeUser,
	}, nil, stopper)
	sender, err := client.NewSender(rpcContext, f.Addr(i, base.DefaultPort))
	if err != nil {
		t.Fatal(err)
	}
	return client.NewDB(sender), stopper
}

// PGUrl returns a URL string for the given node postgres server.
func (f *Farmer) PGUrl(i int) string {
	host := f.Nodes()[i]
	return fmt.Sprintf("postgresql://%s@%s:26257/system?sslmode=disable", security.RootUser, host)
}

// InternalIP returns the address used for inter-node communication.
func (f *Farmer) InternalIP(i int) net.IP {
	// TODO(tschottdorf): This is specific to GCE. On AWS, the following
	// might do it: `curl -sS http://instance-data/latest/meta-data/public-ipv4`.
	// See https://flummox-engineering.blogspot.com/2014/01/get-ip-address-of-google-compute-engine.html
	cmd := `curl -sS "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "X-Google-Metadata-Request: True"`
	stdout, stderr, err := f.ssh(f.Nodes()[i], f.defaultKeyFile(), cmd)
	if err != nil {
		panic(errors.Wrapf(err, stderr+"\n"+stdout))
	}
	stdout = strings.TrimSpace(stdout)
	ip := net.ParseIP(stdout)
	if ip == nil {
		panic(fmt.Sprintf("'%s' did not parse to an IP", stdout))
	}
	return ip
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
// TODO(cuongdo): doesn't handle load generators (photos, block_writer)
func (f *Farmer) Assert(t *testing.T) {
	for _, item := range []struct {
		typ   string
		hosts []string
	}{
		{"cockroach", f.Nodes()},
	} {
		for _, host := range item.hosts {
			f.AssertState(t, host, item.typ, "RUNNING")
		}
	}
}

// AssertState verifies that on the specified host, the given process (managed
// by supervisord) is in the expected state.
func (f *Farmer) AssertState(t *testing.T, host, proc, expState string) {
	out, _, err := f.execSupervisor(host, "status "+proc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, expState) {
		t.Fatalf("%s (%s) is not in expected state %s:\n%s", proc, host, expState, out)
	}
}

// AssertAndStop performs the same test as Assert but then proceeds to
// dismantle the cluster.
func (f *Farmer) AssertAndStop(t *testing.T) {
	f.Assert(t)
	f.MustDestroy(t)
}

// ExecRoot executes the given command with super-user privileges.
func (f *Farmer) ExecRoot(i int, cmd []string) error {
	// TODO(tschottdorf): This doesn't handle escapes properly. May it never
	// have to.
	return f.Exec(i, "sudo "+strings.Join(cmd, " "))
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

// Start starts the given process on the ith node.
func (f *Farmer) Start(i int, process string) error {
	_, _, err := f.execSupervisor(f.Nodes()[i], "start "+process)
	return err
}

// Stop stops the given process on the ith node. This is useful for terminating
// a load generator cleanly to get stats outputted upon process termination.
func (f *Farmer) Stop(i int, process string) error {
	_, _, err := f.execSupervisor(f.Nodes()[i], "stop "+process)
	return err
}

// URL returns the HTTP(s) endpoint.
func (f *Farmer) URL(i int) string {
	return "http://" + net.JoinHostPort(f.Nodes()[i], base.DefaultHTTPPort)
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (f *Farmer) Addr(i int, port string) string {
	return net.JoinHostPort(f.Nodes()[i], port)
}

func (f *Farmer) logf(format string, args ...interface{}) {
	if f.Output != nil {
		fmt.Fprintf(f.Output, format, args...)
	}
}
