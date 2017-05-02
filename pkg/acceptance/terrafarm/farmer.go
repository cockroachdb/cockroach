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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

type node struct {
	hostname  string
	processes []string
}

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
	nodes       []node
	// RPCContext is used to open an ExternalClient which provides a KV connection
	// to the cluster by gRPC.
	RPCContext *rpc.Context
}

func (f *Farmer) refresh() {
	hosts := f.output("instances")
	f.nodes = make([]node, len(hosts))
	for i, host := range hosts {
		out, _, err := f.execSupervisor(host, "status | grep -F RUNNING | cut -f1 -d' '")
		if err != nil {
			panic(err)
		}
		f.nodes[i] = node{
			hostname:  host,
			processes: strings.Split(out, "\n"),
		}
	}
}

// Hostname implements the Cluster interface.
func (f *Farmer) Hostname(i int) string {
	if len(f.nodes) == 0 {
		f.refresh()
	}
	return f.nodes[i].hostname
}

// NumNodes returns the number of nodes.
func (f *Farmer) NumNodes() int {
	if len(f.nodes) == 0 {
		f.refresh()
	}
	return len(f.nodes)
}

// AddEnvVar adds an environment variable to supervisord.conf when starting cockroach.
func (f *Farmer) AddEnvVar(key, value string) {
	s := fmt.Sprintf("%s=%s", key, value)
	// This is a Terraform variable defined in acceptance/terraform/variables.tf
	// and passed through to the supervisor.conf file through
	// acceptance/terraform/main.tf.
	const envVar = "cockroach_env"
	if env := f.AddVars[envVar]; env == "" {
		f.AddVars[envVar] = s
	} else {
		f.AddVars[envVar] += "," + s
	}
}

// Add provisions the given number of nodes.
func (f *Farmer) Add(nodes int) error {
	nodes += f.NumNodes()
	args := []string{
		fmt.Sprintf("-var=num_instances=\"%d\"", nodes),
		fmt.Sprintf("-var=stores=%s", f.Stores),
	}

	// Disable update checks for test clusters by setting the required environment
	// variable.
	f.AddEnvVar("COCKROACH_SKIP_UPDATE_CHECK", "1")

	for v, val := range f.AddVars {
		args = append(args, fmt.Sprintf(`-var=%s="%s"`, v, val))
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
	const src = "logs"
	for i := 0; i < f.NumNodes(); i++ {
		if err := f.scp(f.Hostname(i), f.defaultKeyFile(), src,
			filepath.Join(f.AbsLogDir(), "node."+strconv.Itoa(i))); err != nil {
			f.logf("error collecting %s from host %s: %s\n", src, f.Hostname(i), err)
		}
	}
}

// Destroy collects the logs and tears down the cluster.
func (f *Farmer) Destroy(t testing.TB) error {
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

		t.Logf("not destroying; run:\n(cd %s && terraform destroy -force -state %s && rm %s)",
			baseDir, f.StateFile, f.StateFile)
		return nil
	}

	if err := f.Resize(0); err != nil {
		return err
	}
	return os.Remove(filepath.Join(baseDir, f.StateFile))
}

// MustDestroy calls Destroy(), fataling on error.
func (f *Farmer) MustDestroy(t testing.TB) {
	if err := f.Destroy(t); err != nil {
		t.Fatal(errors.Wrap(err, "cannot destroy cluster"))
	}
}

// Exec executes the given command on the i-th node.
func (f *Farmer) Exec(i int, cmd string) error {
	stdout, stderr, err := f.ssh(f.Hostname(i), f.defaultKeyFile(), cmd)
	if err != nil {
		return fmt.Errorf("failed: %s: %s\nstdout:\n%s\nstderr:\n%s", cmd, err, stdout, stderr)
	}
	return nil
}

// NewClient implements the Cluster interface.
func (f *Farmer) NewClient(ctx context.Context, i int) (*client.DB, error) {
	conn, err := f.RPCContext.GRPCDial(f.Addr(ctx, i, base.DefaultPort))
	if err != nil {
		return nil, err
	}
	return client.NewDB(client.NewSender(conn), f.RPCContext.LocalClock), nil
}

// PGUrl returns a URL string for the given node postgres server.
func (f *Farmer) PGUrl(ctx context.Context, i int) string {
	host := f.Hostname(i)
	return fmt.Sprintf("postgresql://%s@%s:26257/system?sslmode=disable", security.RootUser, host)
}

// InternalIP returns the address used for inter-node communication.
func (f *Farmer) InternalIP(ctx context.Context, i int) net.IP {
	// TODO(tschottdorf): This is specific to GCE. On AWS, the following
	// might do it: `curl -sS http://instance-data/latest/meta-data/public-ipv4`.
	// See https://flummox-engineering.blogspot.com/2014/01/get-ip-address-of-google-compute-engine.html
	cmd := `curl -sS "http://metadata/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" -H "X-Google-Metadata-Request: True"`
	stdout, stderr, err := f.ssh(f.Hostname(i), f.defaultKeyFile(), cmd)
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
		instance := f.Hostname(0)
		if err != nil || instance == "" {
			err = fmt.Errorf("no nodes found: %v", err)
			continue
		}
		for i := 0; i < f.NumNodes(); i++ {
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
func (f *Farmer) Assert(ctx context.Context, t testing.TB) {
	for _, node := range f.nodes {
		for _, process := range node.processes {
			f.AssertState(ctx, t, node.hostname, process, "RUNNING")
		}
	}
}

// AssertState verifies that on the specified host, the given process (managed
// by supervisord) is in the expected state.
func (f *Farmer) AssertState(ctx context.Context, t testing.TB, host, proc, expState string) {
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
func (f *Farmer) AssertAndStop(ctx context.Context, t testing.TB) {
	f.Assert(ctx, t)
	f.MustDestroy(t)
}

// ExecRoot executes the given command with super-user privileges.
func (f *Farmer) ExecRoot(ctx context.Context, i int, cmd []string) error {
	// TODO(tschottdorf): This doesn't handle escapes properly. May it never
	// have to.
	return f.Exec(i, "sudo "+strings.Join(cmd, " "))
}

// Kill terminates the cockroach process running on the given node number.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Kill(ctx context.Context, i int) error {
	return f.Exec(i, "pkill -9 cockroach")
}

// Restart terminates the cockroach process running on the given node
// number, unless it is already stopped, and restarts it.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Restart(ctx context.Context, i int) error {
	_ = f.Kill(ctx, i)
	// supervisorctl is horrible with exit codes (cockroachdb/cockroach-prod#59).
	_, _, err := f.execSupervisor(f.Hostname(i), "start cockroach")
	return err
}

// Start starts the given process on the ith node.
func (f *Farmer) Start(ctx context.Context, i int, process string) error {
	for _, p := range f.nodes[i].processes {
		if p == process {
			return errors.Errorf("already started process '%s'", process)
		}
	}
	if _, _, err := f.execSupervisor(f.Hostname(i), "start "+process); err != nil {
		return err
	}
	f.nodes[i].processes = append(f.nodes[i].processes, process)
	return nil
}

// Stop stops the given process on the ith node. This is useful for terminating
// a load generator cleanly to get stats outputted upon process termination.
func (f *Farmer) Stop(ctx context.Context, i int, process string) error {
	for idx, p := range f.nodes[i].processes {
		if p == process {
			if _, _, err := f.execSupervisor(f.Hostname(i), "stop "+process); err != nil {
				return err
			}
			f.nodes[i].processes = append(f.nodes[i].processes[:idx], f.nodes[i].processes[idx+1:]...)
			return nil
		}
	}
	return errors.Errorf("unable to find process '%s'", process)
}

// URL returns the HTTP(s) endpoint.
func (f *Farmer) URL(ctx context.Context, i int) string {
	return "http://" + net.JoinHostPort(f.Hostname(i), base.DefaultHTTPPort)
}

// Addr returns the host and port from the node in the format HOST:PORT.
func (f *Farmer) Addr(ctx context.Context, i int, port string) string {
	return net.JoinHostPort(f.Hostname(i), port)
}

func (f *Farmer) logf(format string, args ...interface{}) {
	if f.Output != nil {
		fmt.Fprintf(f.Output, format, args...)
	}
}

// StartLoad starts n loadGenerator processes.
func (f *Farmer) StartLoad(ctx context.Context, loadGenerator string, n int) error {
	if n > f.NumNodes() {
		return errors.Errorf("writers (%d) > nodes (%d)", n, f.NumNodes())
	}

	// We may have to retry restarting the load generators, because CockroachDB
	// might have been started too recently to start accepting connections.
	for i := 0; i < n; i++ {
		if err := util.RetryForDuration(10*time.Second, func() error {
			return f.Start(ctx, i, loadGenerator)
		}); err != nil {
			return err
		}
	}
	return nil
}
