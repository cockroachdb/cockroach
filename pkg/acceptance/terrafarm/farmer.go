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

package terrafarm

import (
	"bytes"
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

type process struct {
	session *ssh.Session
	name    string
	done    chan error
}

type node struct {
	hostname     string
	cockroachPID string
	cockroachURL string
	photosURL    string
	ssh          *ssh.Client
	processes    map[string]process
}

// A Farmer sets up and manipulates a test cluster via terraform.
type Farmer struct {
	Output      io.Writer
	Cwd, LogDir string
	KeyName     string
	// SkipClusterInit controls the --join flags for the nodes. If false (the
	// default), then the first node will be empty and thus init the cluster,
	// and each node will have the previous node as its join flag. If true,
	// then all nodes will have all nodes in their join flags.
	//
	// Allows tests to work around https://github.com/cockroachdb/cockroach/issues/13027.
	SkipClusterInit bool
	CockroachBinary string
	CockroachFlags  string
	// NB: CockroachEnv might look like it wants to be a map, but we never use
	// the environment as key-value pairs in Go, we just pass this straight
	// through to the shell when we start cockroach on the remote hosts. The
	// reason for doing it this way rather than using
	// `(*golang.org/x/crypto/ssh.Session).SetEnv` is that setting the
	// session's environment requires server permission, and the default is to
	// allow nothing.
	CockroachEnv  string
	BenchmarkName string
	// TerraformArgs are appended to every Terraform command invocation.
	TerraformArgs []string
	// Prefix will be prepended all names of resources created by Terraform.
	Prefix string
	// StateFile is the file (under `Cwd`) in which Terraform will store its
	// state.
	StateFile   string
	KeepCluster string
	nodes       []node
	// RPCContext is used to open an ExternalClient which provides a KV connection
	// to the cluster by gRPC.
	RPCContext *rpc.Context
}

// Hostname implements the Cluster interface.
func (f *Farmer) Hostname(i int) string {
	return f.nodes[i].hostname
}

// NumNodes returns the number of nodes.
func (f *Farmer) NumNodes() int {
	return len(f.nodes)
}

// Resize resizes a cluster given the desired number of nodes.
func (f *Farmer) Resize(nodes int) error {
	if nodes < len(f.nodes) {
		for _, node := range f.nodes[nodes:] {
			if c := node.ssh; c != nil {
				if err := c.Close(); err != nil {
					return err
				}
			}
		}
		f.nodes = f.nodes[:nodes]
	}

	if stdout, stderr, err := f.run("terraform", "init"); err != nil {
		return errors.Wrapf(err, "failed: %s\nstdout:\n%s\nstderr:\n%s", "terraform init", stdout, stderr)
	}

	args := []string{"destroy", "--force"}
	if nodes > 0 {
		if _, err := os.Stat(f.CockroachBinary); err != nil {
			return err
		}
		args = []string{
			"apply",
			"-input=false",
			fmt.Sprintf("-var=cockroach_binary=%s", f.CockroachBinary),
		}
	}

	args = append(
		args,
		"-no-color",
		fmt.Sprintf("-state=%s", f.StateFile),
		fmt.Sprintf("-var=key_name=%s", f.KeyName),
		fmt.Sprintf("-var=num_instances=%d", nodes),
		fmt.Sprintf("-var=prefix=%s", f.Prefix),
	)
	args = append(args, f.TerraformArgs...)

	if stdout, stderr, err := f.run("terraform", args...); err != nil {
		return errors.Wrapf(err, "failed: %s %s\nstdout:\n%s\nstderr:\n%s", "terraform", args, stdout, stderr)
	}

	if nodes > len(f.nodes) {
		hosts := f.output("instances")

		ch := make(chan error, nodes-len(f.nodes))
		for i := len(f.nodes); i < nodes; i++ {
			f.nodes = append(f.nodes, node{
				hostname:  hosts[i],
				processes: make(map[string]process),
			})
			go func(i int) { ch <- f.Restart(context.TODO(), i) }(i)
		}
		var errs []error
		for i := 0; i < cap(ch); i++ {
			if err := <-ch; err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return errors.Errorf("errors restarting cluster: %s", errs)
		}
	}

	return nil
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
func (f *Farmer) CollectLogs() error {
	logDir := f.AbsLogDir()
	if logDir == "" {
		return nil
	}

	for i := 0; i < f.NumNodes(); i++ {
		dst := filepath.Join(logDir, fmt.Sprintf("node.%d", i))

		host := f.Hostname(i)

		c, err := f.getSSH(host, f.defaultKeyFile())
		if err != nil {
			return errors.Wrapf(err, "could not establish ssh connection to %q", host)
		}

		sftp, err := sftp.NewClient(c)
		if err != nil {
			return errors.Wrapf(err, "could not establish stfp connection to %q", host)
		}
		defer sftp.Close()

		src := "logs"

		lstat, err := sftp.Lstat(src)
		if err != nil {
			return errors.Wrapf(err, "could not lstat %q on %q", src, host)
		}
		if lstat.Mode()&os.ModeSymlink != 0 {
			srcRead, err := sftp.ReadLink(src)
			if err != nil {
				return errors.Wrapf(err, "could not read link %q on %q", src, host)
			}
			src = srcRead
		}

		// Remove the existing log directory, because previous nightly runs might
		// have left log files that would be confusing when mixed with log files for
		// the current run.
		if err := os.RemoveAll(dst); err != nil {
			return errors.Wrapf(err, "could not remove old destination directory %q", dst)
		}

		if err := os.Mkdir(dst, 0777); err != nil {
			return errors.Wrapf(err, "could not create destination directory %q", dst)
		}

		for w := sftp.Walk(src); w.Step(); {
			if err := w.Err(); err != nil {
				return errors.Wrapf(err, "error walking %q on %q", src, host)
			}

			if err := func() error {
				srcPath := w.Path()
				srcPathRel, err := filepath.Rel(src, srcPath)
				if err != nil {
					return err
				}
				destPath := filepath.Join(dst, srcPathRel)

				switch mode := w.Stat().Mode(); {
				case mode&os.ModeDir != 0:
					// Skip the top level directory.
					if srcPath == src {
						return nil
					}
					return errors.Wrapf(os.Mkdir(destPath, mode), "could not create destination directory %q", destPath)
				case mode&os.ModeSymlink != 0:
					srcPathRead, err := sftp.ReadLink(srcPath)
					if err != nil {
						return errors.Wrapf(err, "could not read link %q on %q", src, host)
					}
					return os.Symlink(srcPathRead, destPath)
				}

				srcFile, err := sftp.Open(srcPath)
				if err != nil {
					return errors.Wrapf(err, "could not open %q on %q", srcPath, host)
				}
				defer srcFile.Close()

				destFile, err := os.Create(destPath)
				if err != nil {
					return errors.Wrapf(err, "could not create destination file %q", destPath)
				}
				defer destFile.Close()

				if _, err := io.Copy(destFile, srcFile); err != nil {
					return errors.Wrapf(err, "could not copy %q on %q to %q", srcPath, host, destPath)
				}
				return errors.Wrapf(destFile.Close(), "could not close destination file %q", destPath)
			}(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Destroy collects the logs and tears down the cluster.
func (f *Farmer) Destroy(t testing.TB) error {
	if err := f.CollectLogs(); err != nil {
		f.logf("error collecting cluster logs: %s\n", err)
	} else if logDir := f.AbsLogDir(); logDir != "" {
		defer f.logf("logs copied to %s\n", logDir)
	}

	baseDir := f.Cwd
	if !filepath.IsAbs(baseDir) {
		wd, err := os.Getwd()
		if err != nil {
			wd = "acceptance"
		}
		baseDir = filepath.Join(wd, baseDir)
	}
	if (t.Failed() && f.KeepCluster == KeepClusterFailed) ||
		f.KeepCluster == KeepClusterAlways {

		f.logf("not destroying; run:\n(cd %s && terraform destroy -force -state %[2]s && rm %[2]s)\n",
			baseDir, f.StateFile)
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
		return errors.Wrapf(err, "failed: %s\nstdout:\n%s\nstderr:\n%s", cmd, stdout, stderr)
	}
	return nil
}

// NewDB implements the Cluster interface.
func (f *Farmer) NewDB(ctx context.Context, i int) (*gosql.DB, error) {
	return gosql.Open("postgres", f.PGUrl(ctx, i))
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
			err = errors.Wrap(err, "no nodes found")
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
	const cmd = "pidof cockroach"

	for i, node := range f.nodes {
		stdout, stderr, err := f.ssh(f.Hostname(i), f.defaultKeyFile(), cmd)
		if err != nil {
			t.Fatalf("failed: %s: %s\nstdout:\n%s\nstderr:\n%s", cmd, err, stdout, stderr)
		}
		for _, pid := range strings.Fields(strings.TrimSpace(stdout)) {
			if pid == node.cockroachPID {
				continue
			}
			t.Errorf("unexpected cockroach pid %s on node %d; expected %s", pid, i, node.cockroachPID)
		}
	}
}

// AssertAndStop performs the same test as Assert but then proceeds to
// dismantle the cluster.
func (f *Farmer) AssertAndStop(ctx context.Context, t testing.TB) {
	f.Assert(ctx, t)
	f.MustDestroy(t)
}

// ExecCLI runs ./cockroach <args> with sane defaults.
func (f *Farmer) ExecCLI(ctx context.Context, i int, cmd []string) (string, string, error) {
	// TODO(tschottdorf): This doesn't handle escapes properly. May it never
	// have to.
	//
	// TODO(tschottdorf): Exec() knows stdout/stderr, easy to actually return it
	// if needed.
	return "", "", f.Exec(i, strings.Join(cmd, " "))
}

const (
	listeningURLFileName = "cockroachdb-url"
	pidFileName          = "cockroachdb-pid"
)

// Kill terminates the cockroach process running on the given node number.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Kill(ctx context.Context, i int) error {
	if pid := f.nodes[i].cockroachPID; len(pid) > 0 {
		if err := f.Exec(i, fmt.Sprintf("kill -9 %s && rm -f %s %s", pid, listeningURLFileName, pidFileName)); err != nil {
			return err
		}
		f.nodes[i].cockroachPID = ""
	}
	return nil
}

// Restart terminates the cockroach process running on the given node
// number, unless it is already stopped, and restarts it.
// The given integer must be in the range [0,NumNodes()-1].
func (f *Farmer) Restart(ctx context.Context, i int) error {
	if err := f.Kill(ctx, i); err != nil {
		return err
	}

	cmd := fmt.Sprintf(
		"%s ./cockroach start %s --insecure --background --listening-url-file %s --pid-file %s --cache=2GiB --log-dir logs/cockroach",
		f.CockroachEnv,
		f.CockroachFlags,
		listeningURLFileName,
		pidFileName,
	)

	if f.SkipClusterInit || i != 0 {
		// Unless we're the first node and we're bootstrapping this cluster, list
		// every other node in the cluster in the --join flag. (Since we start nodes
		// in parallel, every non-bootstrapping node needs to know about all the
		// other nodes to avoid startup races.)
		hosts := make([]string, len(f.nodes))
		for i := range hosts {
			hosts[i] = f.nodes[i].hostname
		}
		cmd += " --join=" + strings.Join(hosts, ",")
	}

	// Redirect stdout/stderr to a file, or Cockroach will panic when the SSH
	// session closes. Most logging from this command will go to files, but
	// stderr is occasionally used: log.Shout (for example) will try to write to
	// stderr and is used in panic handling, which obviously shouldn't itself be
	// panicing.
	//
	// It's also nice to have stderr and stdout collected on the cluster
	// machines regardless of whether the acceptance test's network connection
	// drops. This is frequently helpful when iterating on the large
	// backup/restore tests with the -tf.keep-cluster=always option.
	cmd += " 1> logs/cockroach.stdout 2> logs/cockroach.stderr"

	c, err := f.getSSH(f.nodes[i].hostname, f.defaultKeyFile())
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	{
		s, err := c.NewSession()
		if err != nil {
			return err
		}
		defer s.Close()

		if err := s.Start(cmd); err != nil {
			return errors.Wrap(err, cmd)
		}
		go func() {
			err := s.Wait()
			done <- errors.Wrapf(err, "failed: %s", cmd)
		}()
	}

	// Wait for the URL file, paying attention to the Cockroach command itself
	// above.
	{
		var err error
		for start := timeutil.Now(); timeutil.Since(start) < 2*time.Minute; time.Sleep(time.Second) {
			select {
			case err := <-done:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err = func() error {
				s, err := c.NewSession()
				if err != nil {
					return err
				}
				defer s.Close()

				const cmd = "cat " + listeningURLFileName
				stdout, err := s.Output(cmd)
				if err != nil {
					return errors.Wrap(err, cmd)
				}
				f.nodes[i].cockroachURL = string(bytes.TrimSpace(stdout))
				// This is pretty ugly, but photos needs a database name in its URL.
				// TODO(cuongdo): remove this when we've fixed photos
				f.nodes[i].photosURL = strings.Replace(f.nodes[i].cockroachURL, "?", "/photos?", 1)
				return nil
			}(); err == nil {
				break
			}
		}
		if err != nil {
			return err
		}
	}

	{
		s, err := c.NewSession()
		if err != nil {
			return err
		}
		defer s.Close()

		const cmd = "cat " + pidFileName
		stdout, err := s.Output(cmd)
		if err != nil {
			return errors.Wrap(err, cmd)
		}

		f.nodes[i].cockroachPID = string(bytes.TrimSpace(stdout))
	}
	return nil
}

// Start starts the given process on the ith node.
func (f *Farmer) Start(ctx context.Context, i int, name string) error {
	if _, ok := f.nodes[i].processes[name]; ok {
		return errors.Errorf("already started process %q", name)
	}
	c, err := f.getSSH(f.nodes[i].hostname, f.defaultKeyFile())
	if err != nil {
		return err
	}
	s, err := c.NewSession()
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	f.nodes[i].processes[name] = process{
		session: s,
		name:    name,
		done:    done,
	}
	cmd := "./" + name
	switch name {
	// TODO(tamird,petermattis): replace this with "kv".
	case "block_writer":
		cmd += fmt.Sprintf(" --tolerate-errors --min-block-bytes=8 --max-block-bytes=128 --benchmark-name %s '%s'", f.BenchmarkName, f.nodes[i].cockroachURL)
	case "photos":
		cmd += fmt.Sprintf(" --users 1 --benchmark-name %s --db '%s'", f.BenchmarkName, f.nodes[i].photosURL)
	}
	cmd += fmt.Sprintf(" 1>logs/%[1]s.stdout 2>logs/%[1]s.stderr", name)
	f.logf("+ node %d: %s\n", i, cmd)
	if err := s.Start(cmd); err != nil {
		return err
	}
	go func() {
		done <- s.Wait()
	}()
	return nil
}

// GetProcDone returns a channel which will receive the named process' exit
// status.
func (f *Farmer) GetProcDone(i int, name string) <-chan error {
	for _, process := range f.nodes[i].processes {
		if process.name == name {
			return process.done
		}
	}
	return nil
}

// Stop stops the given process on the ith node. This is useful for terminating
// a load generator cleanly to get stats outputted upon process termination.
func (f *Farmer) Stop(ctx context.Context, i int, name string) error {
	if process, ok := f.nodes[i].processes[name]; ok {
		delete(f.nodes[i].processes, name)
		return process.session.Close()
	}
	return errors.Errorf("unable to find process %q", name)
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

var flagCLTWriters = flag.Int("clt.writers", -1, "# of load generators to spawn (defaults to # of nodes)")

// StartLoad starts n loadGenerator processes.
func (f *Farmer) StartLoad(ctx context.Context, loadGenerator string) error {
	n := *flagCLTWriters
	if n == -1 {
		n = f.NumNodes()
	} else if n > f.NumNodes() {
		return errors.Errorf("writers (%d) > nodes (%d)", n, f.NumNodes())
	}

	for i := 0; i < n; i++ {
		if err := f.Start(ctx, i, loadGenerator); err != nil {
			return err
		}
	}
	return nil
}
