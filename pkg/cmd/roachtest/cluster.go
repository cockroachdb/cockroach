// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// "postgres" gosql driver
	_ "github.com/lib/pq"

	"github.com/armon/circbuf"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	local       bool
	cockroach   string
	cloud       = "gce"
	encrypt     bool
	workload    string
	roachprod   string
	buildTag    string
	clusterName string
	clusterID   string
	clusterWipe bool
	zonesF      string
	teamCity    bool
	// For the "list" command: list benchmarks instead of tests.
	listBench bool
)

func ifLocal(trueVal, falseVal string) string {
	if local {
		return trueVal
	}
	return falseVal
}

func filepathAbs(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", errors.Wrap(err, "")
	}
	return path, nil
}

func findBinary(binary, defValue string) (string, error) {
	if binary == "" {
		binary = defValue
	}

	// Check to see if binary exists and is a regular file and executable.
	if fi, err := os.Stat(binary); err == nil && fi.Mode().IsRegular() && (fi.Mode()&0111) != 0 {
		return filepathAbs(binary)
	}

	// Find the binary to run and translate it to an absolute path. First, look
	// for the binary in PATH.
	path, err := exec.LookPath(binary)
	if err != nil {
		if strings.HasPrefix(binary, "/") {
			return "", errors.Wrap(err, "")
		}
		// We're unable to find the binary in PATH and "binary" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			gopath = filepath.Join(os.Getenv("HOME"), "go")
		}

		var binSuffix string
		if !local {
			binSuffix = ".docker_amd64"
		}
		dirs := []string{
			"/src/github.com/cockroachdb/cockroach/",
			"/src/github.com/cockroachdb/cockroach/bin" + binSuffix,
			filepath.Join(os.ExpandEnv("PWD"), "bin"+binSuffix),
		}
		for _, dir := range dirs {
			path = filepath.Join(gopath, dir, binary)
			var err2 error
			path, err2 = exec.LookPath(path)
			if err2 == nil {
				return filepathAbs(path)
			}
		}
		return "", errors.Wrap(err, "")
	}
	return filepathAbs(path)
}

func initBinaries() {
	// If we're running against an existing "local" cluster, force the local flag
	// to true in order to get the "local" test configurations.
	if clusterName == "local" {
		local = true
	}

	cockroachDefault := "cockroach"
	if !local {
		cockroachDefault = "cockroach-linux-2.6.32-gnu-amd64"
	}
	var err error
	cockroach, err = findBinary(cockroach, cockroachDefault)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}

	workload, err = findBinary(workload, "workload")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}

	roachprod, err = findBinary(roachprod, "roachprod")
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

var clusters = map[*cluster]struct{}{}
var clustersMu syncutil.Mutex
var interrupted int32

func destroyAllClusters() {
	atomic.StoreInt32(&interrupted, 1)

	// Fire off a goroutine to destroy all of the clusters.
	done := make(chan struct{})
	go func() {
		defer close(done)

		var wg sync.WaitGroup
		clustersMu.Lock()
		wg.Add(len(clusters))
		for c := range clusters {
			go func(c *cluster) {
				defer wg.Done()
				c.destroy(context.Background())
			}(c)
		}
		clusters = map[*cluster]struct{}{}
		clustersMu.Unlock()

		wg.Wait()
	}()

	// Wait up to 5 min for clusters to be destroyed. This can take a while and
	// we don't want to rush it.
	select {
	case <-done:
	case <-time.After(5 * time.Minute):
	}
}

func registerCluster(c *cluster) {
	clustersMu.Lock()
	clusters[c] = struct{}{}
	clustersMu.Unlock()
}

func unregisterCluster(c *cluster) bool {
	clustersMu.Lock()
	_, exists := clusters[c]
	if exists {
		delete(clusters, c)
	}
	clustersMu.Unlock()
	return exists
}

func execCmd(ctx context.Context, l *logger, args ...string) error {
	// NB: It is important that this waitgroup Waits after cancel() below.
	var wg sync.WaitGroup
	defer wg.Wait()

	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	l.Printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)

	debugStdoutBuffer, _ := circbuf.NewBuffer(1024)
	debugStderrBuffer, _ := circbuf.NewBuffer(1024)

	// Do a dance around https://github.com/golang/go/issues/23019.
	// Briefly put, passing os.Std{out,err} to subprocesses isn't great for
	// context cancellation as Run() will wait for any subprocesses to finish.
	// For example, "roachprod run x -- sleep 20" would wait 20 seconds, even
	// if the context got canceled right away. Work around the problem by passing
	// pipes to the command on which we set aggressive deadlines once the context
	// expires.
	{
		rOut, wOut, err := os.Pipe()
		if err != nil {
			return err
		}
		defer rOut.Close()
		defer wOut.Close()
		rErr, wErr, err := os.Pipe()
		if err != nil {
			return err
		}
		defer rErr.Close()
		defer wErr.Close()

		cmd.Stdout = wOut
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(l.stdout, io.TeeReader(rOut, debugStdoutBuffer))
		}()

		if l.stderr == l.stdout {
			// If l.stderr == l.stdout, we use only one pipe to avoid
			// duplicating everything.
			wg.Done()
			cmd.Stderr = wOut
		} else {
			cmd.Stderr = wErr
			go func() {
				defer wg.Done()
				_, _ = io.Copy(l.stderr, io.TeeReader(rErr, debugStderrBuffer))
			}()
		}

		go func() {
			defer wg.Done()
			<-ctx.Done()
			// NB: setting a more aggressive deadline here makes TestClusterMonitor flaky.
			now := timeutil.Now().Add(3 * time.Second)
			_ = rOut.SetDeadline(now)
			_ = wOut.SetDeadline(now)
			_ = rErr.SetDeadline(now)
			_ = wErr.SetDeadline(now)
		}()
	}

	if err := cmd.Run(); err != nil {
		cancel()
		wg.Wait() // synchronize access to ring buffer
		return errors.Wrapf(
			err,
			"%s returned:\nstderr:\n%s\nstdout:\n%s",
			strings.Join(args, " "),
			debugStderrBuffer.String(),
			debugStdoutBuffer.String(),
		)
	}
	return nil
}

func execCmdWithBuffer(ctx context.Context, l *logger, args ...string) ([]byte, error) {
	l.Printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, errors.Wrapf(err, `%s`, strings.Join(args, ` `))
	}
	return out, nil
}

func makeGCEClusterName(name string) string {
	name = strings.ToLower(name)
	name = regexp.MustCompile(`[^-a-z0-9]+`).ReplaceAllString(name, "-")
	name = regexp.MustCompile(`-+`).ReplaceAllString(name, "-")
	return name
}

func makeClusterName(name string) string {
	return makeGCEClusterName(name)
}

func machineTypeToCPUs(s string) int {
	{
		// GCE machine types.
		var v int
		if _, err := fmt.Sscanf(s, "n1-standard-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n1-highcpu-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n1-highmem-%d", &v); err == nil {
			return v
		}
	}

	// AWS machine types. Yeah, there is probably something more algorithmic that
	// could be done here, but does it matter?
	switch s {
	case "m5.large":
		return 2
	case "m5.xlarge":
		return 4
	case "m5.2xlarge":
		return 8
	case "m5.4xlarge":
		return 16
	case "m5.12xlarge":
		return 48
	case "m5.24xlarge":
		return 96

	case "m5d.large":
		return 2
	case "m5d.xlarge":
		return 4
	case "m5d.2xlarge":
		return 8
	case "m5d.4xlarge":
		return 16
	case "m5d.12xlarge":
		return 48
	case "m5d.24xlarge":
		return 96

	case "i3.large":
		return 2
	case "i3.xlarge":
		return 4
	case "i3.2xlarge":
		return 8
	case "i3.4xlarge":
		return 16
	case "i3.8xlarge":
		return 32
	case "i3.16xlarge":
		return 64
	case "i3.metal":
		return 72
	}

	fmt.Fprintf(os.Stderr, "unknown machine type: %s\n", s)
	os.Exit(1)
	return -1
}

func awsMachineType(cpus int) string {
	switch {
	case cpus <= 2:
		return "m5d.large"
	case cpus <= 4:
		return "m5d.xlarge"
	case cpus <= 8:
		return "m5d.2xlarge"
	case cpus <= 16:
		return "m5d.4xlarge"
	case cpus <= 48:
		return "m5d.12xlarge"
	default:
		return "m5d.24xlarge"
	}
}

func gceMachineType(cpus int) string {
	// TODO(peter): This is awkward: below 16 cpus, use n1-standard so that the
	// machines have a decent amount of RAM. We could use customer machine
	// configurations, but the rules for the amount of RAM per CPU need to be
	// determined (you can't request any arbitrary amount of RAM).
	if cpus < 16 {
		return fmt.Sprintf("n1-standard-%d", cpus)
	}
	return fmt.Sprintf("n1-highcpu-%d", cpus)
}

type testI interface {
	Name() string
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool
	// Path to a directory where the test is supposed to store its log and other
	// artifacts.
	ArtifactsDir() string
	logger() *logger
}

// TODO(tschottdorf): Consider using a more idiomatic approach in which options
// act upon a config struct:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type option interface {
	option()
}

type nodeSelector interface {
	option
	merge(nodeListOption) nodeListOption
}

type nodeListOption []int

func (n nodeListOption) option() {}

func (n nodeListOption) merge(o nodeListOption) nodeListOption {
	t := make(nodeListOption, 0, len(n)+len(o))
	t = append(t, n...)
	t = append(t, o...)
	sort.Ints([]int(t))
	r := t[:1]
	for i := 1; i < len(t); i++ {
		if r[len(r)-1] != t[i] {
			r = append(r, t[i])
		}
	}
	return r
}

func (n nodeListOption) randNode() nodeListOption {
	return nodeListOption{n[rand.Intn(len(n))]}
}

func (n nodeListOption) String() string {
	if len(n) == 0 {
		return ""
	}

	var buf bytes.Buffer
	buf.WriteByte(':')

	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}

	start, end := -1, -1
	for _, i := range n {
		if start != -1 && end == i-1 {
			end = i
			continue
		}
		if start != -1 {
			appendRange(start, end)
		}
		start, end = i, i
	}
	if start != -1 {
		appendRange(start, end)
	}
	return buf.String()
}

type nodeSpec struct {
	Count    int
	CPUs     int
	Zones    string
	Geo      bool
	Lifetime time.Duration
}

func (s *nodeSpec) String() string {
	str := fmt.Sprintf("n%dcpu%d", s.Count, s.CPUs)
	if s.Geo {
		str += "-geo"
	}
	return str
}

func (s *nodeSpec) args() []string {
	var args []string

	switch cloud {
	case "aws":
		if s.Zones != "" {
			fmt.Fprintf(os.Stderr, "zones spec not yet supported on AWS: %s\n", s.Zones)
			os.Exit(1)
		}
		if s.Geo {
			fmt.Fprintf(os.Stderr, "geo-distributed clusters not yet supported on AWS\n")
			os.Exit(1)
		}

		args = append(args, "--clouds=aws")
	}

	if !local && s.CPUs != 0 {
		switch cloud {
		case "aws":
			args = append(args, "--aws-machine-type-ssd="+awsMachineType(s.CPUs))
		case "gce":
			args = append(args, "--gce-machine-type="+gceMachineType(s.CPUs))
		}
	}
	if s.Zones != "" {
		args = append(args, "--gce-zones="+s.Zones)
	}
	if s.Geo {
		args = append(args, "--geo")
	}
	if s.Lifetime != 0 {
		args = append(args, "--lifetime="+s.Lifetime.String())
	}
	return args
}

func (s *nodeSpec) expiration() time.Time {
	l := s.Lifetime
	if l == 0 {
		l = 12 * time.Hour
	}
	return timeutil.Now().Add(l)
}

type createOption interface {
	apply(spec *nodeSpec)
}

type nodeCPUOption int

func (o nodeCPUOption) apply(spec *nodeSpec) {
	spec.CPUs = int(o)
}

// cpu is a node option which requests nodes with the specified number of CPUs.
func cpu(n int) nodeCPUOption {
	return nodeCPUOption(n)
}

type nodeGeoOption struct{}

func (o nodeGeoOption) apply(spec *nodeSpec) {
	spec.Geo = true
}

// geo is a node option which requests geo-distributed nodes.
func geo() nodeGeoOption {
	return nodeGeoOption{}
}

type nodeZonesOption string

func (o nodeZonesOption) apply(spec *nodeSpec) {
	spec.Zones = string(o)
}

// zones is a node option which requests geo-distributed nodes. Note that this
// overrides the --zones flag and is useful for tests that require running on
// specific zones.
func zones(s string) nodeZonesOption {
	return nodeZonesOption(s)
}

type nodeLifetimeOption time.Duration

func (o nodeLifetimeOption) apply(spec *nodeSpec) {
	spec.Lifetime = time.Duration(o)
}

// nodes is a helper method for creating a []nodeSpec given a node count and
// options.
func nodes(count int, opts ...createOption) []nodeSpec {
	spec := nodeSpec{
		Count: count,
	}
	cpu(4).apply(&spec)
	for _, o := range opts {
		o.apply(&spec)
	}
	return []nodeSpec{spec}
}

// cluster provides an interface for interacting with a set of machines,
// starting and stopping a cockroach cluster on a subset of those machines, and
// running load generators and other operations on the machines.
//
// A cluster is intended to be shared between all the subtests under a root.
// A cluster is safe for concurrent use by multiple goroutines.
type cluster struct {
	name   string
	nodes  int
	status func(...interface{})
	t      testI
	// l is the logger used to log various cluster operations.
	// DEPRECATED for use outside of cluster methods: Use a test's t.l instead.
	// This is generally set to the current test's logger.
	l *logger
	// destroyed is used to coordinate between different goroutines that want to
	// destroy a cluster. It is nil when the cluster should not be destroyed (i.e.
	// when Destroy() should not be called).
	destroyed  chan struct{}
	expiration time.Time
	// owned is set if this instance is responsible for `roachprod destroy`ing the
	// cluster. It is set when a new cluster is created, but not when one is
	// cloned or when we attach to an existing roachprod cluster.
	// If not set, Destroy() only wipes the cluster.
	owned bool
}

type clusterConfig struct {
	// name must be empty if localCluster is specified.
	name  string
	nodes []nodeSpec
	// artifactsDir is the path where log file will be stored.
	artifactsDir string
	localCluster bool
	teeOpt       teeOptType
	user         string
}

// newCluster creates a new roachprod cluster.
//
// NOTE: setTest() needs to be called before a test can use this cluster.
//
// TODO(peter): Should set the lifetime of clusters to 2x the expected test
// duration. The default lifetime of 12h is too long for some tests and will be
// too short for others.
//
// TODO(peter): The nodes spec should really contain a nodeSpec per node. Need
// to figure out how to make that work with `roachprod create`. Perhaps one
// invocation of `roachprod create` per unique node-spec. Are there guarantees
// we're making here about the mapping of nodeSpecs to node IDs?
func newCluster(ctx context.Context, l *logger, cfg clusterConfig) (*cluster, error) {
	if atomic.LoadInt32(&interrupted) == 1 {
		return nil, fmt.Errorf("newCluster interrupted")
	}

	var name string
	if cfg.localCluster {
		if cfg.name != "" {
			log.Fatal(ctx, "can't specify name %q with local flag", cfg.name)
		}
		name = "local" // The roachprod tool understands this magic name.
	} else {
		name = makeClusterName(cfg.user + "-" + cfg.name)
	}

	switch {
	case len(cfg.nodes) == 0:
		// For tests. Return the minimum that makes them happy.
		return &cluster{
			expiration: timeutil.Now().Add(24 * time.Hour),
		}, nil
	case len(cfg.nodes) > 1:
		// TODO(peter): Need a motivating test that has different specs per node.
		return nil, fmt.Errorf("TODO(peter): unsupported nodes spec: %v", cfg.nodes)
	}

	c := &cluster{
		name:       name,
		nodes:      cfg.nodes[0].Count,
		status:     func(...interface{}) {},
		l:          l,
		destroyed:  make(chan struct{}),
		expiration: cfg.nodes[0].expiration(),
		owned:      true,
	}
	registerCluster(c)

	sargs := []string{roachprod, "create", c.name, "-n", fmt.Sprint(c.nodes)}
	sargs = append(sargs, cfg.nodes[0].args()...)
	if !local && zonesF != "" && cfg.nodes[0].Zones == "" {
		sargs = append(sargs, "--gce-zones="+zonesF)
	}

	c.status("creating cluster")
	if err := execCmd(ctx, l, sargs...); err != nil {
		return nil, err
	}

	c.status("running test")
	return c, nil
}

type attachOpt struct {
	skipValidation bool
	// Implies skipWipe.
	skipStop bool
	skipWipe bool
}

// attachToExistingCluster creates a cluster object based on machines that have
// already been already allocated by roachprod.
//
// NOTE: setTest() needs to be called before a test can use this cluster.
func attachToExistingCluster(
	ctx context.Context, name string, l *logger, nodes []nodeSpec, opt attachOpt,
) (*cluster, error) {
	if len(nodes) > 1 {
		// TODO(peter): Need a motivating test that has different specs per node.
		return nil, fmt.Errorf("TODO(peter): unsupported nodes spec: %v", nodes)
	}

	c := &cluster{
		name:       name,
		nodes:      nodes[0].Count,
		status:     func(...interface{}) {},
		l:          l,
		destroyed:  make(chan struct{}),
		expiration: nodes[0].expiration(),
		// If we're attaching to an existing cluster, we're not going to destoy it.
		owned: false,
	}
	registerCluster(c)

	if !opt.skipValidation {
		if err := c.validate(ctx, nodes, l); err != nil {
			return nil, err
		}
	}

	if !opt.skipStop {
		c.status("stopping cluster")
		if err := c.StopE(ctx, c.All()); err != nil {
			return nil, err
		}
		if !opt.skipWipe {
			if clusterWipe {
				if err := c.WipeE(ctx, c.All()); err != nil {
					return nil, err
				}
			} else {
				l.Printf("skipping cluster wipe\n")
			}
		}
	}

	c.status("running test")
	return c, nil
}

// setTest prepares c for being used on behalf of t.
//
// TODO(andrei): Get rid of c.t, c.l and of this method.
func (c *cluster) setTest(t testI) {
	c.t = t
	c.l = t.logger()
	if impl, ok := t.(*test); ok {
		c.status = impl.Status
	}
}

// validateCluster takes a cluster and checks that the reality corresponds to
// the cluster's spec. It's intended to be used with clusters created by
// attachToExistingCluster(); otherwise, clusters create with newCluster() are
// know to be up to spec.
func (c *cluster) validate(ctx context.Context, nodes []nodeSpec, l *logger) error {
	// Perform validation on the existing cluster.
	c.status("checking that existing cluster matches spec")
	sargs := []string{roachprod, "list", c.name, "--json"}
	out, err := execCmdWithBuffer(ctx, l, sargs...)
	if err != nil {
		return err
	}

	// jsonOutput matches the structure of the output from `roachprod list`
	// when in json mode.
	type jsonOutput struct {
		Clusters map[string]struct {
			VMs []struct {
				MachineType string `json:"machine_type"`
			} `json:"vms"`
		} `json:"clusters"`
	}
	var details jsonOutput
	if err := json.Unmarshal(out, &details); err != nil {
		return err
	}

	cDetails, ok := details.Clusters[c.name]
	if !ok {
		return fmt.Errorf("cluster %q not found", c.name)
	}
	if len(cDetails.VMs) < c.nodes {
		return fmt.Errorf("cluster has %d nodes, test requires at least %d", len(cDetails.VMs), c.nodes)
	}
	if cpus := nodes[0].CPUs; cpus != 0 {
		for i, vm := range cDetails.VMs {
			vmCPUs := machineTypeToCPUs(vm.MachineType)
			if vmCPUs < cpus {
				return fmt.Errorf("node %d has %d CPUs, test requires %d", i, vmCPUs, cpus)
			}
		}
	}
	return nil
}

// clone creates a non-owned handle on the same cluster.
//
// NOTE: If the cloning has been done for a subtest, setTest() needs to be
// called on the returned cluster.
//
// TODO(andrei): Get rid of the funky concept of cloning once we implement a
// more principled aproach to wiping and destroying cluster.
func (c *cluster) clone() *cluster {
	cpy := *c
	// This cloned cluster is not taking ownership. The parent retains it.
	cpy.owned = false
	cpy.destroyed = nil
	return &cpy
}

// All returns a node list containing all of the nodes in the cluster.
func (c *cluster) All() nodeListOption {
	return c.Range(1, c.nodes)
}

// All returns a node list containing the nodes [begin,end].
func (c *cluster) Range(begin, end int) nodeListOption {
	if begin < 1 || end > c.nodes {
		c.t.Fatalf("invalid node range: %d-%d (1-%d)", begin, end, c.nodes)
	}
	r := make(nodeListOption, 0, 1+end-begin)
	for i := begin; i <= end; i++ {
		r = append(r, i)
	}
	return r
}

// All returns a node list containing only the node i.
func (c *cluster) Node(i int) nodeListOption {
	return c.Range(i, i)
}

// FetchLogs downloads the logs from the cluster using `roachprod get`.
// The logs will be placed in the test's artifacts dir.
func (c *cluster) FetchLogs(ctx context.Context) error {
	if c.nodes == 0 {
		// For tests.
		return nil
	}

	c.l.Printf("fetching logs\n")
	c.status("fetching logs")

	// Don't hang forever if we can't fetch the logs.
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	path := filepath.Join(c.t.ArtifactsDir(), "logs")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	return execCmd(execCtx, c.l, roachprod, "get", c.name, "logs" /* src */, path /* dest */)
}

func (c *cluster) Destroy(ctx context.Context) {
	if c.nodes == 0 {
		// For tests.
		return
	}

	// Only destroy the cluster if it exists in the cluster registry. The cluster
	// may not exist if the test was interrupted and the teardown machinery is
	// destroying all clusters. (See destroyAllClusters).
	if exists := unregisterCluster(c); exists {
		c.destroy(ctx)
	}
	// If the test was interrupted, another goroutine is destroying the cluster
	// and we need to wait for that to finish before closing the
	// logger. Otherwise, the destruction can get interrupted due to closing the
	// stdout/stderr of the roachprod command.
	<-c.destroyed
	c.l.close()
}

func (c *cluster) destroy(ctx context.Context) {
	defer close(c.destroyed)

	if clusterWipe {
		if c.owned {
			c.status("destroying cluster")
			if err := execCmd(ctx, c.l, roachprod, "destroy", c.name); err != nil {
				c.l.Errorf("%s", err)
			}
		} else {
			c.status("wiping cluster")
			if err := execCmd(ctx, c.l, roachprod, "wipe", c.name); err != nil {
				c.l.Errorf("%s", err)
			}
		}
	} else {
		c.l.Printf("skipping cluster wipe\n")
	}
}

// Run a command with output redirected to the logs instead of to os.Stdout
// (which doesn't go anywhere I've been able to find) Don't use this if you're
// going to call cmd.CombinedOutput or cmd.Output.
func (c *cluster) LoggedCommand(ctx context.Context, arg0 string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, arg0, args...)
	cmd.Stdout = c.l.stdout
	cmd.Stderr = c.l.stderr
	return cmd
}

// Put a local file to all of the machines in a cluster.
func (c *cluster) Put(ctx context.Context, src, dest string, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if atomic.LoadInt32(&interrupted) == 1 {
		c.t.Fatal("interrupted")
	}
	c.status("uploading binary")
	err := execCmd(ctx, c.l, roachprod, "put", c.makeNodes(opts...), src, dest)
	if err != nil {
		c.t.Fatal(err)
	}
}

// GitCloneE clones a git repo from src into dest and checks out origin's
// version of the given branch. The src, dest, and branch arguments must not
// contain shell special characters. GitCloneE unlike GitClone returns an
// error.
func (c *cluster) GitCloneE(
	ctx context.Context, src, dest, branch string, node nodeListOption,
) error {
	return c.RunE(ctx, node, "bash", "-e", "-c", fmt.Sprintf(`'
if ! test -d %s; then
  git clone -b %s --depth 1 %s %s
else
  cd %s
  git fetch origin
  git checkout origin/%s
fi
'`, dest,
		branch, src, dest,
		dest,
		branch))
}

// GitClone clones a git repo from src into dest and checks out origin's
// version of the given branch. The src, dest, and branch arguments must not
// contain shell special characters.
func (c *cluster) GitClone(ctx context.Context, src, dest, branch string, node nodeListOption) {
	if err := c.GitCloneE(ctx, src, dest, branch, node); err != nil {
		c.t.Fatal(err)
	}
}

// startArgs specifies extra arguments that are passed to `roachprod` during `c.Start`.
func startArgs(extraArgs ...string) option {
	return roachprodArgOption(extraArgs)
}

// startArgsDontEncrypt will pass '--encrypt=false' to roachprod regardless of the
// --encrypt flag on roachtest. This is useful for tests that cannot pass with
// encryption enabled.
var startArgsDontEncrypt = startArgs("--encrypt=false")

// racks is an option which specifies the number of racks to partition the nodes
// into.
func racks(n int) option {
	return startArgs(fmt.Sprintf("--racks=%d", n))
}

// stopArgs specifies extra arguments that are passed to `roachprod` during `c.Stop`.
func stopArgs(extraArgs ...string) option {
	return roachprodArgOption(extraArgs)
}

type roachprodArgOption []string

func (o roachprodArgOption) option() {}

func roachprodArgs(opts []option) []string {
	var args []string
	for _, opt := range opts {
		a, ok := opt.(roachprodArgOption)
		if !ok {
			continue
		}
		args = append(args, ([]string)(a)...)
	}
	return args
}

// StartE starts cockroach nodes on a subset of the cluster. The nodes parameter
// can either be a specific node, empty (to indicate all nodes), or a pair of
// nodes indicating a range.
func (c *cluster) StartE(ctx context.Context, opts ...option) error {
	if atomic.LoadInt32(&interrupted) == 1 {
		return fmt.Errorf("cluster.Start() interrupted")
	}
	// If the test failed (indicated by a canceled ctx), short-circuit.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.status("starting cluster")
	defer c.status()
	args := []string{
		roachprod,
		"start",
	}
	args = append(args, roachprodArgs(opts)...)
	args = append(args, c.makeNodes(opts...))
	if encrypt && !argExists(args, "--encrypt") {
		args = append(args, "--encrypt")
	}
	return execCmd(ctx, c.l, args...)
}

// Start is like StartE() except it takes a test and, on error, calls t.Fatal().
func (c *cluster) Start(ctx context.Context, t *test, opts ...option) {
	FatalIfErr(t, c.StartE(ctx, opts...))
}

func argExists(args []string, target string) bool {
	for _, arg := range args {
		if arg == target || strings.HasPrefix(arg, target+"=") {
			return true
		}
	}
	return false
}

// StopE cockroach nodes running on a subset of the cluster. See cluster.Start()
// for a description of the nodes parameter.
func (c *cluster) StopE(ctx context.Context, opts ...option) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	args := []string{
		roachprod,
		"stop",
	}
	args = append(args, roachprodArgs(opts)...)
	args = append(args, c.makeNodes(opts...))
	if atomic.LoadInt32(&interrupted) == 1 {
		return fmt.Errorf("interrupted")
	}
	c.status("stopping cluster")
	defer c.status()
	return execCmd(ctx, c.l, args...)
}

// Stop is like StopE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *cluster) Stop(ctx context.Context, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.StopE(ctx, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// WipeE wipes a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *cluster) WipeE(ctx context.Context, opts ...option) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if atomic.LoadInt32(&interrupted) == 1 {
		return fmt.Errorf("interrupted")
	}
	c.status("wiping cluster")
	defer c.status()
	return execCmd(ctx, c.l, roachprod, "wipe", c.makeNodes(opts...))
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *cluster) Wipe(ctx context.Context, opts ...option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.WipeE(ctx, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// Run a command on the specified node.
func (c *cluster) Run(ctx context.Context, node nodeListOption, args ...string) {
	err := c.RunL(ctx, c.l, node, args...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// Reformat the disk on the specified node.
func (c *cluster) Reformat(ctx context.Context, node nodeListOption, args ...string) {
	err := execCmd(ctx, c.l,
		append([]string{roachprod, "reformat", c.makeNodes(node), "--"}, args...)...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// Install a package in a node
func (c *cluster) Install(ctx context.Context, node nodeListOption, args ...string) {
	err := execCmd(ctx, c.l,
		append([]string{roachprod, "install", c.makeNodes(node), "--"}, args...)...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// RunE runs a command on the specified node, returning an error.
func (c *cluster) RunE(ctx context.Context, node nodeListOption, args ...string) error {
	return c.RunL(ctx, c.l, node, args...)
}

// RunL runs a command on the specified node, returning an error.
func (c *cluster) RunL(ctx context.Context, l *logger, node nodeListOption, args ...string) error {
	if err := c.preRunChecks(); err != nil {
		return err
	}
	return execCmd(ctx, l,
		append([]string{roachprod, "run", c.makeNodes(node), "--"}, args...)...)
}

// preRunChecks runs checks to see if it makes sense to run a command.
func (c *cluster) preRunChecks() error {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return errors.New("test already failed")
	}
	if atomic.LoadInt32(&interrupted) == 1 {
		return errors.New("interrupted")
	}

	return nil
}

// RunWithBuffer runs a command on the specified node, returning the resulting combined stderr
// and stdout or an error.
func (c *cluster) RunWithBuffer(
	ctx context.Context, l *logger, node nodeListOption, args ...string,
) ([]byte, error) {
	if err := c.preRunChecks(); err != nil {
		return nil, err
	}
	return execCmdWithBuffer(ctx, l,
		append([]string{roachprod, "run", c.makeNodes(node), "--"}, args...)...)
}

// RemountNoBarrier remounts the cluster's local SSDs with the nobarrier option.
func (c *cluster) RemountNoBarrier(ctx context.Context) {
	c.Run(ctx, c.All(),
		"sudo", "umount", "/mnt/data1", ";",
		"sudo", "mount", "-o", "discard,defaults,nobarrier",
		"$(awk '/\\/mnt\\/data1/ {print $1}' /etc/mtab)", "/mnt/data1")
}

// pgURL returns the Postgres endpoint for the specified node. It accepts a flag
// specifying whether the URL should include the node's internal or external IP
// address. In general, inter-cluster communication and should use internal IPs
// and communication from a test driver to nodes in a cluster should use
// external IPs.
func (c *cluster) pgURL(ctx context.Context, node nodeListOption, external bool) []string {
	args := []string{`pgurl`}
	if external {
		args = append(args, `--external`)
	}
	args = append(args, c.makeNodes(node))
	cmd := exec.CommandContext(ctx, roachprod, args...)
	output, err := cmd.Output()
	if err != nil {
		fmt.Println(strings.Join(cmd.Args, ` `))
		c.t.Fatal(err)
	}
	urls := strings.Split(strings.TrimSpace(string(output)), " ")
	for i := range urls {
		urls[i] = strings.Trim(urls[i], "'")
	}
	return urls
}

// InternalPGUrl returns the internal Postgres endpoint for the specified nodes.
func (c *cluster) InternalPGUrl(ctx context.Context, node nodeListOption) []string {
	return c.pgURL(ctx, node, false /* external */)
}

// Silence unused warning.
var _ = (&cluster{}).InternalPGUrl

// ExternalPGUrl returns the external Postgres endpoint for the specified nodes.
func (c *cluster) ExternalPGUrl(ctx context.Context, node nodeListOption) []string {
	return c.pgURL(ctx, node, true /* external */)
}

func addrToAdminUIAddr(c *cluster, addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		c.t.Fatal(err)
	}
	webPort, err := strconv.Atoi(port)
	if err != nil {
		c.t.Fatal(err)
	}
	// Roachprod makes Admin UI's port to be node's port + 1.
	return fmt.Sprintf("%s:%d", host, webPort+1)
}

func urlToAddr(c *cluster, pgURL string) string {
	u, err := url.Parse(pgURL)
	if err != nil {
		c.t.Fatal(err)
	}
	return u.Host
}

func addrToIP(c *cluster, addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		c.t.Fatal(err)
	}
	return host
}

// InternalAdminUIAddr returns the internal Admin UI address in the form host:port
// for the specified node.
func (c *cluster) InternalAdminUIAddr(ctx context.Context, node nodeListOption) []string {
	var addrs []string
	for _, u := range c.InternalAddr(ctx, node) {
		addrs = append(addrs, addrToAdminUIAddr(c, u))
	}
	return addrs
}

// ExternalAdminUIAddr returns the internal Admin UI address in the form host:port
// for the specified node.
func (c *cluster) ExternalAdminUIAddr(ctx context.Context, node nodeListOption) []string {
	var addrs []string
	for _, u := range c.ExternalAddr(ctx, node) {
		addrs = append(addrs, addrToAdminUIAddr(c, u))
	}
	return addrs
}

// InternalAddr returns the internal address in the form host:port for the
// specified nodes.
func (c *cluster) InternalAddr(ctx context.Context, node nodeListOption) []string {
	var addrs []string
	for _, u := range c.pgURL(ctx, node, false /* external */) {
		addrs = append(addrs, urlToAddr(c, u))
	}
	return addrs
}

// InternalIP returns the internal IP addresses for the specified nodes.
func (c *cluster) InternalIP(ctx context.Context, node nodeListOption) []string {
	var ips []string
	for _, addr := range c.InternalAddr(ctx, node) {
		ips = append(ips, addrToIP(c, addr))
	}
	return ips
}

// ExternalAddr returns the external address in the form host:port for the
// specified node.
func (c *cluster) ExternalAddr(ctx context.Context, node nodeListOption) []string {
	var addrs []string
	for _, u := range c.pgURL(ctx, node, true /* external */) {
		addrs = append(addrs, urlToAddr(c, u))
	}
	return addrs
}

// ExternalIP returns the external IP addresses for the specified node.
func (c *cluster) ExternalIP(ctx context.Context, node nodeListOption) []string {
	var ips []string
	for _, addr := range c.ExternalAddr(ctx, node) {
		ips = append(ips, addrToIP(c, addr))
	}
	return ips
}

// Silence unused warning.
var _ = (&cluster{}).ExternalIP

// Conn returns a SQL connection to the specified node.
func (c *cluster) Conn(ctx context.Context, node int) *gosql.DB {
	url := c.ExternalPGUrl(ctx, c.Node(node))[0]
	db, err := gosql.Open("postgres", url)
	if err != nil {
		c.t.Fatal(err)
	}
	return db
}

// ConnE returns a SQL connection to the specified node.
func (c *cluster) ConnE(ctx context.Context, node int) (*gosql.DB, error) {
	url := c.ExternalPGUrl(ctx, c.Node(node))[0]
	db, err := gosql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (c *cluster) makeNodes(opts ...option) string {
	var r nodeListOption
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			r = s.merge(r)
		}
	}
	return c.name + r.String()
}

func (c *cluster) isLocal() bool {
	return c.name == "local"
}

// getDiskUsageInBytes does what's on the tin. nodeIdx starts at one.
func getDiskUsageInBytes(
	ctx context.Context, c *cluster, logger *logger, nodeIdx int,
) (int, error) {
	out, err := c.RunWithBuffer(ctx, logger, c.Node(nodeIdx), fmt.Sprint("du -sk {store-dir} | grep -oE '^[0-9]+'"))
	if err != nil {
		return 0, err
	}

	str := string(out)
	// We need this check because sometimes the first line of the roachprod output is a warning
	// about adding an ip to a list of known hosts.
	if strings.Contains(str, "Warning") {
		str = strings.Split(str, "\n")[1]
	}

	size, err := strconv.Atoi(strings.TrimSpace(str))
	if err != nil {
		return 0, err
	}

	return size * 1024, nil
}

type monitor struct {
	t         testI
	l         *logger
	nodes     string
	ctx       context.Context
	cancel    func()
	g         *errgroup.Group
	expDeaths int32 // atomically
}

func newMonitor(ctx context.Context, c *cluster, opts ...option) *monitor {
	m := &monitor{
		t:     c.t,
		l:     c.l,
		nodes: c.makeNodes(opts...),
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.g, m.ctx = errgroup.WithContext(m.ctx)
	return m
}

// ExpectDeath lets the monitor know that a node is about to be killed, and that
// this should be ignored.
func (m *monitor) ExpectDeath() {
	m.ExpectDeaths(1)
}

// ExpectDeaths lets the monitor know that a specific number of nodes are about
// to be killed, and that they should be ignored.
func (m *monitor) ExpectDeaths(count int32) {
	atomic.AddInt32(&m.expDeaths, count)
}

var errGoexit = errors.New("Goexit() was called")

func (m *monitor) Go(fn func(context.Context) error) {
	m.g.Go(func() (err error) {
		var returned bool
		defer func() {
			if returned {
				return
			}
			if r := recover(); r != errGoexit && r != nil {
				// Pass any regular panics through.
				panic(r)
			} else {
				// If the invoked method called runtime.Goexit (such as it
				// happens when it calls t.Fatal), exit with a sentinel error
				// here so that the wrapped errgroup cancels itself.
				//
				// Note that the trick here is that we panicked explicitly below,
				// which somehow "overrides" the Goexit which is supposed to be
				// un-recoverable, but we do need to recover to return an error.
				err = errGoexit
			}
		}()
		if impl, ok := m.t.(*test); ok {
			// Automatically clear the worker status message when the goroutine exits.
			defer impl.WorkerStatus()
		}
		defer func() {
			if !returned {
				if r := recover(); r != nil {
					panic(r)
				}
				panic(errGoexit)
			}
		}()
		err = fn(m.ctx)
		returned = true
		return err
	})
}

func (m *monitor) WaitE() error {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return errors.New("already failed")
	}

	return m.wait(roachprod, "monitor", m.nodes)
}

func (m *monitor) Wait() {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := m.WaitE(); err != nil {
		m.t.Fatal(err)
	}
}

func (m *monitor) wait(args ...string) error {
	// It is surprisingly difficult to get the cancellation semantics exactly
	// right. We need to watch for the "workers" group (m.g) to finish, or for
	// the monitor command to emit an unexpected node failure, or for the monitor
	// command itself to exit. We want to capture whichever error happens first
	// and then cancel the other goroutines. This ordering prevents the usage of
	// an errgroup.Group for the goroutines below. Consider:
	//
	//   g, _ := errgroup.WithContext(m.ctx)
	//   g.Go(func(context.Context) error {
	//     defer m.cancel()
	//     return m.g.Wait()
	//   })
	//
	// Now consider what happens when an error is returned. Before the error
	// reaches the errgroup, we invoke the cancellation closure which can cause
	// the other goroutines to wake up and perhaps race and set the errgroup
	// error first.
	//
	// The solution is to implement our own errgroup mechanism here which allows
	// us to set the error before performing the cancellation.

	var errOnce sync.Once
	var err error
	setErr := func(e error) {
		if e != nil {
			errOnce.Do(func() {
				err = e
			})
		}
	}

	// 1. The first goroutine waits for the worker errgroup to exit.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			m.cancel()
			wg.Done()
		}()
		setErr(m.g.Wait())
	}()

	// 2. The second goroutine forks/execs the monitoring command.
	pipeR, pipeW := io.Pipe()
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeW.Close()
			wg.Done()
			// NB: we explicitly do not want to call m.cancel() here as we want the
			// goroutine that is reading the monitoring events to be able to decide
			// on the error if the monitoring command exits peacefully.
		}()

		monL, err := m.l.ChildLogger(`MONITOR`)
		if err != nil {
			setErr(err)
			return
		}
		defer monL.close()

		cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
		cmd.Stdout = io.MultiWriter(pipeW, monL.stdout)
		cmd.Stderr = monL.stderr
		if err := cmd.Run(); err != nil {
			if err != context.Canceled && !strings.Contains(err.Error(), "killed") {
				// The expected reason for an error is that the monitor was killed due
				// to the context being canceled. Any other error is an actual error.
				setErr(err)
				return
			}
		}
		// Returning will cause the pipe to be closed which will cause the reader
		// goroutine to exit and close the monitoring channel.
	}()

	// 3. The third goroutine reads from the monitoring pipe, watching for any
	// unexpected death events.
	wg.Add(1)
	go func() {
		defer func() {
			_ = pipeR.Close()
			m.cancel()
			wg.Done()
		}()

		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			msg := scanner.Text()
			var id int
			var s string
			if n, _ := fmt.Sscanf(msg, "%d: %s", &id, &s); n == 2 {
				if strings.Contains(s, "dead") && atomic.AddInt32(&m.expDeaths, -1) < 0 {
					setErr(fmt.Errorf("unexpected node event: %s", msg))
					return
				}
			}
		}
	}()

	wg.Wait()
	return err
}

func waitForFullReplication(t *test, db *gosql.DB) {
	for ok := false; !ok; time.Sleep(time.Second) {
		if err := db.QueryRow(
			"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
		).Scan(&ok); err != nil {
			t.Fatal(err)
		}
	}
}
