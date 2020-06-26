// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/circbuf"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

const (
	aws   = "aws"
	gce   = "gce"
	azure = "azure"
)

var (
	local        bool
	cockroach    string
	cloud                     = gce
	encrypt      encryptValue = "false"
	instanceType string
	workload     string
	roachprod    string
	buildTag     string
	clusterName  string
	clusterWipe  bool
	zonesF       string
	teamCity     bool
)

type encryptValue string

func (v *encryptValue) String() string {
	return string(*v)
}

func (v *encryptValue) Set(s string) error {
	if s == "random" {
		*v = encryptValue(s)
		return nil
	}
	t, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	*v = encryptValue(fmt.Sprint(t))
	return nil
}

func (v *encryptValue) asBool() bool {
	if *v == "random" {
		return rand.Intn(2) == 0
	}
	t, err := strconv.ParseBool(string(*v))
	if err != nil {
		return false
	}
	return t
}

func (v *encryptValue) Type() string {
	return "string"
}

func ifLocal(trueVal, falseVal string) string {
	if local {
		return trueVal
	}
	return falseVal
}

func filepathAbs(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", errors.WithStack(err)
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
			return "", errors.WithStack(err)
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
			filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/"),
			filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/bin"+binSuffix),
			filepath.Join(os.ExpandEnv("$PWD"), "bin"+binSuffix),
		}
		for _, dir := range dirs {
			path = filepath.Join(dir, binary)
			var err2 error
			path, err2 = exec.LookPath(path)
			if err2 == nil {
				return filepathAbs(path)
			}
		}
		return "", fmt.Errorf("failed to find %q in $PATH or any of %s", binary, dirs)
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

type clusterRegistry struct {
	mu struct {
		syncutil.Mutex
		clusters map[string]*cluster
		tagCount map[string]int
		// savedClusters keeps track of clusters that have been saved for further
		// debugging. Each cluster comes with a message about the test failure
		// causing it to be saved for debugging.
		savedClusters map[*cluster]string
	}
}

func newClusterRegistry() *clusterRegistry {
	cr := &clusterRegistry{}
	cr.mu.clusters = make(map[string]*cluster)
	cr.mu.savedClusters = make(map[*cluster]string)
	return cr
}

func (r *clusterRegistry) registerCluster(c *cluster) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.clusters[c.name] != nil {
		return fmt.Errorf("cluster named %q already exists in registry", c.name)
	}
	r.mu.clusters[c.name] = c
	return nil
}

func (r *clusterRegistry) unregisterCluster(c *cluster) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.mu.clusters[c.name]; !ok {
		// If the cluster is not registered, no-op. This allows the
		// method to be called defensively.
		return false
	}
	delete(r.mu.clusters, c.name)
	if c.tag != "" {
		if _, ok := r.mu.tagCount[c.tag]; !ok {
			panic(fmt.Sprintf("tagged cluster not accounted for: %s", c))
		}
		r.mu.tagCount[c.tag]--
	}
	return true
}

func (r *clusterRegistry) countForTag(tag string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.tagCount[tag]
}

// markClusterAsSaved marks c such that it will not be destroyed by
// destroyAllClusters.
// msg is a message recording the reason why the cluster is being saved (i.e.
// generally a test failure error).
func (r *clusterRegistry) markClusterAsSaved(c *cluster, msg string) {
	r.mu.Lock()
	r.mu.savedClusters[c] = msg
	r.mu.Unlock()
}

type clusterWithMsg struct {
	*cluster
	savedMsg string
}

// savedClusters returns the list of clusters that have been saved for
// debugging.
func (r *clusterRegistry) savedClusters() []clusterWithMsg {
	r.mu.Lock()
	defer r.mu.Unlock()
	res := make([]clusterWithMsg, len(r.mu.savedClusters))
	i := 0
	for c, msg := range r.mu.savedClusters {
		res[i] = clusterWithMsg{
			cluster:  c,
			savedMsg: msg,
		}
		i++
	}
	sort.Slice(res, func(i, j int) bool {
		return strings.Compare(res[i].name, res[j].name) < 0
	})
	return res
}

// destroyAllClusters destroys all the clusters (except for "saved" ones) and
// blocks until they're destroyed. It responds to context cancelation by
// interrupting the waiting; the cluster destruction itself does not inherit the
// cancelation.
func (r *clusterRegistry) destroyAllClusters(ctx context.Context, l *logger) {
	// Fire off a goroutine to destroy all of the clusters.
	done := make(chan struct{})
	go func() {
		defer close(done)

		var clusters []*cluster
		savedClusters := make(map[*cluster]struct{})
		r.mu.Lock()
		for _, c := range r.mu.clusters {
			clusters = append(clusters, c)
		}
		for c := range r.mu.savedClusters {
			savedClusters[c] = struct{}{}
		}
		r.mu.Unlock()

		var wg sync.WaitGroup
		wg.Add(len(clusters))
		for _, c := range clusters {
			go func(c *cluster) {
				defer wg.Done()
				if _, ok := savedClusters[c]; !ok {
					// We don't close the logger here since the cluster may be still in use
					// by a test, and so the logger might still be needed.
					c.Destroy(ctx, dontCloseLogger, l)
				}
			}(c)
		}

		wg.Wait()
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// execCmd is like execCmdEx, but doesn't return the command's output.
func execCmd(ctx context.Context, l *logger, args ...string) error {
	return execCmdEx(ctx, l, args...).err
}

type cmdRes struct {
	err error
	// stdout and stderr are the commands output. Note that this is truncated and
	// only a tail is returned.
	stdout, stderr string
}

// execCmdEx runs a command and returns its error and output.
//
// Note that the output is truncated; only a tail is returned.
// Also note that if the command exits with an error code, its output is also
// included in cmdRes.err.
func execCmdEx(ctx context.Context, l *logger, args ...string) cmdRes {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	l.Printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)

	debugStdoutBuffer, _ := circbuf.NewBuffer(4096)
	debugStderrBuffer, _ := circbuf.NewBuffer(1024)

	// Do a dance around https://github.com/golang/go/issues/23019.
	// When the command we run launches a subprocess, that subprocess receives
	// a copy of our Command's Stdout/Stderr file descriptor, which effectively
	// means that the file descriptors close only when that subcommand returns.
	// However, proactively killing the subcommand is not really possible - we
	// will only manage to kill the parent process that we launched directly.
	// In practice this means that if we try to react to context cancellation,
	// the pipes we read the output from will wait for the *subprocess* to
	// terminate, leaving us hanging, potentially indefinitely.
	// To work around it, use pipes and set a read deadline on our (read) end of
	// the pipes when we detect a context cancellation.
	//
	// See TestExecCmd for a test.
	var closePipes func(ctx context.Context)
	var wg sync.WaitGroup
	{

		var wOut, wErr, rOut, rErr *os.File
		var cwOnce sync.Once
		closePipes = func(ctx context.Context) {
			// Idempotently closes the writing end of the pipes. This is called either
			// when the process returns or when it was killed due to context
			// cancellation. In the former case, close the writing ends of the pipe
			// so that the copy goroutines started below return (without missing any
			// output). In the context cancellation case, we set a deadline to force
			// the goroutines to quit eagerly. This is important since the command
			// may have duplicated wOut and wErr to its possible subprocesses, which
			// may continue to run for long periods of time, and would otherwise
			// block this command. In theory this is possible also when the command
			// returns on its own accord, so we set a (more lenient) deadline in the
			// first case as well.
			//
			// NB: there's also the option (at least on *nix) to use a process group,
			// but it doesn't look portable:
			// https://medium.com/@felixge/killing-a-child-process-and-all-of-its-children-in-go-54079af94773
			cwOnce.Do(func() {
				if wOut != nil {
					_ = wOut.Close()
				}
				if wErr != nil {
					_ = wErr.Close()
				}
				dur := 10 * time.Second // wait up to 10s for subprocesses
				if ctx.Err() != nil {
					dur = 10 * time.Millisecond
				}
				deadline := timeutil.Now().Add(dur)
				if rOut != nil {
					_ = rOut.SetReadDeadline(deadline)
				}
				if rErr != nil {
					_ = rErr.SetReadDeadline(deadline)
				}
			})
		}
		defer closePipes(ctx)

		var err error
		rOut, wOut, err = os.Pipe()
		if err != nil {
			return cmdRes{err: err}
		}

		rErr, wErr, err = os.Pipe()
		if err != nil {
			return cmdRes{err: err}
		}

		cmd.Stdout = wOut
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(l.stdout, io.TeeReader(rOut, debugStdoutBuffer))
		}()

		if l.stderr == l.stdout {
			// If l.stderr == l.stdout, we use only one pipe to avoid
			// duplicating everything.
			cmd.Stderr = wOut
		} else {
			cmd.Stderr = wErr
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = io.Copy(l.stderr, io.TeeReader(rErr, debugStderrBuffer))
			}()
		}
	}

	err := cmd.Run()
	closePipes(ctx)
	wg.Wait()

	if err != nil {
		// Context errors opaquely appear as "signal killed" when manifested.
		// We surface this error explicitly.
		if ctx.Err() != nil {
			err = errors.CombineErrors(ctx.Err(), err)
		}

		if err != nil {
			err = &withCommandDetails{
				cause:  err,
				cmd:    strings.Join(args, " "),
				stderr: debugStderrBuffer.String(),
				stdout: debugStdoutBuffer.String(),
			}
		}
	}

	return cmdRes{
		err:    err,
		stdout: debugStdoutBuffer.String(),
		stderr: debugStderrBuffer.String(),
	}
}

type withCommandDetails struct {
	cause  error
	cmd    string
	stderr string
	stdout string
}

var _ error = (*withCommandDetails)(nil)
var _ errors.Formatter = (*withCommandDetails)(nil)

// Error implements error.
func (e *withCommandDetails) Error() string { return e.cause.Error() }

// Cause implements causer.
func (e *withCommandDetails) Cause() error { return e.cause }

// Format implements fmt.Formatter.
func (e *withCommandDetails) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// FormatError implements errors.Formatter.
func (e *withCommandDetails) FormatError(p errors.Printer) error {
	p.Printf("%s returned", e.cmd)
	if p.Detail() {
		p.Printf("stderr:\n%s\nstdout:\n%s", e.stderr, e.stdout)
	}
	return e.cause
}

// GetStderr retrieves the stderr output of a command that
// returned with an error, or the empty string if there was no stderr.
func GetStderr(err error) string {
	var c *withCommandDetails
	if errors.As(err, &c) {
		return c.stderr
	}
	return ""
}

// execCmdWithBuffer executes the given command and returns its stdout/stderr
// output. If the return code is not 0, an error is also returned.
// l is used to log the command before running it. No output is logged.
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

// MachineTypeToCPUs returns a CPU count for either a GCE or AWS
// machine type.
func MachineTypeToCPUs(s string) int {
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

	typeAndSize := strings.Split(s, ".")

	if len(typeAndSize) == 2 {
		size := typeAndSize[1]

		switch size {
		case "large":
			return 2
		case "xlarge":
			return 4
		case "2xlarge":
			return 8
		case "4xlarge":
			return 16
		case "9xlarge":
			return 36
		case "12xlarge":
			return 48
		case "18xlarge":
			return 72
		case "24xlarge":
			return 96
		}
	}

	// Azure doesn't have a standard way to size machines.
	// This method is implemented for the default machine type.
	// Not all of Azure machine types contain the number of vCPUs int he size and
	// the sizing naming scheme is dependent on the machine type family.
	switch s {
	case "Standard_D2_v3":
		return 2
	case "Standard_D4_v3":
		return 4
	case "Standard_D8_v3":
		return 8
	case "Standard_D16_v3":
		return 16
	case "Standard_D32_v3":
		return 32
	case "Standard_D48_v3":
		return 48
	case "Standard_D64_v3":
		return 64
	}

	// TODO(pbardea): Non-default Azure machine types are not supported
	// and will return unknown machine type error.
	fmt.Fprintf(os.Stderr, "unknown machine type: %s\n", s)
	os.Exit(1)
	return -1
}

func awsMachineType(cpus int) string {
	switch {
	case cpus <= 2:
		return "c5d.large"
	case cpus <= 4:
		return "c5d.xlarge"
	case cpus <= 8:
		return "c5d.2xlarge"
	case cpus <= 16:
		return "c5d.4xlarge"
	case cpus <= 36:
		return "c5d.9xlarge"
	case cpus <= 72:
		return "c5d.18xlarge"
	case cpus <= 96:
		// There is no c5d.24xlarge.
		return "m5d.24xlarge"
	default:
		panic(fmt.Sprintf("no aws machine type with %d cpus", cpus))
	}
}

// Default GCE machine type when none is specified.
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

func azureMachineType(cpus int) string {
	switch {
	case cpus <= 2:
		return "Standard_D2_v3"
	case cpus <= 4:
		return "Standard_D4_v3"
	case cpus <= 8:
		return "Standard_D8_v3"
	case cpus <= 16:
		return "Standard_D16_v3"
	case cpus <= 36:
		return "Standard_D32_v3"
	case cpus <= 48:
		return "Standard_D48_v3"
	case cpus <= 64:
		return "Standard_D64_v3"
	default:
		panic(fmt.Sprintf("no azure machine type with %d cpus", cpus))
	}
}

func machineTypeFlag(machineType string) string {
	switch cloud {
	case aws:
		if isSSD(machineType) {
			return "--aws-machine-type-ssd"
		}
		return "--aws-machine-type"
	case gce:
		return "--gce-machine-type"
	case azure:
		return "--azure-machine-type"
	default:
		panic(fmt.Sprintf("unsupported cloud: %s\n", cloud))
	}
}

func isSSD(machineType string) bool {
	if cloud != aws {
		panic("can only differentiate SSDs based on machine type on AWS")
	}

	typeAndSize := strings.Split(machineType, ".")
	if len(typeAndSize) == 2 {
		awsType := typeAndSize[0]
		// All SSD machine types that we use end in 'd or begins with i3 (e.g. i3, i3en).
		return strings.HasPrefix(awsType, "i3") || strings.HasSuffix(awsType, "d")
	}

	fmt.Fprint(os.Stderr, "aws machine type does not match expected format 'type.size' (e.g. c5d.4xlarge)", machineType)
	os.Exit(1)
	return false
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

// clusterSpec represents a test's description of what its cluster needs to
// look like. It becomes part of a clusterConfig when the cluster is created.
type clusterSpec struct {
	NodeCount int
	// CPUs is the number of CPUs per node.
	CPUs        int
	Zones       string
	Geo         bool
	Lifetime    time.Duration
	ReusePolicy clusterReusePolicy
}

func makeClusterSpec(nodeCount int, opts ...createOption) clusterSpec {
	spec := clusterSpec{NodeCount: nodeCount}
	defaultOpts := []createOption{cpu(4), nodeLifetimeOption(12 * time.Hour), reuseAny()}
	for _, o := range append(defaultOpts, opts...) {
		o.apply(&spec)
	}
	return spec
}

func clustersCompatible(s1, s2 clusterSpec) bool {
	s1.Lifetime = 0
	s2.Lifetime = 0
	return s1 == s2
}

func (s clusterSpec) String() string {
	str := fmt.Sprintf("n%dcpu%d", s.NodeCount, s.CPUs)
	if s.Geo {
		str += "-geo"
	}
	return str
}

func firstZone(zones string) string {
	return strings.SplitN(zones, ",", 2)[0]
}

func (s *clusterSpec) args() []string {
	var args []string

	switch cloud {
	case aws:
		if s.Zones != "" {
			fmt.Fprintf(os.Stderr, "zones spec not yet supported on AWS: %s\n", s.Zones)
			os.Exit(1)
		}
		if s.Geo {
			fmt.Fprintf(os.Stderr, "geo-distributed clusters not yet supported on AWS\n")
			os.Exit(1)
		}

		args = append(args, "--clouds=aws")
	case azure:
		args = append(args, "--clouds=azure")
	}

	if !local && s.CPUs != 0 {
		// Use the machine type specified as a CLI flag.
		machineType := instanceType
		if len(machineType) == 0 {
			// If no machine type was specified, choose one
			// based on the cloud and CPU count.
			switch cloud {
			case aws:
				machineType = awsMachineType(s.CPUs)
			case gce:
				machineType = gceMachineType(s.CPUs)
			case azure:
				machineType = azureMachineType(s.CPUs)
			}
		}
		if cloud == aws {
			if isSSD(machineType) {
				args = append(args, "--local-ssd=true")
			} else {
				args = append(args, "--local-ssd=false")
			}
		}
		machineTypeArg := machineTypeFlag(machineType) + "=" + machineType
		args = append(args, machineTypeArg)
	}
	if s.Zones != "" {
		switch cloud {
		case gce:
			if s.Geo {
				args = append(args, "--gce-zones="+s.Zones)
			} else {
				args = append(args, "--gce-zones="+firstZone(s.Zones))
			}
		case azure:
			args = append(args, "--azure-locations="+s.Zones)
		default:
			fmt.Fprintf(os.Stderr, "specifying zones is not yet supported on %s", cloud)
			os.Exit(1)
		}
	}
	if s.Geo {
		args = append(args, "--geo")
	}
	if s.Lifetime != 0 {
		args = append(args, "--lifetime="+s.Lifetime.String())
	}
	return args
}

func (s *clusterSpec) expiration() time.Time {
	l := s.Lifetime
	if l == 0 {
		l = 12 * time.Hour
	}
	return timeutil.Now().Add(l)
}

type createOption interface {
	apply(spec *clusterSpec)
}

type nodeCPUOption int

func (o nodeCPUOption) apply(spec *clusterSpec) {
	spec.CPUs = int(o)
}

// cpu is a node option which requests nodes with the specified number of CPUs.
func cpu(n int) nodeCPUOption {
	return nodeCPUOption(n)
}

type nodeGeoOption struct{}

func (o nodeGeoOption) apply(spec *clusterSpec) {
	spec.Geo = true
}

// geo is a node option which requests geo-distributed nodes.
func geo() nodeGeoOption {
	return nodeGeoOption{}
}

type nodeZonesOption string

func (o nodeZonesOption) apply(spec *clusterSpec) {
	spec.Zones = string(o)
}

// zones is a node option which requests geo-distributed nodes. Note that this
// overrides the --zones flag and is useful for tests that require running on
// specific zones.
func zones(s string) nodeZonesOption {
	return nodeZonesOption(s)
}

type nodeLifetimeOption time.Duration

func (o nodeLifetimeOption) apply(spec *clusterSpec) {
	spec.Lifetime = time.Duration(o)
}

// clusterReusePolicy indicates what clusters a particular test can run on and
// who (if anybody) can reuse the cluster after the test has finished running
// (either passing or failing). See the individual policies for details.
//
// Only tests whose cluster spec matches can ever run on the same
// cluster, regardless of this policy.
//
// Clean clusters (freshly-created clusters or cluster on which a test with the
// Any policy ran) are accepted by all policies.
//
// Note that not all combinations of "what cluster can I accept" and "how am I
// soiling this cluster" can be expressed. For example, there's no way to
// express that I'll accept a cluster that was tagged a certain way but after me
// nobody else can reuse the cluster at all.
type clusterReusePolicy interface {
	clusterReusePolicy()
}

// reusePolicyAny means that only clean clusters are accepted and the cluster
// can be used by any other test (i.e. the cluster remains "clean").
type reusePolicyAny struct{}

// reusePolicyNone means that only clean clusters are accepted and the cluster
// cannot be reused afterwards.
type reusePolicyNone struct{}

// reusePolicyTagged means that clusters left over by similarly-tagged tests are
// accepted in addition to clean cluster and, regardless of how the cluster
// started up, it will be tagged with the given tag at the end (so only
// similarly-tagged tests can use it afterwards).
//
// The idea is that a tag identifies a particular way in which a test is soiled,
// since it's common for groups of tests to mess clusters up in similar ways and
// to also be able to reset the cluster when the test starts. It's like a virus
// - if you carry it, you infect a clean host and can otherwise intermingle with
// other hosts that are already infected. Note that using this policy assumes
// that the way in which every test soils the cluster is idempotent.
type reusePolicyTagged struct{ tag string }

func (reusePolicyAny) clusterReusePolicy()    {}
func (reusePolicyNone) clusterReusePolicy()   {}
func (reusePolicyTagged) clusterReusePolicy() {}

type clusterReusePolicyOption struct {
	p clusterReusePolicy
}

func reuseAny() clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyAny{}}
}
func reuseNone() clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyNone{}}
}
func reuseTagged(tag string) clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyTagged{tag: tag}}
}

func (p clusterReusePolicyOption) apply(spec *clusterSpec) {
	spec.ReusePolicy = p.p
}

// cluster provides an interface for interacting with a set of machines,
// starting and stopping a cockroach cluster on a subset of those machines, and
// running load generators and other operations on the machines.
//
// A cluster is safe for concurrent use by multiple goroutines.
type cluster struct {
	name string
	tag  string
	spec clusterSpec
	// status is used to communicate the test's status. The callback is a noop
	// until the cluster is passed to a test, at which point it's hooked up to
	// test.Status().
	status func(...interface{})
	t      testI
	// r is the registry tracking this cluster. Destroying the cluster will
	// unregister it.
	r *clusterRegistry
	// l is the logger used to log various cluster operations.
	// DEPRECATED for use outside of cluster methods: Use a test's t.l instead.
	// This is generally set to the current test's logger.
	l          *logger
	expiration time.Time
	// encryptDefault is true if the cluster should default to having encryption
	// at rest enabled. The default only applies if encryption is not explicitly
	// enabled or disabled by options passed to Start.
	encryptDefault bool

	// destroyState contains state related to the cluster's destruction.
	destroyState destroyState
}

func (c *cluster) String() string {
	return fmt.Sprintf("%s [tag:%s] (%d nodes)", c.name, c.tag, c.spec.NodeCount)
}

type destroyState struct {
	// owned is set if this instance is responsible for `roachprod destroy`ing the
	// cluster. It is set when a new cluster is created, but not when we attach to
	// an existing roachprod cluster.
	// If not set, Destroy() only wipes the cluster.
	owned bool

	// alloc is set if owned is set. If set, it represents resources in a
	// QuotaPool that need to be released when the cluster is destroyed.
	alloc *quotapool.IntAlloc

	mu struct {
		syncutil.Mutex
		loggerClosed bool
		// destroyed is used to coordinate between different goroutines that want to
		// destroy a cluster. It is set once the destroy process starts. It it
		// closed when the destruction is complete.
		destroyed chan struct{}
		// saved is set if this cluster should not be wiped or destroyed. It should
		// be left alone for further debugging. This is kept in sync with the
		// clusterRegistry which maintains a list of all saved clusters.
		saved bool
		// savedMsg records a message describing the reason why the cluster is being
		// saved.
		savedMsg string
	}
}

// closeLogger closes c.l. It can be called multiple times.
func (c *cluster) closeLogger() {
	c.destroyState.mu.Lock()
	defer c.destroyState.mu.Unlock()
	if c.destroyState.mu.loggerClosed {
		return
	}
	c.destroyState.mu.loggerClosed = true
	c.l.close()
}

type clusterConfig struct {
	spec clusterSpec
	// artifactsDir is the path where log file will be stored.
	artifactsDir string
	localCluster bool
	useIOBarrier bool
	alloc        *quotapool.IntAlloc
}

// clusterFactory is a creator of clusters.
type clusterFactory struct {
	// namePrefix is prepended to all cluster names.
	namePrefix string
	// counter is incremented with every new cluster. It's used as part of the cluster's name.
	// Accessed atomically.
	counter uint64
	// The registry with whom all clustered will be registered.
	r *clusterRegistry
	// artifactsDir is the directory in which the cluster creation log file will be placed.
	artifactsDir string
	// sem is a semaphore throttling the creation of clusters (because AWS has
	// ridiculous API calls limits).
	sem chan struct{}
}

func newClusterFactory(
	user string, clustersID string, artifactsDir string, r *clusterRegistry, concurrentCreations int,
) *clusterFactory {
	secs := timeutil.Now().Unix()
	var prefix string
	if clustersID != "" {
		prefix = fmt.Sprintf("%s-%s-%d-", user, clustersID, secs)
	} else {
		prefix = fmt.Sprintf("%s-%d-", user, secs)
	}
	return &clusterFactory{
		sem:          make(chan struct{}, concurrentCreations),
		namePrefix:   prefix,
		artifactsDir: artifactsDir,
		r:            r,
	}
}

// acquireSem blocks until the semaphore allows a new cluster creation. The
// returned function needs to be called when cluster creation finished.
func (f *clusterFactory) acquireSem() func() {
	f.sem <- struct{}{}
	return f.releaseSem
}

func (f *clusterFactory) releaseSem() {
	<-f.sem
}

// newCluster creates a new roachprod cluster.
//
// setStatus is called with status messages indicating the stage of cluster
// creation.
//
// NOTE: setTest() needs to be called before a test can use this cluster.
func (f *clusterFactory) newCluster(
	ctx context.Context, cfg clusterConfig, setStatus func(string), teeOpt teeOptType,
) (*cluster, error) {
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "newCluster")
	}

	var name string
	if cfg.localCluster {
		name = "local" // The roachprod tool understands this magic name.
	} else {
		count := atomic.AddUint64(&f.counter, 1)
		name = makeClusterName(
			fmt.Sprintf("%s-%02d-%s", f.namePrefix, count, cfg.spec.String()))
	}

	if cfg.spec.NodeCount == 0 {
		// For tests. Return the minimum that makes them happy.
		c := &cluster{
			name:       name,
			expiration: timeutil.Now().Add(24 * time.Hour),
			status:     func(...interface{}) {},
			r:          f.r,
		}
		if err := f.r.registerCluster(c); err != nil {
			return nil, err
		}
		return c, nil
	}

	exp := cfg.spec.expiration()
	if cfg.localCluster {
		// Local clusters never expire.
		exp = timeutil.Now().Add(100000 * time.Hour)
	}
	c := &cluster{
		name:           name,
		spec:           cfg.spec,
		status:         func(...interface{}) {},
		expiration:     exp,
		encryptDefault: encrypt.asBool(),
		r:              f.r,
		destroyState: destroyState{
			owned: true,
			alloc: cfg.alloc,
		},
	}

	sargs := []string{roachprod, "create", c.name, "-n", fmt.Sprint(c.spec.NodeCount)}
	sargs = append(sargs, cfg.spec.args()...)
	if !local && zonesF != "" && cfg.spec.Zones == "" {
		if cfg.spec.Geo {
			sargs = append(sargs, "--gce-zones="+zonesF)
		} else {
			sargs = append(sargs, "--gce-zones="+firstZone(zonesF))
		}
	}
	if !cfg.useIOBarrier {
		sargs = append(sargs, "--local-ssd-no-ext4-barrier")
	}

	setStatus("acquring cluster creation semaphore")
	release := f.acquireSem()
	defer release()
	setStatus("roachprod create")
	c.status("creating cluster")

	// Logs for creating a new cluster go to a dedicated log file.
	logPath := filepath.Join(f.artifactsDir, runnerLogsDir, "cluster-create", name+".log")
	l, err := rootLogger(logPath, teeOpt)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	success := false
	// Attempt to create a cluster several times, cause them clouds be flaky that
	// my phone says it's snowing.
	for i := 0; i < 3; i++ {
		err = execCmd(ctx, l, sargs...)
		if err == nil {
			success = true
			break
		}
		l.PrintfCtx(ctx, "Failed to create cluster.")
		if !strings.Contains(GetStderr(err), "already exists") {
			l.PrintfCtx(ctx, "Cleaning up in case it was partially created.")
			c.Destroy(ctx, closeLogger, l)
		} else {
			break
		}
	}
	if !success {
		return nil, err
	}

	if err := f.r.registerCluster(c); err != nil {
		return nil, err
	}

	c.status("idle")
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
	ctx context.Context, name string, l *logger, spec clusterSpec, opt attachOpt, r *clusterRegistry,
) (*cluster, error) {
	exp := spec.expiration()
	if name == "local" {
		exp = timeutil.Now().Add(100000 * time.Hour)
	}
	c := &cluster{
		name:           name,
		spec:           spec,
		status:         func(...interface{}) {},
		l:              l,
		expiration:     exp,
		encryptDefault: encrypt.asBool(),
		destroyState: destroyState{
			// If we're attaching to an existing cluster, we're not going to destoy it.
			owned: false,
		},
		r: r,
	}

	if err := r.registerCluster(c); err != nil {
		return nil, err
	}

	if !opt.skipValidation {
		if err := c.validate(ctx, spec, l); err != nil {
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
				if err := c.WipeE(ctx, l, c.All()); err != nil {
					return nil, err
				}
			} else {
				l.Printf("skipping cluster wipe\n")
			}
		}
	}

	c.status("idle")
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

// StopCockroachGracefullyOnNode stops a running cockroach instance on the requested
// node before a version upgrade.
func (c *cluster) StopCockroachGracefullyOnNode(ctx context.Context, node int) error {
	port := fmt.Sprintf("{pgport:%d}", node)
	// Note that the following command line needs to run against both v2.1
	// and the current branch. Do not change it in a manner that is
	// incompatible with 2.1.
	if err := c.RunE(ctx, c.Node(node), "./cockroach quit --insecure --port="+port); err != nil {
		return err
	}
	// TODO (rohany): This comment below might be out of date.
	// NB: we still call Stop to make sure the process is dead when we try
	// to restart it (or we'll catch an error from the RocksDB dir being
	// locked). This won't happen unless run with --local due to timing.
	// However, it serves as a reminder that `./cockroach quit` doesn't yet
	// work well enough -- ideally all listeners and engines are closed by
	// the time it returns to the client.
	c.Stop(ctx, c.Node(node))
	// TODO(tschottdorf): should return an error. I doubt that we want to
	//  call these *testing.T-style methods on goroutines.
	return nil
}

// Save marks the cluster as "saved" so that it doesn't get destroyed.
func (c *cluster) Save(ctx context.Context, msg string, l *logger) {
	l.PrintfCtx(ctx, "saving cluster %s for debugging (--debug specified)", c)
	// TODO(andrei): should we extend the cluster here? For how long?
	if c.destroyState.owned { // we won't have an alloc for an unowned cluster
		c.destroyState.alloc.Freeze()
	}
	c.r.markClusterAsSaved(c, msg)
	c.destroyState.mu.Lock()
	c.destroyState.mu.saved = true
	c.destroyState.mu.savedMsg = msg
	c.destroyState.mu.Unlock()
}

// validateCluster takes a cluster and checks that the reality corresponds to
// the cluster's spec. It's intended to be used with clusters created by
// attachToExistingCluster(); otherwise, clusters create with newCluster() are
// know to be up to spec.
func (c *cluster) validate(ctx context.Context, nodes clusterSpec, l *logger) error {
	// Perform validation on the existing cluster.
	c.status("checking that existing cluster matches spec")
	sargs := []string{roachprod, "list", c.name, "--json", "--quiet"}
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
	if len(cDetails.VMs) < c.spec.NodeCount {
		return fmt.Errorf("cluster has %d nodes, test requires at least %d", len(cDetails.VMs), c.spec.NodeCount)
	}
	if cpus := nodes.CPUs; cpus != 0 {
		for i, vm := range cDetails.VMs {
			vmCPUs := MachineTypeToCPUs(vm.MachineType)
			// vmCPUs will be negative if the machine type is unknown. Give unknown
			// machine types the benefit of the doubt.
			if vmCPUs > 0 && vmCPUs < cpus {
				return fmt.Errorf("node %d has %d CPUs, test requires %d", i, vmCPUs, cpus)
			}
		}
	}
	return nil
}

// All returns a node list containing all of the nodes in the cluster.
func (c *cluster) All() nodeListOption {
	return c.Range(1, c.spec.NodeCount)
}

// All returns a node list containing the nodes [begin,end].
func (c *cluster) Range(begin, end int) nodeListOption {
	if begin < 1 || end > c.spec.NodeCount {
		c.t.Fatalf("invalid node range: %d-%d (1-%d)", begin, end, c.spec.NodeCount)
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
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	c.l.Printf("fetching logs\n")
	c.status("fetching logs")

	// Don't hang forever if we can't fetch the logs.
	return contextutil.RunWithTimeout(ctx, "fetch logs", 2*time.Minute, func(ctx context.Context) error {
		path := filepath.Join(c.t.ArtifactsDir(), "logs")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}

		return execCmd(ctx, c.l, roachprod, "get", c.name, "logs" /* src */, path /* dest */)
	})
}

// CopyRoachprodState copies the roachprod state directory in to the test
// artifacts.
func (c *cluster) CopyRoachprodState(ctx context.Context) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	const roachprodStateDirName = ".roachprod"
	const roachprodStateName = "roachprod_state"
	u, err := user.Current()
	if err != nil {
		return errors.Wrap(err, "failed to get current user")
	}
	src := filepath.Join(u.HomeDir, roachprodStateDirName)
	dest := filepath.Join(c.t.ArtifactsDir(), roachprodStateName)
	cmd := exec.CommandContext(ctx, "cp", "-r", src, dest)
	output, err := cmd.CombinedOutput()
	return errors.Wrapf(err, "command %q failed: output: %v", cmd.Args, string(output))
}

// FetchDebugZip downloads the debug zip from the cluster using `roachprod ssh`.
// The logs will be placed in the test's artifacts dir.
func (c *cluster) FetchDebugZip(ctx context.Context) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	c.l.Printf("fetching debug zip\n")
	c.status("fetching debug zip")

	// Don't hang forever if we can't fetch the debug zip.
	return contextutil.RunWithTimeout(ctx, "debug zip", 5*time.Minute, func(ctx context.Context) error {
		const zipName = "debug.zip"
		path := filepath.Join(c.t.ArtifactsDir(), zipName)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		// Some nodes might be down, so try to find one that works. We make the
		// assumption that a down node will refuse the connection, so it won't
		// waste our time.
		for i := 1; i <= c.spec.NodeCount; i++ {
			// `./cockroach debug zip` is noisy. Suppress the output unless it fails.
			si := strconv.Itoa(i)
			output, err := execCmdWithBuffer(ctx, c.l, roachprod, "ssh", c.name+":"+si, "--",
				"./cockroach", "debug", "zip", "--url", "{pgurl:"+si+"}", zipName)
			if err != nil {
				c.l.Printf("./cockroach debug zip failed: %s", output)
				if i < c.spec.NodeCount {
					continue
				}
				return err
			}
			return execCmd(ctx, c.l, roachprod, "get", c.name+":"+si, zipName /* src */, path /* dest */)
		}
		return nil
	})
}

// FailOnDeadNodes fails the test if nodes that have a populated data dir are
// found to be not running. It prints both to t.l and the test output.
func (c *cluster) FailOnDeadNodes(ctx context.Context, t *test) {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return
	}

	// Don't hang forever.
	_ = contextutil.RunWithTimeout(ctx, "detect dead nodes", time.Minute, func(ctx context.Context) error {
		output, err := execCmdWithBuffer(
			ctx, t.l, roachprod, "monitor", c.name, "--oneshot", "--ignore-empty-nodes",
		)
		// If there's an error, it means either that the monitor command failed
		// completely, or that it found a dead node worth complaining about.
		if err != nil {
			if ctx.Err() != nil {
				// Don't fail if we timed out.
				return nil
			}
			t.printfAndFail(0 /* skip */, "dead node detection: %s %s", err, output)
		}
		return nil
	})
}

// CheckReplicaDivergenceOnDB runs a fast consistency check of the whole keyspace
// against the provided db. If an inconsistency is found, it returns it in the
// error. Note that this will swallow errors returned directly from the consistency
// check since we know that such spurious errors are possibly without any relation
// to the check having failed.
func (c *cluster) CheckReplicaDivergenceOnDB(ctx context.Context, db *gosql.DB) error {
	// NB: we set a statement_timeout since context cancellation won't work here,
	// see:
	// https://github.com/cockroachdb/cockroach/pull/34520
	//
	// We've seen the consistency checks hang indefinitely in some cases.
	rows, err := db.QueryContext(ctx, `
SET statement_timeout = '3m';
SELECT t.range_id, t.start_key_pretty, t.status, t.detail
FROM
crdb_internal.check_consistency(true, '', '') as t
WHERE t.status NOT IN ('RANGE_CONSISTENT', 'RANGE_INDETERMINATE')`)
	if err != nil {
		// TODO(tbg): the checks can fail for silly reasons like missing gossiped
		// descriptors, etc. -- not worth failing the test for. Ideally this would
		// be rock solid.
		c.l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	var finalErr error
	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if scanErr := rows.Scan(&rangeID, &prettyKey, &status, &detail); err != nil {
			return scanErr
		}
		finalErr = errors.CombineErrors(finalErr,
			errors.Newf("r%d (%s) is inconsistent: %s %s\n", rangeID, prettyKey, status, detail))
	}
	if err := rows.Err(); err != nil {
		finalErr = errors.CombineErrors(finalErr, err)
	}

	return finalErr
}

// FailOnReplicaDivergence fails the test if
// crdb_internal.check_consistency(true, '', '') indicates that any ranges'
// replicas are inconsistent with each other. It uses the first node that
// is up to run the query.
func (c *cluster) FailOnReplicaDivergence(ctx context.Context, t *test) {
	if c.spec.NodeCount < 1 {
		return // unit tests
	}

	// Find a live node to run against, if one exists.
	var db *gosql.DB
	for i := 1; i <= c.spec.NodeCount; i++ {
		// Don't hang forever.
		if err := contextutil.RunWithTimeout(
			ctx, "find live node", 5*time.Second,
			func(ctx context.Context) error {
				db = c.Conn(ctx, i)
				_, err := db.ExecContext(ctx, `;`)
				return err
			},
		); err != nil {
			_ = db.Close()
			db = nil
			continue
		}
		c.l.Printf("running (fast) consistency checks on node %d", i)
		break
	}
	if db == nil {
		c.l.Printf("no live node found, skipping consistency check")
		return
	}
	defer db.Close()

	if err := contextutil.RunWithTimeout(
		ctx, "consistency check", time.Minute,
		func(ctx context.Context) error {
			return c.CheckReplicaDivergenceOnDB(ctx, db)
		},
	); err != nil {
		t.Fatal(err)
	}
}

// FetchDmesg grabs the dmesg logs if possible. This requires being able to run
// `sudo dmesg` on the remote nodes.
func (c *cluster) FetchDmesg(ctx context.Context) error {
	if c.spec.NodeCount == 0 || c.isLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	c.l.Printf("fetching dmesg\n")
	c.status("fetching dmesg")

	// Don't hang forever.
	return contextutil.RunWithTimeout(ctx, "dmesg", 20*time.Second, func(ctx context.Context) error {
		const name = "dmesg.txt"
		path := filepath.Join(c.t.ArtifactsDir(), name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := execCmd(
			ctx, c.l, roachprod, "ssh", c.name, "--",
			"/bin/bash", "-c", "'sudo dmesg > "+name+"'", /* src */
		); err != nil {
			// Don't error out because it might've worked on some nodes. Fetching will
			// error out below but will get everything it can first.
			c.l.Printf("during dmesg fetching: %s", err)
		}
		return execCmd(ctx, c.l, roachprod, "get", c.name, name /* src */, path /* dest */)
	})
}

// FetchJournalctl grabs the journalctl logs if possible. This requires being
// able to run `sudo journalctl` on the remote nodes.
func (c *cluster) FetchJournalctl(ctx context.Context) error {
	if c.spec.NodeCount == 0 || c.isLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab journalctl on local runs.
		return nil
	}

	c.l.Printf("fetching journalctl\n")
	c.status("fetching journalctl")

	// Don't hang forever.
	return contextutil.RunWithTimeout(ctx, "journalctl", 20*time.Second, func(ctx context.Context) error {
		const name = "journalctl.txt"
		path := filepath.Join(c.t.ArtifactsDir(), name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := execCmd(
			ctx, c.l, roachprod, "ssh", c.name, "--",
			"/bin/bash", "-c", "'sudo journalctl > "+name+"'", /* src */
		); err != nil {
			// Don't error out because it might've worked on some nodes. Fetching will
			// error out below but will get everything it can first.
			c.l.Printf("during journalctl fetching: %s", err)
		}
		return execCmd(ctx, c.l, roachprod, "get", c.name, name /* src */, path /* dest */)
	})
}

// FetchCores fetches any core files on the cluster.
func (c *cluster) FetchCores(ctx context.Context) error {
	if c.spec.NodeCount == 0 || c.isLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	if true {
		// TeamCity does not handle giant artifacts well. We'd generally profit
		// from having the cores, but we should push them straight into a temp
		// bucket on S3 instead. OTOH, the ROI of this may be low; I don't know
		// of a recent example where we've wanted the Core dumps.
		c.l.Printf("skipped fetching cores\n")
		return nil
	}

	c.l.Printf("fetching cores\n")
	c.status("fetching cores")

	// Don't hang forever. The core files can be large, so we give a generous
	// timeout.
	return contextutil.RunWithTimeout(ctx, "cores", 60*time.Second, func(ctx context.Context) error {
		path := filepath.Join(c.t.ArtifactsDir(), "cores")
		return execCmd(ctx, c.l, roachprod, "get", c.name, "/mnt/data1/cores" /* src */, path /* dest */)
	})
}

type closeLoggerOpt bool

const (
	closeLogger     closeLoggerOpt = true
	dontCloseLogger                = false
)

// Destroy calls `roachprod destroy` or `roachprod wipe` on the cluster.
// If called while another Destroy() or destroyInner() is in progress, the call
// blocks until that first call finishes.
//
// If c.Save() had previously been called, then Destroy() will not actually
// touch the cluster. It might still close c.l, though.
//
// Cluster destruction errors are swallowed.
//
// lo specifies if c.l should be closed or not. If c.l may still be in use by a
// test (i.e. if this Destroy is happening because of a timeout or a signal),
// then we don't want to close the logger.
// l is the logger that will log this destroy operation.
//
// This method generally does not react to ctx cancelation.
func (c *cluster) Destroy(ctx context.Context, lo closeLoggerOpt, l *logger) {
	if ctx.Err() != nil {
		return
	}
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies not much to do.
		c.r.unregisterCluster(c)
		return
	}

	ch := c.doDestroy(ctx, l)
	<-ch
	// NB: Closing the logger without waiting on c.destroyState.destroyed above
	// would be bad because we might cause the ongoing `roachprod destroy` to fail
	// by closing its stdout/stderr.
	if lo == closeLogger && c.l != nil {
		c.closeLogger()
	}
}

func (c *cluster) doDestroy(ctx context.Context, l *logger) <-chan struct{} {
	var inFlight <-chan struct{}
	c.destroyState.mu.Lock()
	if c.destroyState.mu.saved {
		// Nothing to do. Short-circuit.
		c.destroyState.mu.Unlock()
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if c.destroyState.mu.destroyed == nil {
		c.destroyState.mu.destroyed = make(chan struct{})
	} else {
		inFlight = c.destroyState.mu.destroyed
	}
	c.destroyState.mu.Unlock()
	if inFlight != nil {
		return inFlight
	}

	if clusterWipe {
		if c.destroyState.owned {
			l.PrintfCtx(ctx, "destroying cluster %s...", c)
			c.status("destroying cluster")
			// We use a non-cancelable context for running this command. Once we got
			// here, the cluster cannot be destroyed again, so we really want this
			// command to succeed.
			if err := execCmd(context.Background(), l, roachprod, "destroy", c.name); err != nil {
				l.ErrorfCtx(ctx, "error destroying cluster %s: %s", c, err)
			} else {
				l.PrintfCtx(ctx, "destroying cluster %s... done", c)
			}
			c.destroyState.alloc.Release()
		} else {
			l.PrintfCtx(ctx, "wiping cluster %s", c)
			c.status("wiping cluster")
			if err := execCmd(ctx, l, roachprod, "wipe", c.name); err != nil {
				l.Errorf("%s", err)
			}
		}
	} else {
		l.Printf("skipping cluster wipe\n")
	}
	c.r.unregisterCluster(c)
	c.destroyState.mu.Lock()
	ch := c.destroyState.mu.destroyed
	close(ch)
	c.destroyState.mu.Unlock()
	return ch
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
// Put is DEPRECATED. Use PutE instead.
func (c *cluster) Put(ctx context.Context, src, dest string, opts ...option) {
	if err := c.PutE(ctx, c.l, src, dest, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// PutE puts a local file to all of the machines in a cluster.
func (c *cluster) PutE(ctx context.Context, l *logger, src, dest string, opts ...option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}

	c.status("uploading binary")
	defer c.status("")

	err := execCmd(ctx, c.l, roachprod, "put", c.makeNodes(opts...), src, dest)
	if err != nil {
		return errors.Wrap(err, "cluster.Put")
	}
	return nil
}

// Get gets files from remote hosts.
func (c *cluster) Get(ctx context.Context, l *logger, src, dest string, opts ...option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Get error")
	}
	c.status(fmt.Sprintf("getting %v", src))
	defer c.status("")
	return errors.Wrap(
		execCmd(ctx, l, roachprod, "get", c.makeNodes(opts...), src, dest),
		"cluster.Get error")

}

// Put a string into the specified file on the remote(s).
func (c *cluster) PutString(
	ctx context.Context, content, dest string, mode os.FileMode, opts ...option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.PutString error")
	}
	c.status("uploading string")
	defer c.status("")

	temp, err := ioutil.TempFile("", filepath.Base(dest))
	if err != nil {
		return errors.Wrap(err, "PutString")
	}
	if _, err := temp.WriteString(content); err != nil {
		return errors.Wrap(err, "PutString")
	}
	temp.Close()
	src := temp.Name()

	if err := os.Chmod(src, mode); err != nil {
		return errors.Wrap(err, "PutString")
	}
	// NB: we intentionally don't remove the temp files. This is because roachprod
	// will symlink them when running locally.

	if err := execCmd(ctx, c.l, roachprod, "put", c.makeNodes(opts...), src, dest); err != nil {
		return errors.Wrap(err, "PutString")
	}
	return nil
}

// GitClone clones a git repo from src into dest and checks out origin's
// version of the given branch. The src, dest, and branch arguments must not
// contain shell special characters.
func (c *cluster) GitClone(
	ctx context.Context, l *logger, src, dest, branch string, node nodeListOption,
) error {
	return c.RunL(ctx, l, node, "bash", "-e", "-c", fmt.Sprintf(`'
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

// Restart restarts the specified cockroach node. It takes a test and, on error,
// calls t.Fatal().
func (c *cluster) Restart(ctx context.Context, t *test, node nodeListOption) {
	// We bound the time taken to restart a node through roachprod. Because
	// roachprod uses SSH, it's particularly vulnerable to network flakiness (as
	// seen in #35326) and may stall indefinitely. Setting up timeouts better
	// surfaces this kind of failure.
	//
	// TODO(irfansharif): The underlying issue here is the fact that we're running
	// roachprod commands that may (reasonably) fail due to connection issues, and
	// we're unable to retry them safely (the underlying commands are
	// non-idempotent). Presently we simply fail the entire test, when really we
	// should be able to retry the specific roachprod commands.
	var cancel func()
	ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
	c.Stop(ctx, node)
	c.Start(ctx, t, node)
	cancel()
}

// StartE starts cockroach nodes on a subset of the cluster. The nodes parameter
// can either be a specific node, empty (to indicate all nodes), or a pair of
// nodes indicating a range.
func (c *cluster) StartE(ctx context.Context, opts ...option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.StartE")
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
	if !argExists(args, "--encrypt") && c.encryptDefault {
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
		return errors.Wrap(ctx.Err(), "cluster.StopE")
	}
	args := []string{
		roachprod,
		"stop",
	}
	args = append(args, roachprodArgs(opts)...)
	args = append(args, c.makeNodes(opts...))
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
func (c *cluster) WipeE(ctx context.Context, l *logger, opts ...option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.WipeE")
	}
	if c.spec.NodeCount == 0 {
		// For tests.
		return nil
	}
	c.status("wiping cluster")
	defer c.status()
	return execCmd(ctx, l, roachprod, "wipe", c.makeNodes(opts...))
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *cluster) Wipe(ctx context.Context, opts ...option) {
	if ctx.Err() != nil {
		return
	}
	if err := c.WipeE(ctx, c.l, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// Run a command on the specified node.
func (c *cluster) Run(ctx context.Context, node nodeListOption, args ...string) {
	err := c.RunE(ctx, node, args...)
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

// Silence unused warning.
var _ = (&cluster{}).Reformat

// Install a package in a node
func (c *cluster) Install(
	ctx context.Context, l *logger, node nodeListOption, args ...string,
) error {
	return execCmd(ctx, l,
		append([]string{roachprod, "install", c.makeNodes(node), "--"}, args...)...)
}

var reOnlyAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// cmdLogFileName comes up with a log file to use for the given argument string.
func cmdLogFileName(t time.Time, nodes nodeListOption, args ...string) string {
	// Make sure we treat {"./cockroach start"} like {"./cockroach", "start"}.
	args = strings.Split(strings.Join(args, " "), " ")
	prefix := []string{reOnlyAlphanumeric.ReplaceAllString(args[0], "")}
	for _, arg := range args[1:] {
		if s := reOnlyAlphanumeric.ReplaceAllString(arg, ""); s != arg {
			break
		}
		prefix = append(prefix, arg)
	}
	s := strings.Join(prefix, "_")
	const maxLen = 70
	if len(s) > maxLen {
		s = s[:maxLen]
	}
	logFile := fmt.Sprintf(
		"run_%s_n%s_%s",
		t.Format(`150405.000`),
		nodes.String()[1:],
		s,
	)
	return logFile
}

// RunE runs a command on the specified node, returning an error. The output
// will be redirected to a file which is logged via the cluster-wide logger in
// case of an error. Logs will sort chronologically and those belonging to
// failing invocations will be suffixed `.failed.log`.
func (c *cluster) RunE(ctx context.Context, node nodeListOption, args ...string) error {
	cmdString := strings.Join(args, " ")
	logFile := cmdLogFileName(timeutil.Now(), node, args...)

	// NB: we set no prefix because it's only going to a file anyway.
	l, err := c.l.ChildLogger(logFile, quietStderr, quietStdout)
	if err != nil {
		return err
	}
	c.l.PrintfCtx(ctx, "> %s", cmdString)
	err = c.RunL(ctx, l, node, args...)
	l.Printf("> result: %+v", err)
	if err := ctx.Err(); err != nil {
		l.Printf("(note: incoming context was canceled: %s", err)
	}
	physicalFileName := l.file.Name()
	l.close()
	if err != nil {
		_ = os.Rename(physicalFileName, strings.TrimSuffix(physicalFileName, ".log")+".failed.log")
	}
	err = errors.Wrapf(err, "output in %s", logFile)
	return err
}

// RunL runs a command on the specified node, returning an error.
func (c *cluster) RunL(ctx context.Context, l *logger, node nodeListOption, args ...string) error {
	if err := errors.Wrap(ctx.Err(), "cluster.RunL"); err != nil {
		return err
	}
	return execCmd(ctx, l,
		append([]string{roachprod, "run", c.makeNodes(node), "--"}, args...)...)
}

// RunWithBuffer runs a command on the specified node, returning the resulting combined stderr
// and stdout or an error.
func (c *cluster) RunWithBuffer(
	ctx context.Context, l *logger, node nodeListOption, args ...string,
) ([]byte, error) {
	if err := errors.Wrap(ctx.Err(), "cluster.RunWithBuffer"); err != nil {
		return nil, err
	}
	return execCmdWithBuffer(ctx, l,
		append([]string{roachprod, "run", c.makeNodes(node), "--"}, args...)...)
}

// pgURL returns the Postgres endpoint for the specified node. It accepts a flag
// specifying whether the URL should include the node's internal or external IP
// address. In general, inter-cluster communication and should use internal IPs
// and communication from a test driver to nodes in a cluster should use
// external IPs.
func (c *cluster) pgURL(ctx context.Context, node nodeListOption, external bool) []string {
	args := []string{roachprod, "pgurl"}
	if external {
		args = append(args, `--external`)
	}
	nodes := c.makeNodes(node)
	args = append(args, nodes)
	cmd := execCmdEx(ctx, c.l, args...)
	if cmd.err != nil {
		c.t.Fatal(errors.Wrapf(cmd.err, "failed to get pgurl for nodes: %s", nodes))
	}
	urls := strings.Split(strings.TrimSpace(cmd.stdout), " ")
	if len(urls) != len(node) {
		c.t.Fatalf(
			"pgurl for nodes %v got urls %v from stdout:\n%s\nstderr:\n%s",
			node, urls, cmd.stdout, cmd.stderr,
		)
	}
	for i := range urls {
		urls[i] = strings.Trim(urls[i], "'")
		if urls[i] == "" {
			c.t.Fatalf(
				"pgurl for nodes %s empty: %v from\nstdout:\n%s\nstderr:\n%s",
				urls, node, cmd.stdout, cmd.stderr,
			)
		}
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

func addrToHost(c *cluster, addr string) string {
	host, _ := addrToHostPort(c, addr)
	return host
}

func addrToHostPort(c *cluster, addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		c.t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		c.t.Fatal(err)
	}
	return host, port
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
		ips = append(ips, addrToHost(c, addr))
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
		ips = append(ips, addrToHost(c, addr))
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

// Extend extends the cluster's expiration by d, after truncating d to minute
// granularity.
func (c *cluster) Extend(ctx context.Context, d time.Duration, l *logger) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Extend")
	}
	minutes := int(d.Minutes())
	l.PrintfCtx(ctx, "extending cluster by %d minutes", minutes)
	if out, err := execCmdWithBuffer(ctx, l, roachprod, "extend", c.name,
		fmt.Sprintf("--lifetime=%dm", minutes),
	); err != nil {
		l.PrintfCtx(ctx, "roachprod extend failed: %s", out)
		return errors.Wrap(err, "roachprod extend failed")
	}
	// Update c.expiration. Keep it under the real expiration.
	c.expiration = c.expiration.Add(time.Duration((minutes - 1)) * time.Minute)
	return nil
}

// getDiskUsageInBytes does what's on the tin. nodeIdx starts at one.
func getDiskUsageInBytes(
	ctx context.Context, c *cluster, logger *logger, nodeIdx int,
) (int, error) {
	var out []byte
	for {
		if c.t.Failed() {
			return 0, errors.New("already failed")
		}
		var err error
		// `du` can warn if files get removed out from under it (which
		// happens during RocksDB compactions, for example). Discard its
		// stderr to avoid breaking Atoi later.
		// TODO(bdarnell): Refactor this stack to not combine stdout and
		// stderr so we don't need to do this (and the Warning check
		// below).
		out, err = c.RunWithBuffer(ctx, logger, c.Node(nodeIdx),
			fmt.Sprint("du -sk {store-dir} 2>/dev/null | grep -oE '^[0-9]+'"))
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			// If `du` fails, retry.
			// TODO(bdarnell): is this worth doing? It was originally added
			// because of the "files removed out from under it" problem, but
			// that doesn't result in a command failure, just a stderr
			// message.
			logger.Printf("retrying disk usage computation after spurious error: %s", err)
			continue
		}
		break
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

func (m *monitor) ResetDeaths() {
	atomic.StoreInt32(&m.expDeaths, 0)
}

var errTestFatal = errors.New("t.Fatal() was called")

func (m *monitor) Go(fn func(context.Context) error) {
	m.g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				if r != errTestFatal {
					// Pass any regular panics through.
					panic(r)
				}
				// t.{Skip,Fatal} perform a panic(errTestFatal). If we've caught the
				// errTestFatal sentinel we transform the panic into an error return so
				// that the wrapped errgroup cancels itself.
				err = errTestFatal
			}
		}()
		if impl, ok := m.t.(*test); ok {
			// Automatically clear the worker status message when the goroutine exits.
			defer impl.WorkerStatus()
		}
		return fn(m.ctx)
	})
}

func (m *monitor) WaitE() error {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return errors.New("already failed")
	}

	return errors.Wrap(m.wait(roachprod, "monitor", m.nodes), "monitor failure")
}

func (m *monitor) Wait() {
	if m.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := m.WaitE(); err != nil {
		// Note that we used to avoid fataling again if we had already fatal'ed.
		// However, this error here might be the one to actually report, see:
		// https://github.com/cockroachdb/cockroach/issues/44436
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
		setErr(errors.Wrap(m.g.Wait(), "monitor task failed"))
	}()

	setMonitorCmdErr := func(err error) {
		setErr(errors.Wrap(err, "monitor command failure"))
	}

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
			setMonitorCmdErr(err)
			return
		}
		defer monL.close()

		cmd := exec.CommandContext(m.ctx, args[0], args[1:]...)
		cmd.Stdout = io.MultiWriter(pipeW, monL.stdout)
		cmd.Stderr = monL.stderr
		if err := cmd.Run(); err != nil {
			if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "killed") {
				// The expected reason for an error is that the monitor was killed due
				// to the context being canceled. Any other error is an actual error.
				setMonitorCmdErr(err)
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
	t.l.Printf("waiting for up-replication...\n")
	tStart := timeutil.Now()
	for ok := false; !ok; time.Sleep(time.Second) {
		if err := db.QueryRow(
			"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
		).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		if timeutil.Since(tStart) > 30*time.Second {
			t.l.Printf("still waiting for full replication")
		}
	}
}

type loadGroup struct {
	roachNodes nodeListOption
	loadNodes  nodeListOption
}

type loadGroupList []loadGroup

func (lg loadGroupList) roachNodes() nodeListOption {
	var roachNodes nodeListOption
	for _, g := range lg {
		roachNodes = roachNodes.merge(g.roachNodes)
	}
	return roachNodes
}

func (lg loadGroupList) loadNodes() nodeListOption {
	var loadNodes nodeListOption
	for _, g := range lg {
		loadNodes = loadNodes.merge(g.loadNodes)
	}
	return loadNodes
}

// makeLoadGroups create a loadGroupList that has an equal number of cockroach
// nodes per zone. It assumes that numLoadNodes <= numZones and that numZones is
// divisible by numLoadNodes.
func makeLoadGroups(c *cluster, numZones, numRoachNodes, numLoadNodes int) loadGroupList {
	if numLoadNodes > numZones {
		panic("cannot have more than one load node per zone")
	} else if numZones%numLoadNodes != 0 {
		panic("numZones must be divisible by numLoadNodes")
	}
	// roachprod allocates nodes over regions in a round-robin fashion.
	// If the number of nodes is not divisible by the number of regions, the
	// extra nodes are allocated in a round-robin fashion over the regions at
	// the end of cluster.
	loadNodesAtTheEnd := numLoadNodes%numZones != 0
	loadGroups := make(loadGroupList, numLoadNodes)
	roachNodesPerGroup := numRoachNodes / numLoadNodes
	for i := range loadGroups {
		if loadNodesAtTheEnd {
			first := i*roachNodesPerGroup + 1
			loadGroups[i].roachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].loadNodes = c.Node(numRoachNodes + i + 1)
		} else {
			first := i*(roachNodesPerGroup+1) + 1
			loadGroups[i].roachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].loadNodes = c.Node((i + 1) * (roachNodesPerGroup + 1))
		}
	}
	return loadGroups
}
