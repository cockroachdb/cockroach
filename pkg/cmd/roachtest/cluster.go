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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/circbuf"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
)

var (
	// TODO(tbg): this is redundant with --cloud==local. Make the --local flag an
	// alias for `--cloud=local` and remove this variable.
	local bool

	cockroach        string
	libraryFilePaths []string
	cloud                         = spec.GCE
	encrypt          encryptValue = "false"
	instanceType     string
	localSSDArg      bool
	workload         string
	roachprod        string
	createArgs       []string
	buildTag         string
	clusterName      string
	clusterWipe      bool
	zonesF           string
	teamCity         bool
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

func filepathAbs(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return path, nil
}

func findBinary(binary, defValue string) (abspath string, err error) {
	if binary == "" {
		binary = defValue
	}

	// Check to see if binary exists and is a regular file and executable.
	if fi, err := os.Stat(binary); err == nil && fi.Mode().IsRegular() && (fi.Mode()&0111) != 0 {
		return filepathAbs(binary)
	}
	return findBinaryOrLibrary("bin", binary)
}

func findLibrary(libraryName string) (string, error) {
	suffix := ".so"
	if local {
		switch runtime.GOOS {
		case "linux":
		case "freebsd":
		case "openbsd":
		case "dragonfly":
		case "windows":
			suffix = ".dll"
		case "darwin":
			suffix = ".dylib"
		default:
			return "", errors.Newf("failed to find suffix for runtime %s", runtime.GOOS)
		}
	}
	return findBinaryOrLibrary("lib", libraryName+suffix)
}

func findBinaryOrLibrary(binOrLib string, name string) (string, error) {
	// Find the binary to run and translate it to an absolute path. First, look
	// for the binary in PATH.
	path, err := exec.LookPath(name)
	if err != nil {
		if strings.HasPrefix(name, "/") {
			return "", errors.WithStack(err)
		}

		// We're unable to find the name in PATH and "name" is a relative path:
		// look in the cockroach repo.
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			gopath = filepath.Join(os.Getenv("HOME"), "go")
		}

		var suffix string
		if !local {
			suffix = ".docker_amd64"
		}
		dirs := []string{
			filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/"),
			filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach", binOrLib+suffix),
			filepath.Join(os.ExpandEnv("$PWD"), binOrLib+suffix),
			filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach", binOrLib),
		}
		for _, dir := range dirs {
			path = filepath.Join(dir, name)
			var err2 error
			path, err2 = exec.LookPath(path)
			if err2 == nil {
				return filepathAbs(path)
			}
		}
		return "", fmt.Errorf("failed to find %q in $PATH or any of %s", name, dirs)
	}
	return filepathAbs(path)
}

func initBinariesAndLibraries() {
	// If we're running against an existing "local" cluster, force the local flag
	// to true in order to get the "local" test configurations.
	if clusterName == "local" {
		local = true
	}
	if local {
		cloud = spec.Local
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

	// In v20.2 or higher, optionally expect certain library files to exist.
	// Since they may not be found in older versions, do not hard error if they are not found.
	for _, libraryName := range []string{"libgeos", "libgeos_c"} {
		if libraryFilePath, err := findLibrary(libraryName); err != nil {
			fmt.Fprintf(os.Stderr, "error finding library %s, ignoring: %+v\n", libraryName, err)
		} else {
			libraryFilePaths = append(libraryFilePaths, libraryFilePath)
		}
	}
}

type clusterRegistry struct {
	mu struct {
		syncutil.Mutex
		clusters map[string]*clusterImpl
		tagCount map[string]int
		// savedClusters keeps track of clusters that have been saved for further
		// debugging. Each cluster comes with a message about the test failure
		// causing it to be saved for debugging.
		savedClusters map[*clusterImpl]string
	}
}

func newClusterRegistry() *clusterRegistry {
	cr := &clusterRegistry{}
	cr.mu.clusters = make(map[string]*clusterImpl)
	cr.mu.savedClusters = make(map[*clusterImpl]string)
	return cr
}

func (r *clusterRegistry) registerCluster(c *clusterImpl) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.clusters[c.name] != nil {
		return fmt.Errorf("cluster named %q already exists in registry", c.name)
	}
	r.mu.clusters[c.name] = c
	return nil
}

func (r *clusterRegistry) unregisterCluster(c *clusterImpl) bool {
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
func (r *clusterRegistry) markClusterAsSaved(c *clusterImpl, msg string) {
	r.mu.Lock()
	r.mu.savedClusters[c] = msg
	r.mu.Unlock()
}

type clusterWithMsg struct {
	*clusterImpl
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
			clusterImpl: c,
			savedMsg:    msg,
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
func (r *clusterRegistry) destroyAllClusters(ctx context.Context, l *logger.Logger) {
	// Fire off a goroutine to destroy all of the clusters.
	done := make(chan struct{})
	go func() {
		defer close(done)

		var clusters []*clusterImpl
		savedClusters := make(map[*clusterImpl]struct{})
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
			go func(c *clusterImpl) {
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
func execCmd(ctx context.Context, l *logger.Logger, args ...string) error {
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
func execCmdEx(ctx context.Context, l *logger.Logger, args ...string) cmdRes {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	l.Printf("> %s\n", strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)

	debugStdoutBuffer, _ := circbuf.NewBuffer(4096)
	debugStderrBuffer, _ := circbuf.NewBuffer(4096)

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
			_, _ = io.Copy(l.Stdout, io.TeeReader(rOut, debugStdoutBuffer))
		}()

		if l.Stderr == l.Stdout {
			// If l.Stderr == l.Stdout, we use only one pipe to avoid
			// duplicating everything.
			cmd.Stderr = wOut
		} else {
			cmd.Stderr = wErr
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = io.Copy(l.Stderr, io.TeeReader(rErr, debugStderrBuffer))
			}()
		}
	}

	err := cmd.Run()
	closePipes(ctx)
	wg.Wait()

	stdoutString := debugStdoutBuffer.String()
	if debugStdoutBuffer.TotalWritten() > debugStdoutBuffer.Size() {
		stdoutString = "<... some data truncated by circular buffer; go to artifacts for details ...>\n" + stdoutString
	}
	stderrString := debugStderrBuffer.String()
	if debugStderrBuffer.TotalWritten() > debugStderrBuffer.Size() {
		stderrString = "<... some data truncated by circular buffer; go to artifacts for details ...>\n" + stderrString
	}

	if err != nil {
		// Context errors opaquely appear as "signal killed" when manifested.
		// We surface this error explicitly.
		if ctx.Err() != nil {
			err = errors.CombineErrors(ctx.Err(), err)
		}

		if err != nil {
			err = &cluster.WithCommandDetails{
				Wrapped: err,
				Cmd:     strings.Join(args, " "),
				Stderr:  stderrString,
				Stdout:  stdoutString,
			}
		}
	}

	return cmdRes{
		err:    err,
		stdout: stdoutString,
		stderr: stderrString,
	}
}

// execCmdWithBuffer executes the given command and returns its stdout/stderr
// output. If the return code is not 0, an error is also returned.
// l is used to log the command before running it. No output is logged.
func execCmdWithBuffer(ctx context.Context, l *logger.Logger, args ...string) ([]byte, error) {
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

type nodeSelector interface {
	option.Option
	Merge(option.NodeListOption) option.NodeListOption
}

// clusterImpl implements cluster.Cluster.

// It is safe for concurrent use by multiple goroutines.
type clusterImpl struct {
	name string
	tag  string
	spec spec.ClusterSpec
	t    test.Test
	// r is the registry tracking this cluster. Destroying the cluster will
	// unregister it.
	r *clusterRegistry
	// l is the logger used to log various cluster operations.
	// DEPRECATED for use outside of cluster methods: Use a test's t.L() instead.
	// This is generally set to the current test's logger.
	l          *logger.Logger
	expiration time.Time
	// encryptDefault is true if the cluster should default to having encryption
	// at rest enabled. The default only applies if encryption is not explicitly
	// enabled or disabled by options passed to Start.
	encryptDefault bool
	// encryptAtRandom is true if the cluster should enable encryption-at-rest
	// on about half of all runs. Only valid if encryptDefault is false. Only
	// applies if encryption is not explicitly enabled or disabled by options
	// passed to Start. For use in roachtests.
	encryptAtRandom bool

	// destroyState contains state related to the cluster's destruction.
	destroyState destroyState
}

// Name returns the cluster name, i.e. something like `teamcity-....`
func (c *clusterImpl) Name() string {
	return c.name
}

// EncryptAtRandom sets whether the cluster will start new nodes with
// encryption enabled.
func (c *clusterImpl) EncryptAtRandom(b bool) {
	c.encryptAtRandom = b
}

// EncryptDefault sets the default for encryption-at-rest. This can be overridden
// by options passed to `c.Start`.
func (c *clusterImpl) EncryptDefault(b bool) {
	c.encryptDefault = b
}

// Spec returns the spec underlying the cluster.
func (c *clusterImpl) Spec() spec.ClusterSpec {
	return c.spec
}

// status is used to communicate the test's status. It's a no-op until the
// cluster is passed to a test, at which point it's hooked up to test.Status().
func (c *clusterImpl) status(args ...interface{}) {
	if c.t == nil {
		return
	}
	c.t.Status(args...)
}

func (c *clusterImpl) workerStatus(args ...interface{}) {
	if impl, ok := c.t.(*testImpl); ok {
		impl.WorkerStatus(args...)
	}
}

func (c *clusterImpl) String() string {
	return fmt.Sprintf("%s [tag:%s] (%d nodes)", c.name, c.tag, c.Spec().NodeCount)
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
func (c *clusterImpl) closeLogger() {
	c.destroyState.mu.Lock()
	defer c.destroyState.mu.Unlock()
	if c.destroyState.mu.loggerClosed {
		return
	}
	c.destroyState.mu.loggerClosed = true
	c.l.Close()
}

type clusterConfig struct {
	spec spec.ClusterSpec
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
	ctx context.Context, cfg clusterConfig, setStatus func(string), teeOpt logger.TeeOptType,
) (*clusterImpl, error) {
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
		c := &clusterImpl{
			name:       name,
			expiration: timeutil.Now().Add(24 * time.Hour),
			r:          f.r,
		}
		if err := f.r.registerCluster(c); err != nil {
			return nil, err
		}
		return c, nil
	}

	exp := cfg.spec.Expiration()
	if cfg.localCluster {
		// Local clusters never expire.
		exp = timeutil.Now().Add(100000 * time.Hour)
	}
	c := &clusterImpl{
		name:           name,
		spec:           cfg.spec,
		expiration:     exp,
		encryptDefault: encrypt.asBool(),
		r:              f.r,
		destroyState: destroyState{
			owned: true,
			alloc: cfg.alloc,
		},
	}

	sargs := []string{roachprod, "create", c.name, "-n", fmt.Sprint(c.spec.NodeCount)}
	{
		args, err := cfg.spec.Args(createArgs...)
		if err != nil {
			return nil, err
		}
		sargs = append(sargs, args...)
	}
	if !cfg.useIOBarrier && localSSDArg {
		sargs = append(sargs, "--local-ssd-no-ext4-barrier")
	}

	setStatus("acquring cluster creation semaphore")
	release := f.acquireSem()
	defer release()
	setStatus("roachprod create")
	c.status("creating cluster")

	// Logs for creating a new cluster go to a dedicated log file.
	logPath := filepath.Join(f.artifactsDir, runnerLogsDir, "cluster-create", name+".log")
	l, err := logger.RootLogger(logPath, teeOpt)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	success := false
	// Attempt to create a cluster several times, cause them clouds be flaky that
	// my phone says it's snowing.
	for i := 0; i < 3; i++ {
		if i > 0 {
			l.PrintfCtx(ctx, "Retrying cluster creation (attempt #%d)", i+1)
		}
		err = execCmd(ctx, l, sargs...)
		if err == nil {
			success = true
			break
		}
		l.PrintfCtx(ctx, "Failed to create cluster.")
		if !strings.Contains(cluster.GetStderr(err), "already exists") {
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
	ctx context.Context,
	name string,
	l *logger.Logger,
	spec spec.ClusterSpec,
	opt attachOpt,
	r *clusterRegistry,
) (*clusterImpl, error) {
	exp := spec.Expiration()
	if name == "local" {
		exp = timeutil.Now().Add(100000 * time.Hour)
	}
	c := &clusterImpl{
		name:           name,
		spec:           spec,
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
func (c *clusterImpl) setTest(t test.Test) {
	c.t = t
	c.l = t.L()
}

// StopCockroachGracefullyOnNode stops a running cockroach instance on the requested
// node before a version upgrade.
func (c *clusterImpl) StopCockroachGracefullyOnNode(ctx context.Context, node int) error {
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
func (c *clusterImpl) Save(ctx context.Context, msg string, l *logger.Logger) {
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
func (c *clusterImpl) validate(
	ctx context.Context, nodes spec.ClusterSpec, l *logger.Logger,
) error {
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

func (c *clusterImpl) lister() option.NodeLister {
	fatalf := func(string, ...interface{}) {}
	if c.t != nil { // accommodates poorly set up tests
		fatalf = c.t.Fatalf
	}
	return option.NodeLister{NodeCount: c.spec.NodeCount, Fatalf: fatalf}
}

func (c *clusterImpl) All() option.NodeListOption {
	return c.lister().All()
}

func (c *clusterImpl) Range(begin, end int) option.NodeListOption {
	return c.lister().Range(begin, end)
}

func (c *clusterImpl) Nodes(ns ...int) option.NodeListOption {
	return c.lister().Nodes(ns...)
}

func (c *clusterImpl) Node(i int) option.NodeListOption {
	return c.lister().Node(i)
}

// FetchLogs downloads the logs from the cluster using `roachprod get`.
// The logs will be placed in the test's artifacts dir.
func (c *clusterImpl) FetchLogs(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	t.L().Printf("fetching logs\n")
	c.status("fetching logs")

	// Don't hang forever if we can't fetch the logs.
	return contextutil.RunWithTimeout(ctx, "fetch logs", 2*time.Minute, func(ctx context.Context) error {
		path := filepath.Join(c.t.ArtifactsDir(), "logs", "unredacted")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}

		if err := execCmd(ctx, c.l, roachprod, "get", c.name, "logs" /* src */, path /* dest */); err != nil {
			t.L().Printf("failed to fetch logs: %v", err)
			if ctx.Err() != nil {
				return err
			}
		}

		if err := c.RunE(ctx, c.All(), "mkdir -p logs/redacted && ./cockroach debug merge-logs --redact logs/*.log > logs/redacted/combined.log"); err != nil {
			t.L().Printf("failed to redact logs: %v", err)
			if ctx.Err() != nil {
				return err
			}
		}

		return execCmd(
			ctx, c.l, roachprod, "get", c.name, "logs/redacted/combined.log" /* src */, filepath.Join(c.t.ArtifactsDir(), "logs/cockroach.log"),
		)
	})
}

// saveDiskUsageToLogsDir collects a summary of the disk usage to logs/diskusage.txt on each node.
func saveDiskUsageToLogsDir(ctx context.Context, c cluster.Cluster) error {
	// TODO(jackson): This is temporary for debugging out-of-disk-space
	// failures like #44845.
	if c.Spec().NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab disk usage on local runs.
		return nil
	}

	// Don't hang forever.
	return contextutil.RunWithTimeout(ctx, "disk usage", 20*time.Second, func(ctx context.Context) error {
		return c.RunE(ctx, c.All(),
			"du -c /mnt/data1 --exclude lost+found >> logs/diskusage.txt")
	})
}

// CopyRoachprodState copies the roachprod state directory in to the test
// artifacts.
func (c *clusterImpl) CopyRoachprodState(ctx context.Context) error {
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

// FetchTimeseriesData downloads the timeseries from the cluster using
// the first available node. They can be visualized via:
//
// `COCKROACH_DEBUG_TS_IMPORT_FILE=tsdump.gob ./cockroach start-single-node --insecure --store=$(mktemp -d)`
func (c *clusterImpl) FetchTimeseriesData(ctx context.Context, t test.Test) error {
	return contextutil.RunWithTimeout(ctx, "debug zip", 5*time.Minute, func(ctx context.Context) error {
		var err error
		for i := 1; i <= c.spec.NodeCount; i++ {
			err = c.RunE(ctx, c.Node(i), "./cockroach debug tsdump --insecure --format=raw > tsdump.gob")
			if err == nil {
				err = c.Get(ctx, c.l, "tsdump.gob", filepath.Join(c.t.ArtifactsDir(), "tsdump.gob"), c.Node(i))
			}
			if err == nil {
				return nil
			}
			t.L().Printf("while fetching timeseries: %s", err)
		}
		return err
	})
}

// FetchDebugZip downloads the debug zip from the cluster using `roachprod ssh`.
// The logs will be placed in the test's artifacts dir.
func (c *clusterImpl) FetchDebugZip(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	t.L().Printf("fetching debug zip\n")
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
			//
			// Ignore the files in the the log directory; we pull the logs separately anyway
			// so this would only cause duplication.
			si := strconv.Itoa(i)
			output, err := execCmdWithBuffer(ctx, c.l, roachprod, "ssh", c.name+":"+si, "--",
				"./cockroach", "debug", "zip", "--exclude-files='*.log,*.txt,*.pprof'", "--url", "{pgurl:"+si+"}", zipName)
			if err != nil {
				t.L().Printf("./cockroach debug zip failed: %s", output)
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
// found to be not running. It prints both to t.L() and the test output.
func (c *clusterImpl) FailOnDeadNodes(ctx context.Context, t test.Test) {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return
	}

	// Don't hang forever.
	_ = contextutil.RunWithTimeout(ctx, "detect dead nodes", time.Minute, func(ctx context.Context) error {
		output, err := execCmdWithBuffer(
			ctx, t.L(), roachprod, "monitor", c.name, "--oneshot", "--ignore-empty-nodes",
		)
		// If there's an error, it means either that the monitor command failed
		// completely, or that it found a dead node worth complaining about.
		if err != nil {
			if ctx.Err() != nil {
				// Don't fail if we timed out.
				return nil
			}
			// TODO(tbg): remove this type assertion.
			t.(*testImpl).printfAndFail(0 /* skip */, "dead node detection: %s %s", err, output)
		}
		return nil
	})
}

// CheckReplicaDivergenceOnDB runs a fast consistency check of the whole keyspace
// against the provided db. If an inconsistency is found, it returns it in the
// error. Note that this will swallow errors returned directly from the consistency
// check since we know that such spurious errors are possibly without any relation
// to the check having failed.
func (c *clusterImpl) CheckReplicaDivergenceOnDB(
	ctx context.Context, l *logger.Logger, db *gosql.DB,
) error {
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
		l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	var finalErr error
	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if scanErr := rows.Scan(&rangeID, &prettyKey, &status, &detail); scanErr != nil {
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
func (c *clusterImpl) FailOnReplicaDivergence(ctx context.Context, t test.Test) {
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
		t.L().Printf("running (fast) consistency checks on node %d", i)
		break
	}
	if db == nil {
		t.L().Printf("no live node found, skipping consistency check")
		return
	}
	defer db.Close()

	if err := contextutil.RunWithTimeout(
		ctx, "consistency check", time.Minute,
		func(ctx context.Context) error {
			return c.CheckReplicaDivergenceOnDB(ctx, t.L(), db)
		},
	); err != nil {
		// NB: we don't call t.Fatal() here because this method is
		// for use by the test harness beyond the point at which
		// it can interpret `t.Fatal`.
		//
		// TODO(tbg): remove this type assertion.
		t.(*testImpl).printAndFail(0, err)
	}
}

// FetchDmesg grabs the dmesg logs if possible. This requires being able to run
// `sudo dmesg` on the remote nodes.
func (c *clusterImpl) FetchDmesg(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	t.L().Printf("fetching dmesg\n")
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
			t.L().Printf("during dmesg fetching: %s", err)
		}
		return execCmd(ctx, c.l, roachprod, "get", c.name, name /* src */, path /* dest */)
	})
}

// FetchJournalctl grabs the journalctl logs if possible. This requires being
// able to run `sudo journalctl` on the remote nodes.
func (c *clusterImpl) FetchJournalctl(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab journalctl on local runs.
		return nil
	}

	t.L().Printf("fetching journalctl\n")
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
			t.L().Printf("during journalctl fetching: %s", err)
		}
		return execCmd(ctx, c.l, roachprod, "get", c.name, name /* src */, path /* dest */)
	})
}

// FetchCores fetches any core files on the cluster.
func (c *clusterImpl) FetchCores(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	if true {
		// TeamCity does not handle giant artifacts well. We'd generally profit
		// from having the cores, but we should push them straight into a temp
		// bucket on S3 instead. OTOH, the ROI of this may be low; I don't know
		// of a recent example where we've wanted the Core dumps.
		t.L().Printf("skipped fetching cores\n")
		return nil
	}

	t.L().Printf("fetching cores\n")
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
func (c *clusterImpl) Destroy(ctx context.Context, lo closeLoggerOpt, l *logger.Logger) {
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

func (c *clusterImpl) doDestroy(ctx context.Context, l *logger.Logger) <-chan struct{} {
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

// Put a local file to all of the machines in a cluster.
// Put is DEPRECATED. Use PutE instead.
func (c *clusterImpl) Put(ctx context.Context, src, dest string, opts ...option.Option) {
	if err := c.PutE(ctx, c.l, src, dest, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// PutE puts a local file to all of the machines in a cluster.
func (c *clusterImpl) PutE(
	ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}

	c.status("uploading file")
	defer c.status("")

	err := execCmd(ctx, c.l, roachprod, "put", c.MakeNodes(opts...), src, dest)
	if err != nil {
		return errors.Wrap(err, "cluster.Put")
	}
	return nil
}

// PutLibraries inserts all available library files into all nodes on the cluster
// at the specified location.
func (c *clusterImpl) PutLibraries(ctx context.Context, libraryDir string) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}

	c.status("uploading library files")
	defer c.status("")

	if err := c.RunE(ctx, c.All(), "mkdir", "-p", libraryDir); err != nil {
		return err
	}
	for _, libraryFilePath := range libraryFilePaths {
		putPath := filepath.Join(libraryDir, filepath.Base(libraryFilePath))
		if err := c.PutE(
			ctx,
			c.l,
			libraryFilePath,
			putPath,
		); err != nil {
			return err
		}
	}
	return nil
}

// Stage stages a binary to the cluster.
func (c *clusterImpl) Stage(
	ctx context.Context,
	l *logger.Logger,
	application, versionOrSHA, dir string,
	opts ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Stage")
	}

	c.status("staging binary")
	defer c.status("")

	args := []string{roachprod, "stage", c.MakeNodes(opts...), application, versionOrSHA}
	if dir != "" {
		args = append(args, fmt.Sprintf("--dir='%s'", dir))
	}
	err := execCmd(ctx, c.l, args...)
	if err != nil {
		return errors.Wrap(err, "cluster.Stage")
	}
	return nil
}

// Get gets files from remote hosts.
func (c *clusterImpl) Get(
	ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Get error")
	}
	c.status(fmt.Sprintf("getting %v", src))
	defer c.status("")
	return errors.Wrap(
		execCmd(ctx, l, roachprod, "get", c.MakeNodes(opts...), src, dest),
		"cluster.Get error")

}

// Put a string into the specified file on the remote(s).
func (c *clusterImpl) PutString(
	ctx context.Context, content, dest string, mode os.FileMode, opts ...option.Option,
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

	if err := execCmd(ctx, c.l, roachprod, "put", c.MakeNodes(opts...), src, dest); err != nil {
		return errors.Wrap(err, "PutString")
	}
	return nil
}

// GitClone clones a git repo from src into dest and checks out origin's
// version of the given branch. The src, dest, and branch arguments must not
// contain shell special characters.
func (c *clusterImpl) GitClone(
	ctx context.Context, l *logger.Logger, src, dest, branch string, node option.NodeListOption,
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

func roachprodArgs(opts []option.Option) []string {
	var args []string
	for _, opt := range opts {
		a, ok := opt.(option.RoachprodArgOption)
		if !ok {
			continue
		}
		args = append(args, ([]string)(a)...)
	}
	return args
}

func (c *clusterImpl) setStatusForClusterOpt(operation string, opts ...option.Option) {
	var nodes option.NodeListOption
	worker := false
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			nodes = s.Merge(nodes)
		}
		if _, ok := o.(option.WorkerAction); ok {
			worker = true
		}
	}
	nodesString := " cluster"
	if len(nodes) != 0 {
		nodesString = " nodes " + nodes.String()
	}
	msg := operation + nodesString
	if worker {
		c.workerStatus(msg)
	} else {
		c.status(msg)
	}
}

func (c *clusterImpl) clearStatusForClusterOpt(opts ...option.Option) {
	worker := false
	for _, o := range opts {
		if _, ok := o.(option.WorkerAction); ok {
			worker = true
		}
	}
	if worker {
		c.workerStatus()
	} else {
		c.status()
	}
}

// StartE starts cockroach nodes on a subset of the cluster. The nodes parameter
// can either be a specific node, empty (to indicate all nodes), or a pair of
// nodes indicating a range.
func (c *clusterImpl) StartE(ctx context.Context, opts ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.StartE")
	}
	// If the test failed (indicated by a canceled ctx), short-circuit.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.setStatusForClusterOpt("starting", opts...)
	defer c.clearStatusForClusterOpt(opts...)
	args := []string{
		roachprod,
		"start",
	}
	args = append(args, roachprodArgs(opts)...)
	args = append(args, c.MakeNodes(opts...))
	if !argExists(args, "--encrypt") {
		if c.encryptDefault {
			args = append(args, "--encrypt")
		} else if c.encryptAtRandom {
			rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
			if rng.Intn(2) == 1 {
				args = append(args, "--encrypt")
				// Force encryption in future calls of Start with the same cluster.
				c.encryptDefault = true
			}
		}
	}
	return execCmd(ctx, c.l, args...)
}

// Start is like StartE() except that it will fatal the test on error.
func (c *clusterImpl) Start(ctx context.Context, opts ...option.Option) {
	if err := c.StartE(ctx, opts...); err != nil {
		c.t.Fatal(err)
	}
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
func (c *clusterImpl) StopE(ctx context.Context, opts ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.StopE")
	}
	args := []string{
		roachprod,
		"stop",
	}
	args = append(args, roachprodArgs(opts)...)
	args = append(args, c.MakeNodes(opts...))
	c.setStatusForClusterOpt("stopping", opts...)
	defer c.clearStatusForClusterOpt(opts...)
	return execCmd(ctx, c.l, args...)
}

// Stop is like StopE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Stop(ctx context.Context, opts ...option.Option) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.StopE(ctx, opts...); err != nil {
		c.t.Fatal(err)
	}
}

func (c *clusterImpl) Reset(ctx context.Context) error {
	if c.t.Failed() {
		return errors.New("already failed")
	}
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Reset")
	}
	args := []string{
		roachprod,
		"reset",
		c.name,
	}
	c.status("resetting cluster")
	defer c.status()
	return execCmd(ctx, c.l, args...)
}

// WipeE wipes a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *clusterImpl) WipeE(ctx context.Context, l *logger.Logger, opts ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.WipeE")
	}
	if c.spec.NodeCount == 0 {
		// For tests.
		return nil
	}
	c.setStatusForClusterOpt("wiping", opts...)
	defer c.clearStatusForClusterOpt(opts...)
	return execCmd(ctx, l, roachprod, "wipe", c.MakeNodes(opts...))
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Wipe(ctx context.Context, opts ...option.Option) {
	if ctx.Err() != nil {
		return
	}
	if err := c.WipeE(ctx, c.l, opts...); err != nil {
		c.t.Fatal(err)
	}
}

// Run a command on the specified node.
func (c *clusterImpl) Run(ctx context.Context, node option.NodeListOption, args ...string) {
	err := c.RunE(ctx, node, args...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// Reformat the disk on the specified node.
func (c *clusterImpl) Reformat(ctx context.Context, node option.NodeListOption, args ...string) {
	err := execCmd(ctx, c.l,
		append([]string{roachprod, "reformat", c.MakeNodes(node), "--"}, args...)...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// Silence unused warning.
var _ = (&clusterImpl{}).Reformat

// Install a package in a node
func (c *clusterImpl) Install(
	ctx context.Context, node option.NodeListOption, args ...string,
) error {
	l, _, err := c.loggerForCmd(node, append([]string{"install"}, args...)...)
	if err != nil {
		return err
	}
	return c.execRoachprodL(ctx, l, "install", node, args...)
}

var reOnlyAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

// cmdLogFileName comes up with a log file to use for the given argument string.
func cmdLogFileName(t time.Time, nodes option.NodeListOption, args ...string) string {
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

func (c *clusterImpl) loggerForCmd(
	node option.NodeListOption, args ...string,
) (*logger.Logger, string, error) {
	logFile := cmdLogFileName(timeutil.Now(), node, args...)

	// NB: we set no prefix because it's only going to a file anyway.
	l, err := c.l.ChildLogger(logFile, logger.QuietStderr, logger.QuietStdout)
	if err != nil {
		return nil, "", err
	}
	return l, logFile, nil
}

// RunE runs a command on the specified node, returning an error. The output
// will be redirected to a file which is logged via the cluster-wide logger in
// case of an error. Logs will sort chronologically. Failing invocations will
// have an additional marker file with a `.failed` extension instead of `.log`.
func (c *clusterImpl) RunE(ctx context.Context, node option.NodeListOption, args ...string) error {
	l, logFile, err := c.loggerForCmd(node, args...)
	if err != nil {
		return err
	}

	err = c.RunL(ctx, l, node, args...)
	l.Printf("> result: %+v", err)
	if err := ctx.Err(); err != nil {
		l.Printf("(note: incoming context was canceled: %s", err)
	}
	physicalFileName := l.File.Name()
	l.Close()
	if err != nil {
		failedPhysicalFileName := strings.TrimSuffix(physicalFileName, ".log") + ".failed"
		if failedFile, err2 := os.Create(failedPhysicalFileName); err2 != nil {
			failedFile.Close()
		}
	}
	err = errors.Wrapf(err, "output in %s", logFile)
	return err
}

// RunL runs a command on the specified node, returning an error.
func (c *clusterImpl) RunL(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, args ...string,
) error {
	if err := errors.Wrap(ctx.Err(), "cluster.RunL"); err != nil {
		return err
	}
	return c.execRoachprodL(ctx, l, "run", node, args...)
}

func (c *clusterImpl) execRoachprodL(
	ctx context.Context, l *logger.Logger, verb string, node option.NodeListOption, args ...string,
) error {
	return execCmd(ctx, l,
		append([]string{roachprod, verb, c.MakeNodes(node), "--"}, args...)...)
}

// RunWithBuffer runs a command on the specified node, returning the resulting combined stderr
// and stdout or an error.
func (c *clusterImpl) RunWithBuffer(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, args ...string,
) ([]byte, error) {
	if err := errors.Wrap(ctx.Err(), "cluster.RunWithBuffer"); err != nil {
		return nil, err
	}
	return execCmdWithBuffer(ctx, l,
		append([]string{roachprod, "run", c.MakeNodes(node), "--"}, args...)...)
}

// pgURLErr returns the Postgres endpoint for the specified node. It accepts a
// flag specifying whether the URL should include the node's internal or
// external IP address. In general, inter-cluster communication and should use
// internal IPs and communication from a test driver to nodes in a cluster
// should use external IPs.
func (c *clusterImpl) pgURLErr(
	ctx context.Context, node option.NodeListOption, external bool,
) ([]string, error) {
	args := []string{roachprod, "pgurl"}
	if external {
		args = append(args, `--external`)
	}
	nodes := c.MakeNodes(node)
	args = append(args, nodes)
	cmd := execCmdEx(ctx, c.l, args...)
	if cmd.err != nil {
		return nil, errors.Wrapf(cmd.err, "failed to get pgurl for nodes: %s", nodes)
	}
	urls := strings.Split(strings.TrimSpace(cmd.stdout), " ")
	if len(urls) != len(node) {
		return nil, errors.Errorf(
			"pgurl for nodes %v got urls %v from stdout:\n%s\nstderr:\n%s",
			node, urls, cmd.stdout, cmd.stderr)
	}
	for i := range urls {
		urls[i] = strings.Trim(urls[i], "'")
		if urls[i] == "" {
			return nil, errors.Errorf(
				"pgurl for nodes %s empty: %v from\nstdout:\n%s\nstderr:\n%s",
				urls, node, cmd.stdout, cmd.stderr,
			)
		}
	}
	return urls, nil
}

// InternalPGUrl returns the internal Postgres endpoint for the specified nodes.
func (c *clusterImpl) InternalPGUrl(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	return c.pgURLErr(ctx, node, false /* external */)
}

// Silence unused warning.
var _ = (&clusterImpl{}).InternalPGUrl

// ExternalPGUrl returns the external Postgres endpoint for the specified nodes.
func (c *clusterImpl) ExternalPGUrl(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	return c.pgURLErr(ctx, node, true /* external */)
}

// ExternalPGUrlSecure returns the external Postgres endpoint for the specified
// nodes.
func (c *clusterImpl) ExternalPGUrlSecure(
	ctx context.Context, node option.NodeListOption, user string, certsDir string, port int,
) ([]string, error) {
	urlTemplate := "postgres://%s@%s:%d?sslcert=%s/client.%s.crt&sslkey=%s/client.%s.key&sslrootcert=%s/ca.crt&sslmode=require"
	ips, err := c.ExternalIP(ctx, node)
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, ip := range ips {
		urls = append(urls, fmt.Sprintf(urlTemplate, user, ip, port, certsDir, user, certsDir, user, certsDir))
	}
	return urls, nil
}

func addrToAdminUIAddr(c *clusterImpl, addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	webPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}
	// Roachprod makes Admin UI's port to be node's port + 1.
	return fmt.Sprintf("%s:%d", host, webPort+1), nil
}

func urlToAddr(pgURL string) (string, error) {
	u, err := url.Parse(pgURL)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

func addrToHost(addr string) (string, error) {
	host, _, err := addrToHostPort(addr)
	if err != nil {
		return "", err
	}
	return host, nil
}

func addrToHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}
	return host, port, nil
}

// InternalAdminUIAddr returns the internal Admin UI address in the form host:port
// for the specified node.
func (c *clusterImpl) InternalAdminUIAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.InternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for _, u := range urls {
		adminUIAddr, err := addrToAdminUIAddr(c, u)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, adminUIAddr)
	}
	return addrs, nil
}

// ExternalAdminUIAddr returns the internal Admin UI address in the form host:port
// for the specified node.
func (c *clusterImpl) ExternalAdminUIAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	externalAddrs, err := c.ExternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for _, u := range externalAddrs {
		adminUIAddr, err := addrToAdminUIAddr(c, u)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, adminUIAddr)
	}
	return addrs, nil
}

// InternalAddr returns the internal address in the form host:port for the
// specified nodes.
func (c *clusterImpl) InternalAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, node, false /* external */)
	if err != nil {
		return nil, err
	}
	for _, u := range urls {
		addr, err := urlToAddr(u)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// InternalIP returns the internal IP addresses for the specified nodes.
func (c *clusterImpl) InternalIP(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var ips []string
	addrs, err := c.InternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		host, err := addrToHost(addr)
		if err != nil {
			return nil, err
		}
		ips = append(ips, host)
	}
	return ips, nil
}

// ExternalAddr returns the external address in the form host:port for the
// specified node.
func (c *clusterImpl) ExternalAddr(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, node, true /* external */)
	if err != nil {
		return nil, err
	}
	for _, u := range urls {
		addr, err := urlToAddr(u)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// ExternalIP returns the external IP addresses for the specified node.
func (c *clusterImpl) ExternalIP(
	ctx context.Context, node option.NodeListOption,
) ([]string, error) {
	var ips []string
	addrs, err := c.ExternalAddr(ctx, node)
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		host, err := addrToHost(addr)
		if err != nil {
			return nil, err
		}
		ips = append(ips, host)
	}
	return ips, nil
}

// Silence unused warning.
var _ = (&clusterImpl{}).ExternalIP

// Conn returns a SQL connection to the specified node.
func (c *clusterImpl) Conn(ctx context.Context, node int) *gosql.DB {
	urls, err := c.ExternalPGUrl(ctx, c.Node(node))
	if err != nil {
		c.t.Fatal(err)
	}
	db, err := gosql.Open("postgres", urls[0])
	if err != nil {
		c.t.Fatal(err)
	}
	return db
}

// ConnE returns a SQL connection to the specified node.
func (c *clusterImpl) ConnE(ctx context.Context, node int) (*gosql.DB, error) {
	urls, err := c.ExternalPGUrl(ctx, c.Node(node))
	if err != nil {
		return nil, err
	}
	db, err := gosql.Open("postgres", urls[0])
	if err != nil {
		return nil, err
	}
	return db, nil
}

// ConnSecure returns a secure SQL connection to the specified node.
func (c *clusterImpl) ConnSecure(
	ctx context.Context, node int, user string, certsDir string, port int,
) (*gosql.DB, error) {
	urls, err := c.ExternalPGUrlSecure(ctx, c.Node(node), user, certsDir, port)
	if err != nil {
		return nil, err
	}
	db, err := gosql.Open("postgres", urls[0])
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (c *clusterImpl) MakeNodes(opts ...option.Option) string {
	var r option.NodeListOption
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			r = s.Merge(r)
		}
	}
	return c.name + r.String()
}

func (c *clusterImpl) IsLocal() bool {
	return c.name == "local"
}

// Extend extends the cluster's expiration by d, after truncating d to minute
// granularity.
func (c *clusterImpl) Extend(ctx context.Context, d time.Duration, l *logger.Logger) error {
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

func (c *clusterImpl) NewMonitor(ctx context.Context, opts ...option.Option) cluster.Monitor {
	return newMonitor(ctx, c.t, c, opts...)
}
