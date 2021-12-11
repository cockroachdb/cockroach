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
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/fs"
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"github.com/spf13/pflag"
)

func init() {
	_ = roachprod.InitProviders()
}

var (
	// TODO(tbg): this is redundant with --cloud==local. Make the --local flag an
	// alias for `--cloud=local` and remove this variable.
	local bool

	cockroach                 string
	libraryFilePaths          []string
	cloud                                  = spec.GCE
	encrypt                   encryptValue = "false"
	instanceType              string
	localSSDArg               bool
	workload                  string
	deprecatedRoachprodBinary string
	// overrideOpts contains vm.CreateOpts override values passed from the cli.
	overrideOpts vm.CreateOpts
	// overrideFlagset represents the flags passed from the cli for
	// `run` command (used to know if the value of a flag changed,
	// for example: overrideFlagset("lifetime").Changed().
	// TODO(ahmad/healthy-pod): extract runCmd (and other commands) from main
	// to make it global and operate on runCmd.Flags() directly.
	overrideFlagset  *pflag.FlagSet
	overrideNumNodes = -1
	buildTag         string
	clusterName      string
	clusterWipe      bool
	zonesF           string
	teamCity         bool
	disableIssue     bool
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

type errBinaryOrLibraryNotFound struct {
	binary string
}

func (e errBinaryOrLibraryNotFound) Error() string {
	return fmt.Sprintf("binary or library %q not found (or was not executable)", e.binary)
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
		return "", errBinaryOrLibraryNotFound{name}
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
	if errors.As(err, &errBinaryOrLibraryNotFound{}) {
		fmt.Fprintln(os.Stderr, "workload binary not provided, proceeding anyway")
	} else if err != nil {
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

// execCmd is like execCmdEx, but doesn't return the command's output.
func execCmd(ctx context.Context, l *logger.Logger, clusterName string, args ...string) error {
	return execCmdEx(ctx, l, clusterName, args...).err
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
func execCmdEx(ctx context.Context, l *logger.Logger, clusterName string, args ...string) cmdRes {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	l.Printf("> %s\n", strings.Join(args, " "))
	var roachprodRunStdout, roachprodRunStderr io.Writer

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
		roachprodRunStdout = wOut
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(l.Stdout, io.TeeReader(rOut, debugStdoutBuffer))
		}()

		if l.Stderr == l.Stdout {
			// If l.Stderr == l.Stdout, we use only one pipe to avoid
			// duplicating everything.
			roachprodRunStderr = wOut
		} else {
			roachprodRunStderr = wErr
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = io.Copy(l.Stderr, io.TeeReader(rErr, debugStderrBuffer))
			}()
		}
	}

	err := roachprod.Run(ctx, l, clusterName, "" /* SSHOptions */, "" /* processTag */, false /* secure */, roachprodRunStdout, roachprodRunStderr, args)
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
	l *logger.Logger
	// localCertsDir is a local copy of the certs for this cluster. If this is empty,
	// the cluster is running in insecure mode.
	localCertsDir string
	expiration    time.Time
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
	// username is the username passed via the --username argument
	// or the ROACHPROD_USER command-line option.
	username     string
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

func (f *clusterFactory) genName(cfg clusterConfig) string {
	if cfg.localCluster {
		return "local" // The roachprod tool understands this magic name.
	}
	count := atomic.AddUint64(&f.counter, 1)
	return makeClusterName(
		fmt.Sprintf("%s-%02d-%s", f.namePrefix, count, cfg.spec.String()))
}

// createFlagsOverride updates opts with the override values passed from the cli.
func createFlagsOverride(flags *pflag.FlagSet, opts *vm.CreateOpts) {
	if flags != nil {
		if flags.Changed("lifetime") {
			opts.Lifetime = overrideOpts.Lifetime
		}
		if flags.Changed("roachprod-local-ssd") {
			opts.SSDOpts.UseLocalSSD = overrideOpts.SSDOpts.UseLocalSSD
		}
		if flags.Changed("filesystem") {
			opts.SSDOpts.FileSystem = overrideOpts.SSDOpts.FileSystem
		}
		if flags.Changed("local-ssd-no-ext4-barrier") {
			opts.SSDOpts.NoExt4Barrier = overrideOpts.SSDOpts.NoExt4Barrier
		}
		if flags.Changed("os-volume-size") {
			opts.OsVolumeSize = overrideOpts.OsVolumeSize
		}
		if flags.Changed("geo") {
			opts.GeoDistributed = overrideOpts.GeoDistributed
		}
	}
}

// clusterMock creates a cluster to be used for (self) testing.
func (f *clusterFactory) clusterMock(cfg clusterConfig) *clusterImpl {
	return &clusterImpl{
		name:       f.genName(cfg),
		expiration: timeutil.Now().Add(24 * time.Hour),
		r:          f.r,
	}
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

	if overrideFlagset != nil && overrideFlagset.Changed("nodes") {
		cfg.spec.NodeCount = overrideNumNodes
	}

	if cfg.spec.NodeCount == 0 {
		// For tests, use a mock cluster.
		c := f.clusterMock(cfg)
		if err := f.r.registerCluster(c); err != nil {
			return nil, err
		}
		return c, nil
	}

	if cfg.localCluster {
		// Local clusters never expire.
		cfg.spec.Lifetime = 100000 * time.Hour
	}

	setStatus("acquiring cluster creation semaphore")
	release := f.acquireSem()
	defer release()

	setStatus("roachprod create")
	defer setStatus("idle")

	providerOptsContainer := vm.CreateProviderOptionsContainer()
	// The ClusterName is set below in the retry loop to ensure
	// that each create attempt gets a unique cluster name.
	createVMOpts, providerOpts, err := cfg.spec.RoachprodOpts("", cfg.useIOBarrier)
	if err != nil {
		return nil, err
	}
	if cfg.spec.Cloud != spec.Local {
		providerOptsContainer.SetProviderOpts(cfg.spec.Cloud, providerOpts)
	}

	createFlagsOverride(overrideFlagset, &createVMOpts)
	// Make sure expiration is changed if --lifetime override flag
	// is passed.
	cfg.spec.Lifetime = createVMOpts.Lifetime

	// Attempt to create a cluster several times to be able to move past
	// temporary flakiness in the cloud providers.
	const maxAttempts = 3
	// loop assumes maxAttempts is atleast (1).
	for i := 1; ; i++ {
		c := &clusterImpl{
			// NB: this intentionally avoids re-using the name across iterations in
			// the loop. See:
			//
			// https://github.com/cockroachdb/cockroach/issues/67906#issuecomment-887477675
			name:           f.genName(cfg),
			spec:           cfg.spec,
			expiration:     cfg.spec.Expiration(),
			encryptDefault: encrypt.asBool(),
			r:              f.r,
			destroyState: destroyState{
				owned: true,
				alloc: cfg.alloc,
			},
		}
		c.status("creating cluster")

		// Logs for creating a new cluster go to a dedicated log file.
		var retryStr string
		if i > 1 {
			retryStr = "-retry" + strconv.Itoa(i-1)
		}
		logPath := filepath.Join(f.artifactsDir, runnerLogsDir, "cluster-create", c.name+retryStr+".log")
		l, err := logger.RootLogger(logPath, teeOpt)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}

		l.PrintfCtx(ctx, "Attempting cluster creation (attempt #%d/%d)", i, maxAttempts)
		createVMOpts.ClusterName = c.name
		err = roachprod.Create(ctx, l, cfg.username, cfg.spec.NodeCount, createVMOpts, providerOptsContainer)
		if err == nil {
			if err := f.r.registerCluster(c); err != nil {
				return nil, err
			}
			c.status("idle")
			l.Close()
			return c, nil
		}

		if errors.HasType(err, (*roachprod.ClusterAlreadyExistsError)(nil)) {
			// If the cluster couldn't be created because it existed already, bail.
			// In reality when this is hit is when running with the `local` flag
			// or a destroy from the previous iteration failed.
			return nil, err
		}

		l.PrintfCtx(ctx, "cluster creation failed, cleaning up in case it was partially created: %s", err)
		// Set the alloc to nil so that Destroy won't release it.
		// This is ugly, but given that the alloc is created very far away from this code
		// (when selecting the test) it's the best we can do for now.
		c.destroyState.alloc = nil
		c.Destroy(ctx, closeLogger, l)
		if i >= maxAttempts {
			// Here we have to release the alloc, as we are giving up.
			cfg.alloc.Release()
			return nil, err
		}
		// Try again to create the cluster.
	}
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
			// If we're attaching to an existing cluster, we're not going to destroy it.
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
		if err := c.StopE(ctx, l, option.DefaultStopOpts(), c.All()); err != nil {
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
func (c *clusterImpl) StopCockroachGracefullyOnNode(
	ctx context.Context, l *logger.Logger, node int,
) error {
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
	c.Stop(ctx, l, option.DefaultStopOpts(), c.Node(node))
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
	pattern := "^" + regexp.QuoteMeta(c.name) + "$"
	cloudClusters, err := roachprod.List(l, false /* listMine */, pattern)
	if err != nil {
		return err
	}
	cDetails, ok := cloudClusters.Clusters[c.name]
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

		if err := c.Get(ctx, c.l, "logs" /* src */, path /* dest */); err != nil {
			t.L().Printf("failed to fetch logs: %v", err)
			if ctx.Err() != nil {
				return errors.Wrap(err, "cluster.FetchLogs")
			}
		}

		if err := c.RunE(ctx, c.All(), "mkdir -p logs/redacted && ./cockroach debug merge-logs --redact logs/*.log > logs/redacted/combined.log"); err != nil {
			t.L().Printf("failed to redact logs: %v", err)
			if ctx.Err() != nil {
				return err
			}
		}
		dest := filepath.Join(c.t.ArtifactsDir(), "logs/cockroach.log")
		return errors.Wrap(c.Get(ctx, c.l, "logs/redacted/combined.log" /* src */, dest), "cluster.FetchLogs")
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
	return contextutil.RunWithTimeout(ctx, "fetch tsdata", 5*time.Minute, func(ctx context.Context) error {
		node := 1
		for ; node <= c.spec.NodeCount; node++ {
			db, err := c.ConnE(ctx, t.L(), node)
			if err == nil {
				err = db.Ping()
				db.Close()
			}
			if err != nil {
				t.L().Printf("node %d not responding to SQL, trying next one", node)
				continue
			}
			break
		}
		if node > c.spec.NodeCount {
			return errors.New("no node responds to SQL, cannot fetch tsdata")
		}
		if err := c.RunE(
			ctx, c.Node(node), "./cockroach debug tsdump --insecure --format=raw > tsdump.gob",
		); err != nil {
			return err
		}
		tsDumpGob := filepath.Join(c.t.ArtifactsDir(), "tsdump.gob")
		if err := c.Get(
			ctx, c.l, "tsdump.gob", tsDumpGob, c.Node(node),
		); err != nil {
			return errors.Wrap(err, "cluster.FetchTimeseriesData")
		}
		db, err := c.ConnE(ctx, t.L(), node)
		if err != nil {
			return err
		}
		defer db.Close()
		rows, err := db.QueryContext(
			ctx,
			` SELECT store_id, node_id FROM crdb_internal.kv_store_status`,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		var buf bytes.Buffer
		for rows.Next() {
			var storeID, nodeID int
			if err := rows.Scan(&storeID, &nodeID); err != nil {
				return err
			}
			fmt.Fprintf(&buf, "%d: %d\n", storeID, nodeID)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := ioutil.WriteFile(tsDumpGob+".yaml", buf.Bytes(), 0644); err != nil {
			return err
		}
		return ioutil.WriteFile(tsDumpGob+"-run.sh", []byte(`#!/usr/bin/env bash

COCKROACH_DEBUG_TS_IMPORT_FILE=tsdump.gob cockroach start-single-node --insecure
`), 0755)
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
			cmd := []string{"./cockroach", "debug", "zip", "--exclude-files='*.log,*.txt,*.pprof'", "--url", "{pgurl:" + si + "}", zipName}
			if err := c.RunE(ctx, c.All(), cmd...); err != nil {
				t.L().Printf("./cockroach debug zip failed: %v", err)
				if i < c.spec.NodeCount {
					continue
				}
				return err
			}
			return errors.Wrap(c.Get(ctx, c.l, zipName /* src */, path /* dest */, c.Node(i)), "cluster.FetchDebugZip")
		}
		return nil
	})
}

// checkNoDeadNode reports an error (via `t.Error`) if nodes that have a populated
// data dir are found to be not running. It prints both to t.L() and the test
// output.
func (c *clusterImpl) assertNoDeadNode(ctx context.Context, t test.Test) {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return
	}

	_, err := roachprod.Monitor(ctx, t.L(), c.name, install.MonitorOpts{OneShot: true, IgnoreEmptyNodes: true})
	// If there's an error, it means either that the monitor command failed
	// completely, or that it found a dead node worth complaining about.
	if err != nil {
		t.Errorf("dead node detection: %s", err)
	}
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
SET statement_timeout = '5m';
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
	defer rows.Close()
	var finalErr error
	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if scanErr := rows.Scan(&rangeID, &prettyKey, &status, &detail); scanErr != nil {
			l.Printf("consistency check failed with %v; ignoring", scanErr)
			return nil
		}
		finalErr = errors.CombineErrors(finalErr,
			errors.Newf("r%d (%s) is inconsistent: %s %s\n", rangeID, prettyKey, status, detail))
	}
	if err := rows.Err(); err != nil {
		l.Printf("consistency check failed with %v; ignoring", err)
		return nil
	}
	return finalErr
}

// FailOnReplicaDivergence fails the test if
// crdb_internal.check_consistency(true, '', '') indicates that any ranges'
// replicas are inconsistent with each other. It uses the first node that
// is up to run the query.
func (c *clusterImpl) FailOnReplicaDivergence(ctx context.Context, t *testImpl) {
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
				db = c.Conn(ctx, t.L(), i)
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
		ctx, "consistency check", 5*time.Minute,
		func(ctx context.Context) error {
			return c.CheckReplicaDivergenceOnDB(ctx, t.L(), db)
		},
	); err != nil {
		t.Errorf("consistency check failed: %v", err)
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

		// Run dmesg on all nodes to redirect the kernel ring buffer content to a file.
		cmd := []string{"/bin/bash", "-c", "'sudo dmesg > " + name + "'"}
		var results []install.RunResultDetails
		var combinedDmesgError error
		if results, combinedDmesgError = c.RunWithDetails(ctx, nil, c.All(), cmd...); combinedDmesgError != nil {
			return errors.Wrap(combinedDmesgError, "cluster.FetchDmesg")
		}

		var successfulNodes option.NodeListOption
		for _, result := range results {
			if result.Err != nil {
				// Store `Run` errors to return later (after copying files from successful nodes).
				combinedDmesgError = errors.CombineErrors(combinedDmesgError, result.Err)
				t.L().Printf("running dmesg failed on node %d: %v", result.Node, result.Err)
			} else {
				// Only run `Get` on successful nodes to avoid pseudo-failure on `Get` caused by an earlier failure on `Run`.
				successfulNodes = append(successfulNodes, int(result.Node))
			}
		}

		// Get dmesg files from successful nodes only.
		if err := c.Get(ctx, c.l, name /* src */, path /* dest */, successfulNodes); err != nil {
			t.L().Printf("getting dmesg files failed: %v", err)
			return errors.Wrap(err, "cluster.FetchDmesg")
		}

		// Return an error if running dmesg failed on any node.
		return combinedDmesgError
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

		// Run journalctl on all nodes to redirect journal logs to a file.
		cmd := []string{"/bin/bash", "-c", "'sudo journalctl > " + name + "'"}
		var results []install.RunResultDetails
		var combinedJournalctlError error
		if results, combinedJournalctlError = c.RunWithDetails(ctx, nil, c.All(), cmd...); combinedJournalctlError != nil {
			return errors.Wrap(combinedJournalctlError, "cluster.FetchJournalctl")
		}

		var successfulNodes option.NodeListOption
		for _, result := range results {
			if result.Err != nil {
				// Store `Run` errors to return later (after copying files from successful nodes).
				combinedJournalctlError = errors.CombineErrors(combinedJournalctlError, result.Err)
				t.L().Printf("running journalctl failed on node %d: %v", result.Node, result.Err)
			} else {
				// Only run `Get` on successful nodes to avoid pseudo-failure on `Get` caused by an earlier failure on `Run`.
				successfulNodes = append(successfulNodes, int(result.Node))
			}
		}

		// Get files from successful nodes only.
		if err := c.Get(ctx, c.l, name /* src */, path /* dest */, successfulNodes); err != nil {
			t.L().Printf("getting files failed: %v", err)
			return errors.Wrap(err, "cluster.FetchJournalctl")
		}

		// Return an error if running journalctl failed on any node.
		return combinedJournalctlError
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
		return errors.Wrap(c.Get(ctx, c.l, "/mnt/data1/cores" /* src */, path /* dest */), "cluster.FetchCores")
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
			if err := roachprod.Destroy(l, false /* destroyAllMine */, false /* destroyAllLocal */, c.name); err != nil {
				l.ErrorfCtx(ctx, "error destroying cluster %s: %s", c, err)
			} else {
				l.PrintfCtx(ctx, "destroying cluster %s... done", c)
			}
			if c.destroyState.alloc != nil {
				// We should usually have an alloc here, but if we're getting into this
				// code path while retrying cluster creation, we don't want the alloc
				// to be released (as we're going to retry cluster creation) and it will
				// be nil here.
				c.destroyState.alloc.Release()
			}
		} else {
			l.PrintfCtx(ctx, "wiping cluster %s", c)
			c.status("wiping cluster")
			if err := roachprod.Wipe(ctx, l, c.name, false /* preserveCerts */); err != nil {
				l.Errorf("%s", err)
			}
			if c.localCertsDir != "" {
				if err := os.RemoveAll(c.localCertsDir); err != nil {
					l.Errorf("failed to remove local certs in %s: %s", c.localCertsDir, err)
				}
				c.localCertsDir = ""
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
func (c *clusterImpl) Put(ctx context.Context, src, dest string, nodes ...option.Option) {
	if err := c.PutE(ctx, c.l, src, dest, nodes...); err != nil {
		c.t.Fatal(err)
	}
}

// PutE puts a local file to all of the machines in a cluster.
func (c *clusterImpl) PutE(
	ctx context.Context, l *logger.Logger, src, dest string, nodes ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}

	c.status("uploading file")
	defer c.status("")
	return errors.Wrap(roachprod.Put(ctx, l, c.MakeNodes(nodes...), src, dest, true /* useTreeDist */), "cluster.PutE")
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
			return errors.Wrap(err, "cluster.PutLibraries")
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
	return errors.Wrap(roachprod.Stage(ctx, l, c.MakeNodes(opts...), "" /* stageOS */, dir, application, versionOrSHA), "cluster.Stage")
}

// Get gets files from remote hosts.
func (c *clusterImpl) Get(
	ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Get")
	}
	c.status(fmt.Sprintf("getting %v", src))
	defer c.status("")
	return errors.Wrap(roachprod.Get(l, c.MakeNodes(opts...), src, dest), "cluster.Get")
}

// Put a string into the specified file on the remote(s).
func (c *clusterImpl) PutString(
	ctx context.Context, content, dest string, mode os.FileMode, nodes ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.PutString")
	}
	c.status("uploading string")
	defer c.status("")

	temp, err := ioutil.TempFile("", filepath.Base(dest))
	if err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	if _, err := temp.WriteString(content); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	temp.Close()
	src := temp.Name()

	if err := os.Chmod(src, mode); err != nil {
		return errors.Wrap(err, "cluster.PutString")
	}
	// NB: we intentionally don't remove the temp files. This is because roachprod
	// will symlink them when running locally.

	return errors.Wrap(c.PutE(ctx, c.l, src, dest, nodes...), "cluster.PutString")
}

// GitClone clones a git repo from src into dest and checks out origin's
// version of the given branch. The src, dest, and branch arguments must not
// contain shell special characters.
func (c *clusterImpl) GitClone(
	ctx context.Context, l *logger.Logger, src, dest, branch string, nodes option.NodeListOption,
) error {
	cmd := []string{"bash", "-e", "-c", fmt.Sprintf(`'
		if ! test -d %[1]s; then
			git clone -b %[2]s --depth 1 %[3]s %[1]s
  		else
			cd %[1]s
		git fetch origin
		git checkout origin/%[2]s
  		fi
  		'`, dest, branch, src),
	}
	return errors.Wrap(c.RunE(ctx, nodes, cmd...), "cluster.GitClone")
}

func (c *clusterImpl) setStatusForClusterOpt(
	operation string, worker bool, nodesOptions ...option.Option,
) {
	var nodes option.NodeListOption
	for _, o := range nodesOptions {
		if s, ok := o.(nodeSelector); ok {
			nodes = s.Merge(nodes)
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

func (c *clusterImpl) clearStatusForClusterOpt(worker bool) {
	if worker {
		c.workerStatus()
	} else {
		c.status()
	}
}

// StartE starts cockroach nodes on a subset of the cluster. The nodes parameter
// can either be a specific node, empty (to indicate all nodes), or a pair of
// nodes indicating a range.
func (c *clusterImpl) StartE(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
	opts ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.StartE")
	}
	// If the test failed (indicated by a canceled ctx), short-circuit.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.setStatusForClusterOpt("starting", startOpts.RoachtestOpts.Worker, opts...)
	defer c.clearStatusForClusterOpt(startOpts.RoachtestOpts.Worker)

	if startOpts.RoachtestOpts.DontEncrypt {
		startOpts.RoachprodOpts.EncryptedStores = false
	} else if c.encryptDefault {
		startOpts.RoachprodOpts.EncryptedStores = true
	} else if c.encryptAtRandom {
		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		if rng.Intn(2) == 1 {
			// Force encryption in future calls of Start with the same cluster.
			c.encryptDefault = true
			startOpts.RoachprodOpts.EncryptedStores = true
		}
	}

	// Set some env vars. The first two also the default for `roachprod start`,
	// but we have to add them so that the third one doesn't wipe them out.
	if !envExists(settings.Env, "COCKROACH_ENABLE_RPC_COMPRESSION") {
		// RPC compressions costs around 5% on kv95, so we disable it. It might help
		// when moving snapshots around, though.
		settings.Env = append(settings.Env, "COCKROACH_ENABLE_RPC_COMPRESSION=false")
	}

	if !envExists(settings.Env, "COCKROACH_UI_RELEASE_NOTES_SIGNUP_DISMISSED") {
		// Get rid of an annoying popup in the UI.
		settings.Env = append(settings.Env, "COCKROACH_UI_RELEASE_NOTES_SIGNUP_DISMISSED=true")
	}

	if !envExists(settings.Env, "COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH") {
		// Panic on span use-after-Finish, so we catch such bugs.
		settings.Env = append(settings.Env, "COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH=true")
	}

	clusterSettingsOpts := []install.ClusterSettingOption{
		install.TagOption(settings.Tag),
		install.PGUrlCertsDirOption(settings.PGUrlCertsDir),
		install.SecureOption(settings.Secure),
		install.UseTreeDistOption(settings.UseTreeDist),
		install.EnvOption(settings.Env),
		install.NumRacksOption(settings.NumRacks),
		install.BinaryOption(settings.Binary),
	}

	if err := roachprod.Start(ctx, l, c.MakeNodes(opts...), startOpts.RoachprodOpts, clusterSettingsOpts...); err != nil {
		return err
	}

	if settings.Secure {
		var err error
		c.localCertsDir, err = ioutil.TempDir("", "roachtest-certs")
		if err != nil {
			return err
		}
		// `roachprod get` behaves differently with `--local` depending on whether
		// the target dir exists. With `--local`, it'll put the files into the
		// existing dir. Without `--local`, it'll create a new subdir to house the
		// certs. Bypass that distinction (which should be fixed independently, but
		// that might cause fallout) by using a non-existing dir here.
		c.localCertsDir = filepath.Join(c.localCertsDir, "certs")
		// Get the certs from the first node.
		if err := c.Get(ctx, c.l, "./certs", c.localCertsDir, c.Node(1)); err != nil {
			return errors.Wrap(err, "cluster.StartE")
		}
		// Need to prevent world readable files or lib/pq will complain.
		if err := filepath.Walk(c.localCertsDir, func(path string, info fs.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			return os.Chmod(path, 0600)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Start is like StartE() except that it will fatal the test on error.
func (c *clusterImpl) Start(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
	opts ...option.Option,
) {
	if err := c.StartE(ctx, l, startOpts, settings, opts...); err != nil {
		c.t.Fatal(err)
	}
}

func envExists(envs []string, prefix string) bool {
	for _, env := range envs {
		if strings.HasPrefix(env, prefix) {
			return true
		}
	}
	return false
}

// StopE cockroach nodes running on a subset of the cluster. See cluster.Start()
// for a description of the nodes parameter.
func (c *clusterImpl) StopE(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, nodes ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.StopE")
	}
	if c.spec.NodeCount == 0 {
		return nil // unit tests
	}
	c.setStatusForClusterOpt("stopping", stopOpts.RoachtestOpts.Worker, nodes...)
	defer c.clearStatusForClusterOpt(stopOpts.RoachtestOpts.Worker)
	return errors.Wrap(roachprod.Stop(ctx, l, c.MakeNodes(nodes...), stopOpts.RoachprodOpts), "cluster.StopE")
}

// Stop is like StopE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Stop(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option,
) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.StopE(ctx, l, stopOpts, opts...); err != nil {
		c.t.Fatal(err)
	}
}

func (c *clusterImpl) Reset(ctx context.Context, l *logger.Logger) error {
	if c.t.Failed() {
		return errors.New("already failed")
	}
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Reset")
	}
	c.status("resetting cluster")
	defer c.status()
	return errors.Wrap(roachprod.Reset(l, c.name), "cluster.Reset")
}

// WipeE wipes a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *clusterImpl) WipeE(ctx context.Context, l *logger.Logger, nodes ...option.Option) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.WipeE")
	}
	if c.spec.NodeCount == 0 {
		// For tests.
		return nil
	}
	c.setStatusForClusterOpt("wiping", false, nodes...)
	defer c.clearStatusForClusterOpt(false)
	return roachprod.Wipe(ctx, l, c.MakeNodes(nodes...), false /* preserveCerts */)
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Wipe(ctx context.Context, nodes ...option.Option) {
	if ctx.Err() != nil {
		return
	}
	if err := c.WipeE(ctx, c.l, nodes...); err != nil {
		c.t.Fatal(err)
	}
}

// Run a command on the specified nodes and call test.Fatal if there is an error.
func (c *clusterImpl) Run(ctx context.Context, node option.NodeListOption, args ...string) {
	err := c.RunE(ctx, node, args...)
	if err != nil {
		c.t.Fatal(err)
	}
}

// RunE runs a command on the specified node, returning an error. The output
// will be redirected to a file which is logged via the cluster-wide logger in
// case of an error. Logs will sort chronologically. Failing invocations will
// have an additional marker file with a `.failed` extension instead of `.log`.
func (c *clusterImpl) RunE(ctx context.Context, node option.NodeListOption, args ...string) error {
	if len(args) == 0 {
		return errors.New("No command passed")
	}
	l, logFile, err := c.loggerForCmd(node, args...)
	if err != nil {
		return err
	}

	if err := errors.Wrap(ctx.Err(), "cluster.RunE"); err != nil {
		return err
	}
	err = execCmd(ctx, l, c.MakeNodes(node), args...)

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

// RunWithDetailsSingleNode is just like RunWithDetails but used when 1) operating
// on a single node AND 2) an error from roachprod itself would be treated the same way
// you treat an error from the command. This makes error checking easier / friendlier
// and helps us avoid code replication.
func (c *clusterImpl) RunWithDetailsSingleNode(
	ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string,
) (install.RunResultDetails, error) {
	if len(nodes) != 1 {
		return install.RunResultDetails{}, errors.Newf("RunWithDetailsSingleNode received %d nodes. Use RunWithDetails if you need to run on multiple nodes.", len(nodes))
	}
	results, err := c.RunWithDetails(ctx, testLogger, nodes, args...)
	if err != nil {
		return install.RunResultDetails{}, err
	}
	return results[0], results[0].Err
}

// RunWithDetails runs a command on the specified nodes, returning the results
// details and a `roachprod` error. The output will be redirected to a file which is logged
// via the cluster-wide logger in case of an error. Failing invocations will have
// an additional marker file with a `.failed` extension instead of `.log`.
func (c *clusterImpl) RunWithDetails(
	ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string,
) ([]install.RunResultDetails, error) {
	if len(args) == 0 {
		return nil, errors.New("No command passed")
	}
	l, _, err := c.loggerForCmd(nodes, args...)
	if err != nil {
		return nil, err
	}
	physicalFileName := l.File.Name()

	if err := ctx.Err(); err != nil {
		l.Printf("(note: incoming context was canceled: %s", err)
		return nil, err
	}

	l.Printf("running %s on nodes: %v", strings.Join(args, " "), nodes)
	if testLogger != nil {
		testLogger.Printf("> %s\n", strings.Join(args, " "))
	}

	results, err := roachprod.RunWithDetails(ctx, l, c.MakeNodes(nodes), "" /* SSHOptions */, "" /* processTag */, false /* secure */, args)
	if err != nil {
		l.Printf("> result: %+v", err)
		createFailedFile(physicalFileName)
		return results, err
	}

	for _, result := range results {
		if result.Err != nil {
			err = result.Err
			l.Printf("> Error for Node %d: %+v", int(result.Node), result.Err)
		}
	}
	if err != nil {
		createFailedFile(physicalFileName)
	}
	l.Close()
	return results, nil
}

func createFailedFile(logFileName string) {
	failedPhysicalFileName := strings.TrimSuffix(logFileName, ".log") + ".failed"
	if failedFile, err2 := os.Create(failedPhysicalFileName); err2 != nil {
		failedFile.Close()
	}
}

// Reformat the disk on the specified node.
func (c *clusterImpl) Reformat(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, filesystem string,
) error {
	return roachprod.Reformat(ctx, l, c.name, filesystem)
}

// Silence unused warning.
var _ = (&clusterImpl{}).Reformat

// Install a package in a node
func (c *clusterImpl) Install(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, software ...string,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Install")
	}
	if len(software) == 0 {
		return errors.New("Error running cluster.Install: no software passed")
	}
	return errors.Wrap(roachprod.Install(ctx, l, c.MakeNodes(nodes), software), "cluster.Install")
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
		t.Format(`150405.000000000`),
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

// pgURLErr returns the Postgres endpoint for the specified node. It accepts a
// flag specifying whether the URL should include the node's internal or
// external IP address. In general, inter-cluster communication and should use
// internal IPs and communication from a test driver to nodes in a cluster
// should use external IPs.
func (c *clusterImpl) pgURLErr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, external bool,
) ([]string, error) {
	urls, err := roachprod.PgURL(ctx, l, c.MakeNodes(node), c.localCertsDir, external, c.localCertsDir != "" /* secure */)
	if err != nil {
		return nil, err
	}
	for i, url := range urls {
		urls[i] = strings.Trim(url, "'")
	}
	return urls, nil
}

// InternalPGUrl returns the internal Postgres endpoint for the specified nodes.
func (c *clusterImpl) InternalPGUrl(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	return c.pgURLErr(ctx, l, node, false /* external */)
}

// Silence unused warning.
var _ = (&clusterImpl{}).InternalPGUrl

// ExternalPGUrl returns the external Postgres endpoint for the specified nodes.
func (c *clusterImpl) ExternalPGUrl(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	return c.pgURLErr(ctx, l, node, true /* external */)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.InternalAddr(ctx, l, node)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	externalAddrs, err := c.ExternalAddr(ctx, l, node)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, l, node, false /* external */)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var ips []string
	addrs, err := c.InternalAddr(ctx, l, node)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, l, node, true /* external */)
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var ips []string
	addrs, err := c.ExternalAddr(ctx, l, node)
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
func (c *clusterImpl) Conn(ctx context.Context, l *logger.Logger, node int) *gosql.DB {
	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node))
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
func (c *clusterImpl) ConnE(ctx context.Context, l *logger.Logger, node int) (*gosql.DB, error) {
	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node))
	if err != nil {
		return nil, err
	}
	db, err := gosql.Open("postgres", urls[0])
	if err != nil {
		return nil, err
	}
	return db, nil
}

// ConnEAsUser returns a SQL connection to the specified node as a specific user
func (c *clusterImpl) ConnEAsUser(
	ctx context.Context, l *logger.Logger, node int, user string,
) (*gosql.DB, error) {
	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node))
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(urls[0])
	if err != nil {
		return nil, err
	}
	u.User = url.User(user)
	dataSourceName := u.String()
	db, err := gosql.Open("postgres", dataSourceName)
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

// Extend extends the cluster's expiration by d.
func (c *clusterImpl) Extend(ctx context.Context, d time.Duration, l *logger.Logger) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Extend")
	}
	l.PrintfCtx(ctx, "extending cluster by %s", d.String())
	if err := roachprod.Extend(l, c.name, d); err != nil {
		l.PrintfCtx(ctx, "roachprod extend failed: %v", err)
		return errors.Wrap(err, "roachprod extend failed")
	}
	// Update c.expiration. Keep it under the real expiration.
	c.expiration = c.expiration.Add(d)
	return nil
}

func (c *clusterImpl) NewMonitor(ctx context.Context, opts ...option.Option) cluster.Monitor {
	return newMonitor(ctx, c.t, c, opts...)
}
