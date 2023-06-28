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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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
	// user-specified path to crdb binary
	cockroachPath string
	// maps cpuArch to the corresponding crdb binary's absolute path
	cockroach = make(map[vm.CPUArch]string)
	// user-specified path to crdb binary with runtime assertions enabled (EA)
	cockroachEAPath string
	// maps cpuArch to the corresponding crdb binary with runtime assertions enabled (EA)
	cockroachEA = make(map[vm.CPUArch]string)
	// user-specified path to workload binary
	workloadPath string
	// maps cpuArch to the corresponding workload binary's absolute path
	workload = make(map[vm.CPUArch]string)
	// maps cpuArch to the corresponding dynamically-linked libraries' absolute paths
	libraryFilePaths = make(map[vm.CPUArch][]string)
	cloud            = spec.GCE
	// encryptionProbability controls when encryption-at-rest is enabled
	// in a cluster for tests that have opted-in to metamorphic
	// encryption (EncryptionMetamorphic).
	//
	// Tests that have opted-in to metamorphic encryption will run with
	// encryption enabled by default (probability 1). In order to run
	// them with encryption disabled (perhaps to reproduce a test
	// failure), roachtest can be invoked with --metamorphic-encryption-probability=0
	encryptionProbability float64
	// Total probability with which new ARM64 clusters are provisioned, modulo test specs. which are incompatible.
	// N.B. if all selected tests are incompatible with ARM64, then arm64Probability is effectively 0.
	// In other words, ClusterSpec.Arch takes precedence over the arm64Probability flag.
	arm64Probability float64
	// Conditional probability with which new FIPS clusters are provisioned, modulo test specs. The total probability
	// is the product of this and 1-arm64Probability.
	// As in the case of arm64Probability, ClusterSpec.Arch takes precedence over the fipsProbability flag.
	fipsProbability float64

	instanceType              string
	localSSDArg               bool
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
	clusterName      string
	clusterWipe      bool
	zonesF           string
	teamCity         bool
	disableIssue     bool
)

const (
	defaultEncryptionProbability = 1
	defaultFIPSProbability       = 0
	defaultARM64Probability      = 0
	defaultCockroachPath         = "./cockroach-default"
)

type errBinaryOrLibraryNotFound struct {
	binary string
}

func (e errBinaryOrLibraryNotFound) Error() string {
	return fmt.Sprintf("binary or library %q not found (or was not executable)", e.binary)
}

func validateBinaryFormat(path string, arch vm.CPUArch, checkEA bool) (string, error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return "", errors.WithStack(err)
	}
	// Check that the binary ELF format matches the expected architecture.
	cmd := exec.Command("file", "-b", abspath)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", errors.Wrapf(err, "error executing 'file %s'", abspath)
	}
	fileFormat := strings.ToLower(out.String())
	// N.B. 'arm64' is returned on macOS, while 'aarch64' is returned on Linux;
	// "x86_64" string is returned on macOS, while "x86-64" is returned on Linux.
	if arch == vm.ArchARM64 && !strings.Contains(fileFormat, "arm64") && !strings.Contains(fileFormat, "aarch64") {
		return "", errors.Newf("%s has incompatible architecture; want: %q, got: %q", abspath, arch, fileFormat)
	} else if arch == vm.ArchAMD64 && !strings.Contains(fileFormat, "x86-64") && !strings.Contains(fileFormat, "x86_64") {
		// Otherwise, we expect a binary that was built for amd64.
		return "", errors.Newf("%s has incompatible architecture; want: %q, got: %q", abspath, arch, fileFormat)
	}
	if arch == vm.ArchFIPS && strings.HasSuffix(abspath, "cockroach") {
		// Check that the binary is patched to use OpenSSL FIPS.
		// N.B. only the cockroach binary is patched, so we exclude this check for dynamically-linked libraries.
		cmd = exec.Command("bash", "-c", fmt.Sprintf("nm %s | grep golang-fips |head -1", abspath))
		if err := cmd.Run(); err != nil {
			return "", errors.Newf("%s is not compiled with FIPS", abspath)
		}
	}
	if checkEA {
		// Check that the binary was compiled with assertions _enabled_.
		cmd = exec.Command("bash", "-c", fmt.Sprintf("%s version |grep \"Enabled Assertions\" |grep true", abspath))
		if err := cmd.Run(); err != nil {
			return "", errors.Newf("%s is not compiled with assertions enabled", abspath)
		}
	}

	return abspath, nil
}

func findBinary(
	name string, osName string, arch vm.CPUArch, checkEA bool,
) (abspath string, err error) {
	// Check to see if binary exists and is a regular file and executable.
	if fi, err := os.Stat(name); err == nil && fi.Mode().IsRegular() && (fi.Mode()&0111) != 0 {
		return validateBinaryFormat(name, arch, checkEA)
	}
	return findBinaryOrLibrary("bin", name, "", osName, arch, checkEA)
}

func findLibrary(libraryName string, os string, arch vm.CPUArch) (string, error) {
	suffix := ".so"
	if cloud == spec.Local {
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

	return findBinaryOrLibrary("lib", libraryName, suffix, os, arch, false)
}

// findBinaryOrLibrary searches for a binary or library, _first_ in the $PATH, _then_ in the following hardcoded paths,
//
//	$GOPATH/src/github.com/cockroachdb/cockroach/
//	$GOPATH/src/github.com/cockroachdb/artifacts/
//	$PWD/binOrLib
//	$GOPATH/src/github.com/cockroachdb/cockroach/binOrLib
//
// in the above order, unless 'name' is an absolute path, in which case the hardcoded paths are skipped.
//
// binOrLib is either 'bin' or 'lib'; nameSuffix is either empty, '.so', '.dll', or '.dylib'.
// Both osName and arch are used to derive a fully qualified binary or library name by inserting the
// corresponding arch suffix (see install.ArchInfoForOS), e.g. '.linux-arm64' or '.darwin-amd64'.
// That is, each hardcoded path is searched for a file named 'name' or 'name.nameSuffix.archSuffix', respectively.
//
// If no binary or library is found, an error is returned.
// Otherwise, if multiple binaries or libraries are located at the above paths, the first one found is returned.
// If the found binary or library happens to be of the wrong type, e.g., architecture is different from 'arch', or
// checkEA is true, and the binary was not compiled with runtime assertions enabled, an error is returned.
// While we could continue the search instead of returning an error, it is assumed the user can stage the binaries
// to avoid such ambiguity. Alternatively, the user can specify the absolute path to the binary or library,
// e.g., via --cockroach; in this case, only the absolute path is checked and validated.
func findBinaryOrLibrary(
	binOrLib string, name string, nameSuffix string, osName string, arch vm.CPUArch, checkEA bool,
) (string, error) {
	// Find the binary to run and translate it to an absolute path. First, look
	// for the binary in PATH.
	pathFromEnv, err := exec.LookPath(name)
	if err == nil {
		// Found it in PATH, validate and return absolute path.
		return validateBinaryFormat(pathFromEnv, arch, checkEA)
	}
	if strings.HasPrefix(name, "/") {
		// Specified name is an absolute path, but we couldn't find it; bail out.
		return "", errors.WithStack(err)
	}
	// We're unable to find the name in PATH and "name" is a relative path:
	// look in the cockroach repo.
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		gopath = filepath.Join(os.Getenv("HOME"), "go")
	}

	dirs := []string{
		filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/"),
		filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach/artifacts/"),
		filepath.Join(os.ExpandEnv("$PWD"), binOrLib),
		filepath.Join(gopath, "/src/github.com/cockroachdb/cockroach", binOrLib),
	}

	archInfo, err := install.ArchInfoForOS(osName, arch)
	if err != nil {
		return "", err
	}
	archSuffixes := []string{"." + archInfo.DebugArchitecture, "." + archInfo.ReleaseArchitecture}

	for _, dir := range dirs {
		var path string

		if path, err = exec.LookPath(filepath.Join(dir, name+nameSuffix)); err == nil {
			return validateBinaryFormat(path, arch, checkEA)
		}
		for _, archSuffix := range archSuffixes {
			if path, err = exec.LookPath(filepath.Join(dir, name+archSuffix+nameSuffix)); err == nil {
				return validateBinaryFormat(path, arch, checkEA)
			}
		}
	}
	return "", errBinaryOrLibraryNotFound{name}
}

// VerifyLibraries verifies that the required libraries, specified by name, are
// available for the target environment.
func VerifyLibraries(requiredLibs []string, arch vm.CPUArch) error {
	foundLibraryPaths := libraryFilePaths[arch]

	for _, requiredLib := range requiredLibs {
		if !contains(foundLibraryPaths, libraryNameFromPath, requiredLib) {
			return errors.Wrap(errors.Errorf("missing required library %s (arch=%q)", requiredLib, arch), "cluster.VerifyLibraries")
		}
	}
	return nil
}

// libraryNameFromPath returns the name of a library without the extension(s), for a
// given path.
func libraryNameFromPath(path string) string {
	filename := filepath.Base(path)
	// N.B. filename may contain multiple extensions, e.g. "libgeos.linux-amd64.fips.so".
	for ext := filepath.Ext(filename); ext != ""; ext = filepath.Ext(filename) {
		filename = strings.TrimSuffix(filename, ext)
	}
	return filename
}

func contains(list []string, transformString func(s string) string, str string) bool {
	if transformString == nil {
		transformString = func(s string) string { return s }
	}
	for _, element := range list {
		if transformString(element) == str {
			return true
		}
	}
	return false
}

func initBinariesAndLibraries() {
	// TODO(srosenberg): enable metamorphic local clusters; currently, spec.Local means run all tests locally.
	// This could be revisited after we have a way to specify which clouds a given test supports,
	//	see https://github.com/cockroachdb/cockroach/issues/104029.
	defaultOSName := "linux"
	defaultArch := vm.ArchAMD64
	if cloud == spec.Local {
		defaultOSName = runtime.GOOS
		if arm64Probability == 1 {
			// N.B. if arm64Probability != 1, then we're running a local cluster with both arm64 and amd64.
			defaultArch = vm.ArchARM64
		}
		if string(defaultArch) != runtime.GOARCH {
			fmt.Printf("WARN: local cluster's architecture (%q) differs from default (%q)\n", runtime.GOARCH, defaultArch)
		}
	}
	fmt.Printf("Locating and verifying binaries for os=%q, arch=%q\n", defaultOSName, defaultArch)

	// Finds and validates a binary.
	resolveBinary := func(binName string, userSpecified string, arch vm.CPUArch, exitOnErr bool, checkEA bool) (string, error) {
		path := binName
		if userSpecified != "" {
			path = userSpecified
		}
		abspath, err := findBinary(path, defaultOSName, arch, checkEA)
		if err != nil {
			if exitOnErr {
				fmt.Fprintf(os.Stderr, "ERROR: unable to find required binary %q for %q: %v\n", binName, arch, err)
				os.Exit(1)
			}
			return "", err
		}
		if userSpecified == "" {
			// No user-specified path, so return the found absolute path.
			return abspath, nil
		}
		// Bail out if a path other than the user-specified was found.
		userPath, err := filepath.Abs(userSpecified)
		if err != nil {
			if exitOnErr {
				fmt.Fprintf(os.Stderr, "ERROR: unable to find required binary %q for %q: %v\n", binName, arch, err)
				os.Exit(1)
			}
			return "", err
		}
		if userPath != abspath {
			err = errors.Errorf("found %q at: %q instead of the user-specified path: %q\n", binName, abspath, userSpecified)

			if exitOnErr {
				fmt.Fprintf(os.Stderr, "ERROR: unable to find required binary %q for %q: %v\n", binName, arch, err)
				os.Exit(1)
			}
			return "", err
		}
		return abspath, nil
	}
	// We need to verify we have at least both the cockroach and the workload binaries.
	var err error

	cockroach[defaultArch], _ = resolveBinary("cockroach", cockroachPath, defaultArch, true, false)
	workload[defaultArch], _ = resolveBinary("workload", workloadPath, defaultArch, true, false)
	cockroachEA[defaultArch], err = resolveBinary("cockroach-ea", cockroachEAPath, defaultArch, false, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: unable to find %q for %q: %s\n", "cockroach-ea", defaultArch, err)
	}

	if arm64Probability > 0 && defaultArch != vm.ArchARM64 {
		fmt.Printf("Locating and verifying binaries for os=%q, arch=%q\n", defaultOSName, vm.ArchARM64)
		// We need to verify we have all the required binaries for arm64.
		cockroach[vm.ArchARM64], _ = resolveBinary("cockroach", cockroachPath, vm.ArchARM64, true, false)
		workload[vm.ArchARM64], _ = resolveBinary("workload", workloadPath, vm.ArchARM64, true, false)
		cockroachEA[vm.ArchARM64], err = resolveBinary("cockroach-ea", cockroachEAPath, vm.ArchARM64, false, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: unable to find %q for %q: %s\n", "cockroach-ea", vm.ArchARM64, err)
		}
	}
	if fipsProbability > 0 && defaultArch != vm.ArchFIPS {
		fmt.Printf("Locating and verifying binaries for os=%q, arch=%q\n", defaultOSName, vm.ArchFIPS)
		// We need to verify we have all the required binaries for fips.
		cockroach[vm.ArchFIPS], _ = resolveBinary("cockroach", cockroachPath, vm.ArchFIPS, true, false)
		workload[vm.ArchFIPS], _ = resolveBinary("workload", workloadPath, vm.ArchFIPS, true, false)
		cockroachEA[vm.ArchFIPS], err = resolveBinary("cockroach-ea", cockroachEAPath, vm.ArchFIPS, false, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: unable to find %q for %q: %s\n", "cockroach-ea", vm.ArchFIPS, err)
		}
	}

	// In v20.2 or higher, optionally expect certain library files to exist.
	// Since they may not be found in older versions, do not hard error if they are not found.
	for _, arch := range []vm.CPUArch{vm.ArchAMD64, vm.ArchARM64, vm.ArchFIPS} {
		if arm64Probability == 0 && defaultArch != vm.ArchARM64 && arch == vm.ArchARM64 {
			// arm64 isn't used, skip finding libs for it.
			continue
		}
		if fipsProbability == 0 && arch == vm.ArchFIPS {
			// fips isn't used, skip finding libs for it.
			continue
		}
		paths := []string(nil)

		for _, libraryName := range []string{"libgeos", "libgeos_c"} {
			if libraryFilePath, err := findLibrary(libraryName, defaultOSName, arch); err != nil {
				fmt.Fprintf(os.Stderr, "WARN: unable to find library %s, ignoring: %s\n", libraryName, err)
			} else {
				paths = append(paths, libraryFilePath)
			}
		}
		libraryFilePaths[arch] = paths
	}
	// Looks like we have all the binaries we'll need. Let's print them out.
	fmt.Printf("\nFound the following binaries:\n")
	for arch, path := range cockroach {
		if path != "" {
			fmt.Printf("\tcockroach %q at: %s\n", arch, path)
		}
	}
	for arch, path := range workload {
		if path != "" {
			fmt.Printf("\tworkload %q at: %s\n", arch, path)
		}
	}
	for arch, path := range cockroachEA {
		if path != "" {
			fmt.Printf("\tcockroach-ea %q at: %s\n", arch, path)
		}
	}
	for arch, paths := range libraryFilePaths {
		if len(paths) > 0 {
			fmt.Printf("\tlibraries %q at: %s\n", arch, strings.Join(paths, ", "))
		}
	}
}

// execCmd is like execCmdEx, but doesn't return the command's output.
func execCmd(
	ctx context.Context, l *logger.Logger, clusterName string, secure bool, args ...string,
) error {
	return execCmdEx(ctx, l, clusterName, secure, args...).err
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
func execCmdEx(
	ctx context.Context, l *logger.Logger, clusterName string, secure bool, args ...string,
) cmdRes {
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

	err := roachprod.Run(ctx, l, clusterName, "" /* SSHOptions */, "" /* processTag */, secure, roachprodRunStdout, roachprodRunStderr, args)
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
		if _, err := fmt.Sscanf(s, "n2-standard-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n2-standard-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n2-highcpu-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n2-highmem-%d", &v); err == nil {
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
		case "8xlarge":
			return 32
		case "12xlarge":
			return 48
		case "16xlarge":
			return 64
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
	encAtRest     bool // use encryption at rest

	// clusterSettings are additional cluster settings set on cluster startup.
	clusterSettings map[string]string

	os   string     // OS of the cluster
	arch vm.CPUArch // CPU architecture of the cluster
	// destroyState contains state related to the cluster's destruction.
	destroyState destroyState
}

// Name returns the cluster name, i.e. something like `teamcity-....`
func (c *clusterImpl) Name() string {
	return c.name
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
	nameOverride string
	spec         spec.ClusterSpec
	// artifactsDir is the path where log file will be stored.
	artifactsDir string
	// username is the username passed via the --username argument
	// or the ROACHPROD_USER command-line option.
	username     string
	localCluster bool
	useIOBarrier bool
	alloc        *quotapool.IntAlloc
	// Specifies CPU architecture which may require a custom AMI and cockroach binary.
	arch vm.CPUArch
	// Specifies the OS which may require a custom AMI and cockroach binary.
	os string
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
	if cfg.nameOverride != "" {
		return cfg.nameOverride
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
		if flags.Changed("label") {
			opts.CustomLabels = overrideOpts.CustomLabels
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
) (*clusterImpl, *vm.CreateOpts, error) {
	if ctx.Err() != nil {
		return nil, nil, errors.Wrap(ctx.Err(), "newCluster")
	}

	if overrideFlagset != nil && overrideFlagset.Changed("nodes") {
		cfg.spec.NodeCount = overrideNumNodes
	}

	if cfg.spec.NodeCount == 0 {
		// For tests, use a mock cluster.
		c := f.clusterMock(cfg)
		if err := f.r.registerCluster(c); err != nil {
			return nil, nil, err
		}
		return c, nil, nil
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
	createVMOpts, providerOpts, err := cfg.spec.RoachprodOpts("", cfg.useIOBarrier, cfg.arch)
	if err != nil {
		// We must release the allocation because cluster creation is not possible at this point.
		cfg.alloc.Release()

		return nil, nil, err
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
	maxAttempts := 3
	if cfg.nameOverride != "" {
		// Usually when retrying we pick a new name (to avoid repeat failures due to
		// partially created resources), but we were were asked to use a specific
		// name. To keep things simple, disable retries in that case.
		maxAttempts = 1
	}
	// loop assumes maxAttempts is atleast (1).
	for i := 1; ; i++ {
		c := &clusterImpl{
			// NB: this intentionally avoids re-using the name across iterations in
			// the loop. See:
			//
			// https://github.com/cockroachdb/cockroach/issues/67906#issuecomment-887477675
			name:       f.genName(cfg),
			spec:       cfg.spec,
			expiration: cfg.spec.Expiration(),
			r:          f.r,
			arch:       cfg.arch,
			os:         cfg.os,
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
				return nil, nil, err
			}
			c.status("idle")
			l.Close()
			return c, &createVMOpts, nil
		}

		if errors.HasType(err, (*roachprod.ClusterAlreadyExistsError)(nil)) {
			// If the cluster couldn't be created because it existed already, bail.
			// In reality when this is hit is when running with the `local` flag
			// or a destroy from the previous iteration failed.
			return nil, nil, err
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
			return nil, nil, err
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
		name:       name,
		spec:       spec,
		l:          l,
		expiration: exp,
		destroyState: destroyState{
			// If we're attaching to an existing cluster, we're not going to destroy it.
			owned: false,
		},
		r: r,
	}

	if !opt.skipValidation {
		if err := c.validate(ctx, spec, l); err != nil {
			return nil, err
		}
	}

	if err := r.registerCluster(c); err != nil {
		return nil, err
	}

	if !opt.skipStop {
		c.status("stopping cluster")
		if err := c.StopE(ctx, l, option.DefaultStopOpts(), c.All()); err != nil {
			return nil, err
		}
		if !opt.skipWipe {
			if clusterWipe {
				if err := c.WipeE(ctx, l, false /* preserveCerts */, c.All()); err != nil {
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
	// A graceful shutdown is sending SIGTERM to the node, then waiting
	// some reasonable amount of time, then sending a non-graceful SIGKILL.
	gracefulOpts := option.DefaultStopOpts()
	gracefulOpts.RoachprodOpts.Sig = 15 // SIGTERM
	gracefulOpts.RoachprodOpts.Wait = true
	gracefulOpts.RoachprodOpts.MaxWait = 60
	if err := c.StopE(ctx, l, gracefulOpts, c.Node(node)); err != nil {
		return err
	}

	// NB: we still call Stop to make sure the process is dead when we
	// try to restart it (in case it takes longer than `MaxWait` for it
	// to finish).
	return c.StopE(ctx, l, option.DefaultStopOpts(), c.Node(node))
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

var errClusterNotFound = errors.New("cluster not found")

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
	cloudClusters, err := roachprod.List(l, false /* listMine */, pattern, vm.ListOptions{})
	if err != nil {
		return err
	}
	cDetails, ok := cloudClusters.Clusters[c.name]
	if !ok {
		return errors.Wrapf(errClusterNotFound, "%q", c.name)
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
			// Clouds typically don't support odd numbers of vCPUs; they can result in subtle performance issues.
			// N.B. Some machine families, e.g., n2 in GCE, do not support 1 vCPU. (See AWSMachineType and GCEMachineType.)
			if vmCPUs > 1 && vmCPUs&1 == 1 {
				return fmt.Errorf("node %d has an _odd_ number of CPUs (%d)", i, vmCPUs)
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
func (c *clusterImpl) FetchLogs(ctx context.Context, l *logger.Logger) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	l.Printf("fetching logs\n")
	c.status("fetching logs")

	// Don't hang forever if we can't fetch the logs.
	return timeutil.RunWithTimeout(ctx, "fetch logs", 2*time.Minute, func(ctx context.Context) error {
		path := filepath.Join(c.t.ArtifactsDir(), "logs", "unredacted")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}

		if err := c.Get(ctx, c.l, "logs" /* src */, path /* dest */); err != nil {
			l.Printf("failed to fetch logs: %v", err)
			if ctx.Err() != nil {
				return errors.Wrap(err, "cluster.FetchLogs")
			}
		}

		if err := c.RunE(ctx, c.All(), fmt.Sprintf("mkdir -p logs/redacted && %s debug merge-logs --redact logs/*.log > logs/redacted/combined.log", defaultCockroachPath)); err != nil {
			l.Printf("failed to redact logs: %v", err)
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
	return timeutil.RunWithTimeout(ctx, "disk usage", 20*time.Second, func(ctx context.Context) error {
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
func (c *clusterImpl) FetchTimeseriesData(ctx context.Context, l *logger.Logger) error {
	l.Printf("fetching timeseries data\n")
	return timeutil.RunWithTimeout(ctx, "fetch tsdata", 5*time.Minute, func(ctx context.Context) error {
		node := 1
		for ; node <= c.spec.NodeCount; node++ {
			db, err := c.ConnE(ctx, l, node)
			if err == nil {
				err = db.Ping()
				db.Close()
			}
			if err != nil {
				l.Printf("node %d not responding to SQL, trying next one", node)
				continue
			}
			break
		}
		if node > c.spec.NodeCount {
			return errors.New("no node responds to SQL, cannot fetch tsdata")
		}
		sec := "--insecure"
		if c.IsSecure() {
			certs := "certs"
			if c.IsLocal() {
				certs = c.localCertsDir
			}
			sec = fmt.Sprintf("--certs-dir=%s", certs)
		}
		if err := c.RunE(
			ctx, c.Node(node), fmt.Sprintf("%s debug tsdump %s --format=raw > tsdump.gob", defaultCockroachPath, sec),
		); err != nil {
			return err
		}
		tsDumpGob := filepath.Join(c.t.ArtifactsDir(), "tsdump.gob")
		if err := c.Get(
			ctx, c.l, "tsdump.gob", tsDumpGob, c.Node(node),
		); err != nil {
			return errors.Wrap(err, "cluster.FetchTimeseriesData")
		}
		db, err := c.ConnE(ctx, l, node)
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
		if err := os.WriteFile(tsDumpGob+".yaml", buf.Bytes(), 0644); err != nil {
			return err
		}
		return os.WriteFile(tsDumpGob+"-run.sh", []byte(`#!/usr/bin/env bash

COCKROACH_DEBUG_TS_IMPORT_FILE=tsdump.gob cockroach start-single-node --insecure $*
`), 0755)
	})
}

// FetchDebugZip downloads the debug zip from the cluster using `roachprod ssh`.
// The logs will be placed at `dest`, relative to the test's artifacts dir.
func (c *clusterImpl) FetchDebugZip(ctx context.Context, l *logger.Logger, dest string) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	l.Printf("fetching debug zip\n")
	c.status("fetching debug zip")

	// Don't hang forever if we can't fetch the debug zip.
	return timeutil.RunWithTimeout(ctx, "debug zip", 5*time.Minute, func(ctx context.Context) error {
		const zipName = "debug.zip"
		path := filepath.Join(c.t.ArtifactsDir(), dest)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		// Some nodes might be down, so try to find one that works. We make the
		// assumption that a down node will refuse the connection, so it won't
		// waste our time.
		for i := 1; i <= c.spec.NodeCount; i++ {
			// `cockroach debug zip` is noisy. Suppress the output unless it fails.
			//
			// Ignore the files in the the log directory; we pull the logs separately anyway
			// so this would only cause duplication.
			excludeFiles := "*.log,*.txt,*.pprof"
			cmd := roachtestutil.NewCommand("%s debug zip", defaultCockroachPath).
				Option("include-range-info").
				Flag("exclude-files", fmt.Sprintf("'%s'", excludeFiles)).
				Flag("url", fmt.Sprintf("{pgurl:%d}", i)).
				MaybeFlag(c.IsSecure(), "certs-dir", "certs").
				Arg(zipName).
				String()
			if err := c.RunE(ctx, c.Node(i), cmd); err != nil {
				l.Printf("%s debug zip failed on node %d: %v", defaultCockroachPath, i, err)
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

// checkNoDeadNode returns an error if at least one of the nodes that have a populated
// data dir are found to be not running. It prints both to t.L() and the test
// output.
func (c *clusterImpl) assertNoDeadNode(ctx context.Context, t test.Test) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	t.L().Printf("checking for dead nodes")
	ch, err := roachprod.Monitor(ctx, t.L(), c.name, install.MonitorOpts{OneShot: true, IgnoreEmptyNodes: true})

	// An error here means there was a problem initialising a SyncedCluster.
	if err != nil {
		return err
	}

	deadNodes := 0
	for n := range ch {
		// If there's an error, it means either that the monitor command failed
		// completely, or that it found a dead node worth complaining about.
		if n.Err != nil || strings.HasPrefix(n.Msg, "dead") {
			deadNodes++
		}

		t.L().Printf("n%d: err=%v,msg=%s", n.Node, n.Err, n.Msg)
	}

	if deadNodes > 0 {
		return errors.Newf("%d dead node(s) detected", deadNodes)
	}
	return nil
}

type HealthStatusResult struct {
	Node   int
	Status int
	Body   []byte
	Err    error
}

func newHealthStatusResult(node int, status int, body []byte, err error) *HealthStatusResult {
	return &HealthStatusResult{
		Node:   node,
		Status: status,
		Body:   body,
		Err:    err,
	}
}

// HealthStatus returns the result of the /health?ready=1 endpoint for each node.
func (c *clusterImpl) HealthStatus(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]*HealthStatusResult, error) {
	if len(node) < 1 {
		return nil, nil // unit tests
	}
	adminAddrs, err := c.ExternalAdminUIAddr(ctx, l, node)
	if err != nil {
		return nil, errors.WithDetail(err, "Unable to get admin UI address(es)")
	}
	getStatus := func(ctx context.Context, node int) *HealthStatusResult {
		url := fmt.Sprintf(`http://%s/health?ready=1`, adminAddrs[node-1])
		resp, err := httputil.Get(ctx, url)
		if err != nil {
			return newHealthStatusResult(node, 0, nil, err)
		}

		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)

		return newHealthStatusResult(node, resp.StatusCode, body, err)
	}

	results := make([]*HealthStatusResult, c.spec.NodeCount)

	_ = timeutil.RunWithTimeout(ctx, "health status", 15*time.Second, func(ctx context.Context) error {
		var wg sync.WaitGroup
		wg.Add(c.spec.NodeCount)
		for i := 1; i <= c.spec.NodeCount; i++ {
			go func(node int) {
				defer wg.Done()
				results[node-1] = getStatus(ctx, node)
			}(i)
		}
		wg.Wait()
		return nil
	})

	return results, nil
}

// assertValidDescriptors fails the test if there exists any descriptors in
// the crdb_internal.invalid_objects virtual table.
func (c *clusterImpl) assertValidDescriptors(ctx context.Context, db *gosql.DB, t *testImpl) error {
	t.L().Printf("checking for invalid descriptors")
	return timeutil.RunWithTimeout(
		ctx, "invalid descriptors check", 1*time.Minute,
		func(ctx context.Context) error {
			return roachtestutil.CheckInvalidDescriptors(ctx, db)
		},
	)
}

// assertConsistentReplicas fails the test if
// crdb_internal.check_consistency(true, ”, ”) indicates that any ranges'
// replicas are inconsistent with each other.
func (c *clusterImpl) assertConsistentReplicas(
	ctx context.Context, db *gosql.DB, t *testImpl,
) error {
	t.L().Printf("checking for replica divergence")
	return timeutil.RunWithTimeout(
		ctx, "consistency check", 5*time.Minute,
		func(ctx context.Context) error {
			return roachtestutil.CheckReplicaDivergenceOnDB(ctx, t.L(), db)
		},
	)
}

// FetchDmesg grabs the dmesg logs if possible. This requires being able to run
// `sudo dmesg` on the remote nodes.
func (c *clusterImpl) FetchDmesg(ctx context.Context, l *logger.Logger) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	l.Printf("fetching dmesg\n")
	c.status("fetching dmesg")

	// Don't hang forever.
	return timeutil.RunWithTimeout(ctx, "dmesg", 20*time.Second, func(ctx context.Context) error {
		const name = "dmesg.txt"
		path := filepath.Join(c.t.ArtifactsDir(), name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}

		// Run dmesg on all nodes to redirect the kernel ring buffer content to a file.
		cmd := []string{"/bin/bash", "-c", "'sudo dmesg -T > " + name + "'"}
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
				l.Printf("running dmesg failed on node %d: %v", result.Node, result.Err)
			} else {
				// Only run `Get` on successful nodes to avoid pseudo-failure on `Get` caused by an earlier failure on `Run`.
				successfulNodes = append(successfulNodes, int(result.Node))
			}
		}

		// Get dmesg files from successful nodes only.
		if err := c.Get(ctx, c.l, name /* src */, path /* dest */, successfulNodes); err != nil {
			l.Printf("getting dmesg files failed: %v", err)
			return errors.Wrap(err, "cluster.FetchDmesg")
		}

		// Return an error if running dmesg failed on any node.
		return combinedDmesgError
	})
}

// FetchJournalctl grabs the journalctl logs if possible. This requires being
// able to run `sudo journalctl` on the remote nodes.
func (c *clusterImpl) FetchJournalctl(ctx context.Context, l *logger.Logger) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab journalctl on local runs.
		return nil
	}

	l.Printf("fetching journalctl\n")
	c.status("fetching journalctl")

	// Don't hang forever.
	return timeutil.RunWithTimeout(ctx, "journalctl", 20*time.Second, func(ctx context.Context) error {
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
				l.Printf("running journalctl failed on node %d: %v", result.Node, result.Err)
			} else {
				// Only run `Get` on successful nodes to avoid pseudo-failure on `Get` caused by an earlier failure on `Run`.
				successfulNodes = append(successfulNodes, int(result.Node))
			}
		}

		// Get files from successful nodes only.
		if err := c.Get(ctx, c.l, name /* src */, path /* dest */, successfulNodes); err != nil {
			l.Printf("getting files failed: %v", err)
			return errors.Wrap(err, "cluster.FetchJournalctl")
		}

		// Return an error if running journalctl failed on any node.
		return combinedJournalctlError
	})
}

// FetchCores fetches any core files on the cluster.
func (c *clusterImpl) FetchCores(ctx context.Context, l *logger.Logger) error {
	if c.spec.NodeCount == 0 || c.IsLocal() {
		// No nodes can happen during unit tests and implies nothing to do.
		// Also, don't grab dmesg on local runs.
		return nil
	}

	if !c.spec.GatherCores {
		// TeamCity does not handle giant artifacts well. We'd generally profit
		// from having the cores, but we should push them straight into a temp
		// bucket on S3 instead. OTOH, the ROI of this may be low; I don't know
		// of a recent example where we've wanted the Core dumps.
		l.Printf("skipped fetching cores\n")
		return nil
	}

	l.Printf("fetching cores\n")
	c.status("fetching cores")

	// Don't hang forever. The core files can be large, so we give a generous
	// timeout.
	return timeutil.RunWithTimeout(ctx, "cores", 60*time.Second, func(ctx context.Context) error {
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

func (c *clusterImpl) ListSnapshots(
	ctx context.Context, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return roachprod.ListSnapshots(ctx, c.l, c.spec.Cloud, vslo)
}

func (c *clusterImpl) DeleteSnapshots(ctx context.Context, snapshots ...vm.VolumeSnapshot) error {
	return roachprod.DeleteSnapshots(ctx, c.l, c.spec.Cloud, snapshots...)
}

func (c *clusterImpl) CreateSnapshot(
	ctx context.Context, snapshotPrefix string,
) ([]vm.VolumeSnapshot, error) {
	return roachprod.CreateSnapshot(ctx, c.l, c.name, vm.VolumeSnapshotCreateOpts{
		Name:        snapshotPrefix,
		Description: fmt.Sprintf("snapshot for test: %s", c.t.Name()),
		Labels: map[string]string{
			vm.TagUsage: "roachtest",
		},
	})
}

func (c *clusterImpl) ApplySnapshots(ctx context.Context, snapshots []vm.VolumeSnapshot) error {
	opts := vm.VolumeCreateOpts{
		Size: c.spec.VolumeSize,
		Type: c.spec.GCEVolumeType, // TODO(irfansharif): This is only applicable to GCE. Change that.
		Labels: map[string]string{
			vm.TagUsage: "roachtest",
		},
	}
	return roachprod.ApplySnapshots(ctx, c.l, c.name, snapshots, opts)
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

// PutDefaultCockroach uploads the cockroach binary passed in the
// command line to `defaultCockroachPath` in every node in the
// cluster. This binary is used by the test runner to collect failure
// artifacts since tests are free to upload the cockroach binary they
// use to any location they desire.
func (c *clusterImpl) PutDefaultCockroach(
	ctx context.Context, l *logger.Logger, cockroachPath string,
) error {
	c.status("uploading default cockroach binary to nodes")
	return c.PutE(ctx, l, cockroachPath, defaultCockroachPath, c.All())
}

// PutLibraries inserts the specified libraries, by name, into all nodes on the cluster
// at the specified location.
func (c *clusterImpl) PutLibraries(
	ctx context.Context, libraryDir string, libraries []string,
) error {
	if len(libraries) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Put")
	}
	c.status("uploading library files")
	defer c.status("")

	if err := c.RunE(ctx, c.All(), "mkdir", "-p", libraryDir); err != nil {
		return err
	}

	for _, libraryFilePath := range libraryFilePaths[c.arch] {
		libName := libraryNameFromPath(libraryFilePath)
		if !contains(libraries, nil, libName) {
			continue
		}
		// Get the last extension (e.g., .so) to create a destination file.
		// N.B. The optional arch-specific extension is elided since the destination doesn't need it, nor does it know
		// how to resolve it. (E.g., see findLibraryDirectories in geos.go)
		ext := filepath.Ext(filepath.Base(libraryFilePath))
		putPath := filepath.Join(libraryDir, libName+ext)
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
	return errors.Wrap(roachprod.Stage(ctx, l, c.MakeNodes(opts...),
		c.os, string(c.arch), dir, application, versionOrSHA), "cluster.Stage")
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
	return errors.Wrap(roachprod.Get(ctx, l, c.MakeNodes(opts...), src, dest), "cluster.Get")
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

	temp, err := os.CreateTemp("", filepath.Base(dest))
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
			git config --global --add safe.directory %[1]s
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
	c.setStatusForClusterOpt("starting", startOpts.RoachtestOpts.Worker, opts...)
	defer c.clearStatusForClusterOpt(startOpts.RoachtestOpts.Worker)

	startOpts.RoachprodOpts.EncryptedStores = c.encAtRest

	if !envExists(settings.Env, "COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH") {
		// Panic on span use-after-Finish, so we catch such bugs.
		settings.Env = append(settings.Env, "COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH=true")
	}

	// Needed for backward-compat on crdb_internal.ranges{_no_leases}.
	// Remove in v23.2.
	if !envExists(settings.Env, "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR") {
		// This makes all roachtest use the new SHOW RANGES behavior,
		// regardless of cluster settings.
		settings.Env = append(settings.Env, "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR=false")
	}

	clusterSettingsOpts := []install.ClusterSettingOption{
		install.TagOption(settings.Tag),
		install.PGUrlCertsDirOption(settings.PGUrlCertsDir),
		install.SecureOption(settings.Secure),
		install.UseTreeDistOption(settings.UseTreeDist),
		install.EnvOption(settings.Env),
		install.NumRacksOption(settings.NumRacks),
		install.BinaryOption(settings.Binary),
		install.ClusterSettingsOption(c.clusterSettings),
		install.ClusterSettingsOption(settings.ClusterSettings),
	}

	if err := roachprod.Start(ctx, l, c.MakeNodes(opts...), startOpts.RoachprodOpts, clusterSettingsOpts...); err != nil {
		return err
	}

	// Do not refetch certs if that step already happened once (i.e., we
	// are restarting a node).
	if settings.Secure && c.localCertsDir == "" {
		if err := c.RefetchCertsFromNode(ctx, 1); err != nil {
			return err
		}
	}
	return nil
}

func (c *clusterImpl) RefetchCertsFromNode(ctx context.Context, node int) error {
	var err error
	c.localCertsDir, err = os.MkdirTemp("", "roachtest-certs")
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
	if err := c.Get(ctx, c.l, "./certs", c.localCertsDir, c.Node(node)); err != nil {
		return errors.Wrap(err, "cluster.StartE")
	}
	// Need to prevent world readable files or lib/pq will complain.
	return filepath.Walk(c.localCertsDir, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return os.Chmod(path, 0600)
	})
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

// SignalE sends a signal to the given nodes.
func (c *clusterImpl) SignalE(
	ctx context.Context, l *logger.Logger, sig int, nodes ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.Signal")
	}
	if c.spec.NodeCount == 0 {
		return nil // unit tests
	}
	return errors.Wrap(roachprod.Signal(ctx, l, c.MakeNodes(nodes...), sig), "cluster.Signal")
}

// Signal is like SignalE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Signal(
	ctx context.Context, l *logger.Logger, sig int, nodes ...option.Option,
) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.SignalE(ctx, l, sig, nodes...); err != nil {
		c.t.Fatal(err)
	}
}

// WipeE wipes a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *clusterImpl) WipeE(
	ctx context.Context, l *logger.Logger, preserveCerts bool, nodes ...option.Option,
) error {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.WipeE")
	}
	if c.spec.NodeCount == 0 {
		// For tests.
		return nil
	}
	c.setStatusForClusterOpt("wiping", false, nodes...)
	defer c.clearStatusForClusterOpt(false)
	return roachprod.Wipe(ctx, l, c.MakeNodes(nodes...), preserveCerts)
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.t.Fatal(). c.t needs to be set.
func (c *clusterImpl) Wipe(ctx context.Context, preserveCerts bool, nodes ...option.Option) {
	if ctx.Err() != nil {
		return
	}
	if err := c.WipeE(ctx, c.l, preserveCerts, nodes...); err != nil {
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
	err = execCmd(ctx, l, c.MakeNodes(node), c.IsSecure(), args...)

	l.Printf("> result: %+v", err)
	if err := ctx.Err(); err != nil {
		l.Printf("(note: incoming context was canceled: %s", err)
	}
	// We need to protect ourselves from a race where cluster logger is
	// concurrently closed before child logger is created. In that case child
	// logger will have no log file but would write to stderr instead and we can't
	// create a meaningful ".failed" file for it.
	physicalFileName := ""
	if l.File != nil {
		physicalFileName = l.File.Name()
	}
	l.Close()
	if err != nil && len(physicalFileName) > 0 {
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
	physicalFileName := ""
	if l.File != nil {
		physicalFileName = l.File.Name()
	}

	if err := ctx.Err(); err != nil {
		l.Printf("(note: incoming context was canceled: %s", err)
		return nil, err
	}

	l.Printf("running %s on nodes: %v", strings.Join(args, " "), nodes)
	if testLogger != nil {
		testLogger.Printf("> %s\n", strings.Join(args, " "))
	}

	results, err := roachprod.RunWithDetails(ctx, l, c.MakeNodes(nodes), "" /* SSHOptions */, "" /* processTag */, c.IsSecure(), args)
	if err != nil && len(physicalFileName) > 0 {
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

// cmdLogFileName comes up with a log file to use for the given argument string.
func cmdLogFileName(t time.Time, nodes option.NodeListOption, args ...string) string {
	logFile := fmt.Sprintf(
		"run_%s_n%s_%s",
		t.Format(`150405.000000000`),
		nodes.String()[1:],
		install.GenFilenameFromArgs(20, args...),
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption, external bool, tenant string,
) ([]string, error) {
	urls, err := roachprod.PgURL(ctx, l, c.MakeNodes(node), c.localCertsDir, roachprod.PGURLOptions{
		External:   external,
		Secure:     c.localCertsDir != "",
		TenantName: tenant})
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
	ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string,
) ([]string, error) {
	return c.pgURLErr(ctx, l, node, false, tenant)
}

// Silence unused warning.
var _ = (&clusterImpl{}).InternalPGUrl

// ExternalPGUrl returns the external Postgres endpoint for the specified nodes.
func (c *clusterImpl) ExternalPGUrl(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string,
) ([]string, error) {
	return c.pgURLErr(ctx, l, node, true, tenant)
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

// ExternalAdminUIAddr returns the external Admin UI address in the form host:port
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
	urls, err := c.pgURLErr(ctx, l, node, false, "")
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
	return roachprod.IP(l, c.MakeNodes(node), false)
}

// ExternalAddr returns the external address in the form host:port for the
// specified node.
func (c *clusterImpl) ExternalAddr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, l, node, true, "")
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
func (c *clusterImpl) Conn(
	ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption),
) *gosql.DB {
	db, err := c.ConnE(ctx, l, node, opts...)
	if err != nil {
		c.t.Fatal(err)
	}
	return db
}

// ConnE returns a SQL connection to the specified node.
func (c *clusterImpl) ConnE(
	ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption),
) (*gosql.DB, error) {

	connOptions := &option.ConnOption{}
	for _, opt := range opts {
		opt(connOptions)
	}
	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node), connOptions.TenantName)
	if err != nil {
		return nil, err
	}

	dataSourceName := urls[0]
	if connOptions.User != "" {
		u, err := url.Parse(urls[0])
		if err != nil {
			return nil, err
		}
		u.User = url.User(connOptions.User)
		dataSourceName = u.String()
	}
	if len(connOptions.Options) > 0 {
		vals := make(url.Values)
		for k, v := range connOptions.Options {
			vals.Add(k, v)
		}
		dataSourceName = dataSourceName + "&" + vals.Encode()
	}
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
	return config.IsLocalClusterName(c.name)
}

func (c *clusterImpl) IsSecure() bool {
	return c.localCertsDir != ""
}

func (c *clusterImpl) Architecture() vm.CPUArch {
	return c.arch
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

// NewMonitor creates a monitor that can watch for unexpected crdb node deaths on m.Wait()
// and provide roachtest safe goroutines.
//
// As a general rule, if the user has a workload node, do not monitor it. A
// monitor's semantics around handling expected node deaths breaks down if it's
// monitoring a workload node.
func (c *clusterImpl) NewMonitor(ctx context.Context, opts ...option.Option) cluster.Monitor {
	return newMonitor(ctx, c.t, c, opts...)
}

func (c *clusterImpl) StartGrafana(
	ctx context.Context, l *logger.Logger, promCfg *prometheus.Config,
) error {
	return roachprod.StartGrafana(ctx, l, c.name, c.arch, "", nil, promCfg)
}

func (c *clusterImpl) StopGrafana(ctx context.Context, l *logger.Logger, dumpDir string) error {
	return roachprod.StopGrafana(ctx, l, c.name, dumpDir)
}
