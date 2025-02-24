// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net"
	"net/http"
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

	"github.com/DataExMachina-dev/side-eye-go/sideeyeclient"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	_ "github.com/lib/pq"
	"golang.org/x/sys/unix"
)

func init() {
	_ = roachprod.InitProviders()
}

//go:embed tsdump-run.sh
var tsdumpRunSh string

var (
	// maps cpuArch to the corresponding crdb binary's absolute path
	cockroach = make(map[vm.CPUArch]string)
	// maps cpuArch to the corresponding crdb binary with runtime assertions enabled (EA)
	cockroachEA = make(map[vm.CPUArch]string)
	// maps cpuArch to the corresponding workload binary's absolute path
	workload = make(map[vm.CPUArch]string)
	// maps cpuArch to the corresponding dynamically-linked libraries' absolute paths
	libraryFilePaths = make(map[vm.CPUArch][]string)
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
		if s, err := validateBinaryFormat(name, arch, checkEA); err == nil {
			return s, nil
		}
	}
	return findBinaryOrLibrary("bin", name, "", osName, arch, checkEA)
}

func findLibrary(libraryName string, os string, arch vm.CPUArch) (string, error) {
	suffix := ".so"
	if roachtestflags.Cloud == spec.Local {
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
//	$PWD/artifacts
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
//
// Each resulting path is searched for a file named 'name', 'name.nameSuffix.archSuffix', or 'name.nameSuffix', in
// the specified order.
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
		filepath.Join(os.ExpandEnv("$PWD"), "artifacts"),
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

		for _, archSuffix := range archSuffixes {
			if path, err = exec.LookPath(filepath.Join(dir, name+archSuffix+nameSuffix)); err == nil {
				return validateBinaryFormat(path, arch, checkEA)
			}
		}
		if path, err = exec.LookPath(filepath.Join(dir, name+nameSuffix)); err == nil {
			return validateBinaryFormat(path, arch, checkEA)
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
	if roachtestflags.Cloud == spec.Local {
		defaultOSName = runtime.GOOS
		if roachtestflags.ARM64Probability == 1 {
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

	cockroachPath := roachtestflags.CockroachPath
	cockroachEAPath := roachtestflags.CockroachEAPath
	workloadPath := roachtestflags.WorkloadPath
	cockroach[defaultArch], _ = resolveBinary("cockroach", cockroachPath, defaultArch, true, false)
	// Let the test runner verify the workload binary exists if TestSpec.RequiresDeprecatedWorkload is true.
	workload[defaultArch], _ = resolveBinary("workload", workloadPath, defaultArch, false, false)
	cockroachEA[defaultArch], err = resolveBinary("cockroach-ea", cockroachEAPath, defaultArch, false, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: unable to find %q for %q: %s\n", "cockroach-ea", defaultArch, err)
	}

	if roachtestflags.ARM64Probability > 0 && defaultArch != vm.ArchARM64 {
		fmt.Printf("Locating and verifying binaries for os=%q, arch=%q\n", defaultOSName, vm.ArchARM64)
		// We need to verify we have all the required binaries for arm64.
		cockroach[vm.ArchARM64], _ = resolveBinary("cockroach", cockroachPath, vm.ArchARM64, true, false)
		workload[vm.ArchARM64], _ = resolveBinary("workload", workloadPath, vm.ArchARM64, true, false)
		cockroachEA[vm.ArchARM64], err = resolveBinary("cockroach-ea", cockroachEAPath, vm.ArchARM64, false, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: unable to find %q for %q: %s\n", "cockroach-ea", vm.ArchARM64, err)
		}
	}
	if roachtestflags.FIPSProbability > 0 && defaultArch != vm.ArchFIPS {
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
		if roachtestflags.ARM64Probability == 0 && defaultArch != vm.ArchARM64 && arch == vm.ArchARM64 {
			// arm64 isn't used, skip finding libs for it.
			continue
		}
		if roachtestflags.FIPSProbability == 0 && arch == vm.ArchFIPS {
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
	if err := c.addLabels(map[string]string{VmLabelTestRunID: runID}); err != nil && c.l != nil {
		c.l.Printf("failed to add label to cluster [%s] - %s", c.name, err)
	}
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
	if err := c.removeLabels([]string{VmLabelTestRunID}); err != nil && c.l != nil {
		c.l.Printf("failed to remove label from cluster [%s] - %s", c.name, err)
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

func makeClusterName(name string) string {
	return vm.DNSSafeName(name)
}

// MachineTypeToCPUs returns a CPU count for GCE, AWS, and Azure machine types.
// -1 is returned for unknown machine types.
func MachineTypeToCPUs(s string) int {
	{
		// GCE machine types.
		var v int
		if _, err := fmt.Sscanf(s, "n2-standard-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "t2a-standard-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n2-highcpu-%d", &v); err == nil {
			return v
		}
		if _, err := fmt.Sscanf(s, "n2-custom-%d", &v); err == nil {
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
	// Not all of Azure machine types contain the number of vCPUs in the size and
	// the sizing naming scheme is dependent on the machine type family.
	switch s {
	case "Standard_D2ds_v5", "Standard_D2pds_v5", "Standard_D2lds_v5",
		"Standard_D2plds_v5", "Standard_E2ds_v5", "Standard_E2pds_v5":
		return 2
	case "Standard_D4ds_v5", "Standard_D4pds_v5", "Standard_D4lds_v5",
		"Standard_D4plds_v5", "Standard_E4ds_v5", "Standard_E4pds_v5":
		return 4
	case "Standard_D8ds_v5", "Standard_D8pds_v5", "Standard_D8lds_v5",
		"Standard_D8plds_v5", "Standard_E8ds_v5", "Standard_E8pds_v5":
		return 8
	case "Standard_D16ds_v5", "Standard_D16pds_v5", "Standard_D16lds_v5",
		"Standard_D16plds_v5", "Standard_E16ds_v5", "Standard_E16pds_v5":
		return 16
	case "Standard_D32ds_v5", "Standard_D32pds_v5", "Standard_D32lds_v5",
		"Standard_D32plds_v5", "Standard_E32ds_v5", "Standard_E32pds_v5":
		return 32
	case "Standard_D48ds_v5", "Standard_D48pds_v5", "Standard_D48lds_v5",
		"Standard_D48plds_v5", "Standard_E48ds_v5", "Standard_E48pds_v5":
		return 48
	case "Standard_D64ds_v5", "Standard_D64pds_v5", "Standard_D64lds_v5",
		"Standard_D64plds_v5", "Standard_E64ds_v5", "Standard_E64pds_v5":
		return 64
	case "Standard_D96ds_v5", "Standard_D96pds_v5", "Standard_D96lds_v5",
		"Standard_D96plds_v5", "Standard_E96ds_v5", "Standard_E96pds_v5":
		return 96
	}
	// Unknown or unsupported machine type.
	return -1
}

type nodeSelector interface {
	option.Option
	Merge(option.NodeListOption) option.NodeListOption
}

// clusterImpl implements cluster.Cluster.

// It is safe for concurrent use by multiple goroutines.
type clusterImpl struct {
	name  string
	tag   string
	cloud spec.Cloud
	spec  spec.ClusterSpec
	t     test.Test
	f     roachtestutil.Fataler
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

	// clusterSettings are additional cluster settings set on the storage cluster startup.
	clusterSettings map[string]string
	// virtualClusterSettings are additional cluster settings to set on the
	// virtual cluster startup.
	virtualClusterSettings map[string]string
	// goCoverDir is the directory for Go coverage data (if coverage is enabled).
	// BAZEL_COVER_DIR will be set to this value when starting a node.
	goCoverDir string

	os         string     // OS of the cluster
	arch       vm.CPUArch // CPU architecture of the cluster
	randomSeed struct {
		mu   syncutil.Mutex
		seed *int64
	}

	// defaultVirtualCluster, when set, changes the default virtual
	// cluster tests connect to by default.
	defaultVirtualCluster string

	// destroyState contains state related to the cluster's destruction.
	destroyState destroyState

	// grafanaTags contains the cluster and test information that grafana will separate
	// test runs by. This is used by the roachtest grafana API to create appropriately
	// tagged grafana annotations. If empty, grafana is not available.
	grafanaTags               []string
	disableGrafanaAnnotations atomic.Bool

	// sideEyeClient, if set, is the client used to communicate with the Side-Eye
	// debugging service.
	sideEyeClient *sideeyeclient.SideEyeClient

	// State that can be accessed concurrently (in particular, read from the UI
	// HTML generator).
	mu struct {
		syncutil.Mutex
		// sideEyeEnvName is the name of the environment used by the Side-Eye agents
		// running on this cluster. Empty if the Side-Eye integration is not active.
		sideEyeEnvName string
	}
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

// sideEyeEnvName is the name of the environment used by the Side-Eye agents
// running on this cluster. Empty if the Side-Eye integration is not active.
func (c *clusterImpl) sideEyeEnvName() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.sideEyeEnvName
}

func (c *clusterImpl) setSideEyeEnvName(newEnv string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.sideEyeEnvName = newEnv
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
	// Specifies CPU architecture which may require a custom AMI and cockroach binary.
	arch vm.CPUArch
	// Specifies the OS which may require a custom AMI and cockroach binary.
	os string
	// sideEyeToken, if not empty, is the token used to authenticate with the
	// Side-Eye. If set, each node in the cluster will run the Side-Eye agent.
	// These agents are configured to allow monitoring of cockroach processes.
	// app.side-eye.io will list all currently-running clusters. The test runner's
	// Side-Eye client can be used to programmatically take snapshots of this
	// cluster using the cluster's name; snapshots are taken when the test times
	// out.
	sideEyeToken string
}

// clusterFactory is a creator of clusters.
type clusterFactory struct {
	// namePrefix is prepended to all cluster names.
	namePrefix string
	// counter is incremented with every new cluster. It's used as part of the cluster's name.
	// Accessed atomically.
	counter atomic.Uint64
	// The registry with whom all clustered will be registered.
	r *clusterRegistry
	// artifactsDir is the directory in which the cluster creation log file will be placed.
	artifactsDir string
	// sem is a semaphore throttling the creation of clusters (because AWS has
	// ridiculous API calls limits).
	sem chan struct{}
	// sideEyeClient, if set, is the client used to communicate with the Side-Eye
	// debugging service.
	sideEyeClient *sideeyeclient.SideEyeClient
}

func newClusterFactory(
	user string,
	clustersID string,
	artifactsDir string,
	r *clusterRegistry,
	concurrentCreations int,
	sideEyeClient *sideeyeclient.SideEyeClient,
) *clusterFactory {
	secs := timeutil.Now().Unix()
	var prefix string
	if clustersID != "" {
		prefix = fmt.Sprintf("%s-%s-%d-", user, clustersID, secs)
	} else {
		prefix = fmt.Sprintf("%s-%d-", user, secs)
	}
	return &clusterFactory{
		sem:           make(chan struct{}, concurrentCreations),
		namePrefix:    prefix,
		artifactsDir:  artifactsDir,
		r:             r,
		sideEyeClient: sideEyeClient,
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
	count := f.counter.Add(1)
	return makeClusterName(
		fmt.Sprintf("%s-%02d-%s", f.namePrefix, count, cfg.spec.String()))
}

// createFlagsOverride updates opts with the override values passed from the cli.
func createFlagsOverride(opts *vm.CreateOpts) {
	if roachtestflags.Changed(&roachtestflags.Lifetime) != nil {
		opts.Lifetime = roachtestflags.Lifetime
	}
	if roachtestflags.Changed(&roachtestflags.OverrideUseLocalSSD) != nil {
		opts.SSDOpts.UseLocalSSD = roachtestflags.OverrideUseLocalSSD
	}
	if roachtestflags.Changed(&roachtestflags.OverrideFilesystem) != nil {
		opts.SSDOpts.FileSystem = roachtestflags.OverrideFilesystem
	}
	if roachtestflags.Changed(&roachtestflags.OverrideNoExt4Barrier) != nil {
		opts.SSDOpts.NoExt4Barrier = roachtestflags.OverrideNoExt4Barrier
	}
	if roachtestflags.Changed(&roachtestflags.OverrideOSVolumeSizeGB) != nil {
		opts.OsVolumeSize = roachtestflags.OverrideOSVolumeSizeGB
	}
	if roachtestflags.Changed(&roachtestflags.OverrideGeoDistributed) != nil {
		opts.GeoDistributed = roachtestflags.OverrideGeoDistributed
	}
}

// clusterMock creates a cluster to be used for (self) testing.
func (f *clusterFactory) clusterMock(cfg clusterConfig) *clusterImpl {
	return &clusterImpl{
		name:       f.genName(cfg),
		expiration: timeutil.Now().Add(24 * time.Hour),
		r:          f.r,
		spec:       cfg.spec,
	}
}

// create is a hook for tests to inject their own cluster create implementation.
// i.e. unit tests that don't want to actually access a provider.
var create = roachprod.Create

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

	if roachtestflags.Changed(&roachtestflags.OverrideNumNodes) != nil {
		cfg.spec.NodeCount = roachtestflags.OverrideNumNodes
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
	workloadProviderOptsContainer := vm.CreateProviderOptionsContainer()

	clusterCloud := roachtestflags.Cloud
	params := spec.RoachprodClusterConfig{
		Cloud:                  clusterCloud,
		UseIOBarrierOnLocalSSD: cfg.useIOBarrier,
		PreferredArch:          cfg.arch,
	}
	params.Defaults.MachineType = roachtestflags.InstanceType
	params.Defaults.Zones = roachtestflags.Zones
	params.Defaults.PreferLocalSSD = roachtestflags.PreferLocalSSD

	// The ClusterName is set below in the retry loop to ensure
	// that each create attempt gets a unique cluster name.
	// N.B. selectedArch may not be the same as PreferredArch, depending on (spec.CPU, spec.Mem)
	createVMOpts, providerOpts, workloadProviderOpts, selectedArch, err := cfg.spec.RoachprodOpts(params)
	if err != nil {
		return nil, nil, err
	}

	createFlagsOverride(&createVMOpts)
	// Make sure expiration is changed if --lifetime override flag
	// is passed.
	cfg.spec.Lifetime = createVMOpts.Lifetime

	// Attempt to create a cluster several times to be able to move past
	// temporary flakiness in the cloud providers.
	maxAttempts := 3
	if cfg.nameOverride != "" {
		// Usually when retrying we pick a new name (to avoid repeat failures due to
		// partially created resources), but we were asked to use a specific
		// name. To keep things simple, disable retries in that case.
		maxAttempts = 1
	}
	// loop assumes maxAttempts is atleast (1).
	for i := 1; ; i++ {
		// NB: this intentionally avoids re-using the name across iterations in
		// the loop. See:
		//
		// https://github.com/cockroachdb/cockroach/issues/67906#issuecomment-887477675
		genName := f.genName(cfg)

		// Set the zones used for the cluster. We call this in the loop as the default GCE zone
		// is randomized to avoid zone exhaustion errors.
		providerOpts, workloadProviderOpts = cfg.spec.SetRoachprodOptsZones(providerOpts, workloadProviderOpts, params, string(selectedArch))
		if clusterCloud != spec.Local {
			providerOptsContainer.SetProviderOpts(clusterCloud.String(), providerOpts)
			workloadProviderOptsContainer.SetProviderOpts(clusterCloud.String(), workloadProviderOpts)
		}

		// Logs for creating a new cluster go to a dedicated log file.
		var retryStr string
		if i > 1 {
			retryStr = "-retry" + strconv.Itoa(i-1)
		}
		logPath := filepath.Join(f.artifactsDir, runnerLogsDir, "cluster-create", genName+retryStr+".log")
		l, err := logger.RootLogger(logPath, teeOpt)
		if err != nil {
			log.Fatalf("%v", err)
		}

		c := &clusterImpl{
			cloud:      clusterCloud,
			name:       genName,
			spec:       cfg.spec,
			expiration: cfg.spec.Expiration(),
			r:          f.r,
			arch:       selectedArch,
			os:         cfg.os,
			destroyState: destroyState{
				owned: true,
			},
			sideEyeClient: f.sideEyeClient,
			l:             l,
		}
		c.status("creating cluster")

		l.PrintfCtx(ctx, "Attempting cluster creation (attempt #%d/%d)", i, maxAttempts)
		createVMOpts.ClusterName = c.name
		opts := []*cloud.ClusterCreateOpts{{Nodes: cfg.spec.NodeCount, CreateOpts: createVMOpts, ProviderOptsContainer: providerOptsContainer}}
		// There can only be one local cluster so creating two sequentially overwrites the first.
		// There isn't a point to creating a different sized vm for local clusters, so skip it.
		if cfg.spec.WorkloadNode && !cfg.localCluster {
			opts = []*cloud.ClusterCreateOpts{
				{Nodes: cfg.spec.NodeCount - cfg.spec.WorkloadNodeCount, CreateOpts: createVMOpts, ProviderOptsContainer: providerOptsContainer},
				{Nodes: cfg.spec.WorkloadNodeCount, CreateOpts: createVMOpts, ProviderOptsContainer: workloadProviderOptsContainer},
			}
		}
		err = create(ctx, l, cfg.username, opts...)
		if err == nil {
			// Start the Side-Eye agents on all the nodes if we are configured to do so. Side-Eye
			// doesn't currently support ARM64, so skip those clusters.
			if cfg.sideEyeToken != "" && !cfg.localCluster && c.arch != vm.ArchARM64 {
				if err := c.StartSideEyeAgents(ctx, l, cfg.sideEyeToken); err != nil {
					l.Errorf("failed to start Side-Eye agents. Continuing without them.\nError: %s", err)
				}
			}
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
		if errors.HasType(err, (*roachprod.MalformedClusterNameError)(nil)) {
			return nil, nil, err
		}

		l.PrintfCtx(ctx, "cluster creation failed, cleaning up in case it was partially created: %s", err)
		c.Destroy(ctx, closeLogger, l)
		if i >= maxAttempts {
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
			if roachtestflags.ClusterWipe {
				if err := roachprod.Wipe(ctx, l, c.MakeNodes(c.All()), false /* preserveCerts */); err != nil {
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
	c.f = t
	c.l = t.L()
}

// Save marks the cluster as "saved" so that it doesn't get destroyed.
func (c *clusterImpl) Save(ctx context.Context, msg string, l *logger.Logger) {
	l.PrintfCtx(ctx, "saving cluster %s for debugging (--debug specified)", c)
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
	crdbNodes := c.spec.NodeCount - c.spec.WorkloadNodeCount
	if cpus := nodes.CPUs; cpus != 0 {
		for i, vm := range cDetails.VMs {
			nodeID := i + 1
			// If we are using a workload node, workload nodes may have a different cpu count.
			if nodeID > crdbNodes && c.spec.WorkloadNode {
				cpus = c.spec.WorkloadNodeCPUs
			}
			vmCPUs := MachineTypeToCPUs(vm.MachineType)
			// vmCPUs will be negative if the machine type is unknown. Give unknown
			// machine types the benefit of the doubt.
			if vmCPUs > 0 && vmCPUs < cpus {
				return fmt.Errorf("node %d has %d CPUs, test requires %d", nodeID, vmCPUs, cpus)
			}
			// Clouds typically don't support odd numbers of vCPUs; they can result in subtle performance issues.
			// N.B. Some machine families, e.g., n2 in GCE, do not support 1 vCPU. (See AWSMachineType and GCEMachineType.)
			if vmCPUs > 1 && vmCPUs&1 == 1 {
				return fmt.Errorf("node %d has an _odd_ number of CPUs (%d)", nodeID, vmCPUs)
			}
		}
	}
	return nil
}

func (c *clusterImpl) lister() option.NodeLister {
	fatalf := func(string, ...interface{}) {}
	if c.f != nil { // accommodates poorly set up tests
		fatalf = c.f.Fatalf
	}
	return option.NodeLister{NodeCount: c.spec.NodeCount, WorkloadNodeCount: c.spec.WorkloadNodeCount, Fatalf: fatalf}
}

func (c *clusterImpl) All() option.NodeListOption {
	return c.lister().All()
}

func (c *clusterImpl) CRDBNodes() option.NodeListOption {
	return c.lister().CRDBNodes()
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

func (c *clusterImpl) WorkloadNode() option.NodeListOption {
	return c.lister().WorkloadNode()
}

// FetchLogs downloads the logs from the cluster using `roachprod get`.
// The logs will be placed in the test's artifacts dir.
func (c *clusterImpl) FetchLogs(ctx context.Context, l *logger.Logger) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	c.status("fetching logs")

	err := roachprod.FetchLogs(ctx, l, c.name, c.t.ArtifactsDir(), 5*time.Minute)

	var logFileFull string
	if l.File != nil {
		logFileFull = l.File.Name()
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			l.Printf("(note: incoming context was canceled: %s)", err)
			return ctxErr
		}

		l.Printf("> result: %s", err)
		createFailedFile(logFileFull)
	}
	return err
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
		return c.RunE(
			ctx,
			option.WithNodes(c.All()),
			"du {store-dir} -c --exclude lost+found >> logs/diskusage.txt",
		)
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
	l.Printf("fetching timeseries data")
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
			certs := install.CockroachNodeCertsDir
			if c.IsLocal() {
				certs = c.localCertsDir
			}
			sec = fmt.Sprintf("--certs-dir=%s", certs)
		}
		if err := c.RunE(
			ctx, option.WithNodes(c.Node(node)),
			fmt.Sprintf(
				"%s debug tsdump %s --port={pgport%s:%s} --format=raw > tsdump.gob",
				test.DefaultCockroachPath, sec, c.Node(node), install.SystemInterfaceName,
			),
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
		return os.WriteFile(tsDumpGob+"-run.sh", []byte(tsdumpRunSh), 0755)
	})
}

// FetchDebugZip downloads the debug zip from the cluster using `roachprod ssh`.
// The logs will be placed at `dest`, relative to the test's artifacts dir.
//
// By default, FetchDebugZip will attempt to download the zip from the first
// node, and if that fails, it will try subsequent nodes. The caller may pass a
// list of nodes via opts if they want to target which node(s) to grab the debug
// zip from.
func (c *clusterImpl) FetchDebugZip(
	ctx context.Context, l *logger.Logger, dest string, opts ...option.Option,
) error {
	if c.spec.NodeCount == 0 {
		// No nodes can happen during unit tests and implies nothing to do.
		return nil
	}

	l.Printf("fetching debug zip")
	c.status("fetching debug zip")

	nodes := selectedNodesOrDefault(opts, c.All())

	// Don't hang forever if we can't fetch the debug zip.
	return timeutil.RunWithTimeout(ctx, "debug zip", 10*time.Minute, func(ctx context.Context) error {
		const zipName = "debug.zip"
		path := filepath.Join(c.t.ArtifactsDir(), dest)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		// Some nodes might be down, so try to find one that works. We make the
		// assumption that a down node will refuse the connection, so it won't
		// waste our time.
		for _, node := range nodes {
			pgURLOpts := roachprod.PGURLOptions{
				// `cockroach debug zip` does not support non root authentication.
				Auth: install.AuthRootCert,
				// request the system tenant specifically in case the test
				// changed the default virtual cluster.
				VirtualClusterName: install.SystemInterfaceName,
			}
			nodePgUrl, err := c.InternalPGUrl(ctx, l, c.Node(node), pgURLOpts)
			if err != nil {
				l.Printf("cluster.FetchDebugZip failed to retrieve PGUrl on node %d: %v", node, err)
				continue
			}

			// `cockroach debug zip` is noisy. Suppress the output unless it fails.
			//
			// Ignore the files in the log directory; we pull the logs separately anyway
			// so this would only cause duplication.
			excludeFiles := "*.log,*.pprof"

			cmd := roachtestutil.NewCommand("%s debug zip", test.DefaultCockroachPath).
				Option("include-range-info").
				Flag("exclude-files", fmt.Sprintf("'%s'", excludeFiles)).
				Flag("url", fmt.Sprintf("'%s'", nodePgUrl[0])).
				MaybeFlag(c.IsSecure(), "certs-dir", install.CockroachNodeCertsDir).
				Arg(zipName).
				String()
			if err = c.RunE(ctx, option.WithNodes(c.Node(node)), cmd); err != nil {
				l.Printf("%s debug zip failed on node %d: %v", test.DefaultCockroachPath, node, err)
				continue
			}
			return errors.Wrap(c.Get(ctx, c.l, zipName /* src */, path /* dest */, c.Node(node)), "cluster.FetchDebugZip")
		}
		return nil
	})
}

// FetchVMSpecs saves the VM specs for each VM in the cluster.
// The logs will be placed in the test's artifacts dir.
func (c *clusterImpl) FetchVMSpecs(ctx context.Context, l *logger.Logger) error {
	if c.IsLocal() {
		return nil
	}

	l.Printf("fetching VM specs")

	vmSpecsFolder := filepath.Join(c.t.ArtifactsDir(), "vm_specs")
	if err := os.MkdirAll(vmSpecsFolder, 0755); err != nil {
		return err
	}

	// Don't hang forever if we can't fetch the VM specs.
	return timeutil.RunWithTimeout(ctx, "fetch logs", 5*time.Minute, func(ctx context.Context) error {
		cachedCluster, err := getCachedCluster(c.name)
		if err != nil {
			return err
		}
		providerToVMs := bucketVMsByProvider(cachedCluster)

		for provider, vms := range providerToVMs {
			p := vm.Providers[provider]
			vmSpecs, err := p.GetVMSpecs(l, vms)
			if err != nil {
				l.Errorf("failed to get VM spec for provider %s: %s", provider, err)
				continue
			}
			for name, vmSpec := range vmSpecs {
				dest := filepath.Join(vmSpecsFolder, name+".json")
				specJSON, err := json.MarshalIndent(vmSpec, "", "  ")
				if err != nil {
					l.Errorf("Failed to marshal JSON: %v", err)
					continue
				}

				err = os.WriteFile(dest, specJSON, 0644)
				if err != nil {
					l.Printf("Failed to write spec to file for %s", name)
					continue
				}
			}
		}
		return nil
	})
}

func selectedNodesOrDefault(
	opts []option.Option, defaultNodes option.NodeListOption,
) option.NodeListOption {
	var nodes option.NodeListOption
	for _, o := range opts {
		if s, ok := o.(nodeSelector); ok {
			nodes = s.Merge(nodes)
		}
	}

	if len(nodes) == 0 {
		return defaultNodes
	}

	return nodes
}

type HealthStatusResult struct {
	Node   int
	URL    string
	Status int
	Body   []byte
	Err    error
}

func newHealthStatusResult(
	node int, url string, status int, body []byte, err error,
) *HealthStatusResult {
	return &HealthStatusResult{
		Node:   node,
		URL:    url,
		Status: status,
		Body:   body,
		Err:    err,
	}
}

// HealthStatus returns the result of the /health?ready=1 endpoint for the
// specified nodes.
func (c *clusterImpl) HealthStatus(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) ([]*HealthStatusResult, error) {
	nodeCount := len(nodes)
	if nodeCount < 1 {
		return nil, nil // unit tests
	}

	// Make sure we run the health checks on the KV pod.
	adminAddrs, err := c.ExternalAdminUIAddr(ctx, l, nodes, option.VirtualClusterName(install.SystemInterfaceName))
	if err != nil {
		return nil, errors.WithDetail(err, "Unable to get admin UI address(es)")
	}
	client := roachtestutil.DefaultHTTPClient(c, l)
	protocol := "http"
	if c.IsSecure() {
		protocol = "https"
	}
	getStatus := func(ctx context.Context, nodeIndex, node int) *HealthStatusResult {
		url := fmt.Sprintf(`%s://%s/health?ready=1`, protocol, adminAddrs[nodeIndex])
		resp, err := client.Get(ctx, url)
		if err != nil {
			return newHealthStatusResult(node, url, 0, nil, err)
		}

		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)

		return newHealthStatusResult(node, url, resp.StatusCode, body, err)
	}

	results := make([]*HealthStatusResult, nodeCount)

	_ = timeutil.RunWithTimeout(ctx, "health status", 15*time.Second, func(ctx context.Context) error {
		var wg sync.WaitGroup
		wg.Add(nodeCount)
		for i := 0; i < nodeCount; i++ {
			go func() {
				defer wg.Done()
				for {
					results[i] = getStatus(ctx, i, nodes[i])
					if results[i].Err == nil && results[i].Status == http.StatusOK {
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(3 * time.Second):
					}
				}
			}()
		}
		wg.Wait()
		return nil
	})

	return results, nil
}

// assertConsistentReplicas fails the test if
// crdb_internal.check_consistency(false, ”, ”) indicates that any ranges'
// replicas are inconsistent with each other.
func (c *clusterImpl) assertConsistentReplicas(
	ctx context.Context, db *gosql.DB, t *testImpl,
) error {
	t.L().Printf("checking for replica divergence")
	return timeutil.RunWithTimeout(
		ctx, "consistency check", 20*time.Minute,
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

	l.Printf("fetching dmesg")
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
		if results, combinedDmesgError = c.RunWithDetails(ctx, nil, option.WithNodes(c.All()), cmd...); combinedDmesgError != nil {
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

	l.Printf("fetching journalctl")
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
		if results, combinedJournalctlError = c.RunWithDetails(ctx, nil, option.WithNodes(c.All()), cmd...); combinedJournalctlError != nil {
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
		l.Printf("skipped fetching cores")
		return nil
	}

	l.Printf("fetching cores")
	c.status("fetching cores")

	// Don't hang forever. The core files can be large, so we give a generous
	// timeout.
	return timeutil.RunWithTimeout(ctx, "cores", 60*time.Second, func(ctx context.Context) error {
		path := filepath.Join(c.t.ArtifactsDir(), "cores")
		return errors.Wrap(c.Get(ctx, c.l, "/mnt/data1/cores" /* src */, path /* dest */), "cluster.FetchCores")
	})
}

// FetchPebbleCheckpoints fetches any Pebble checkpoints on the cluster. These
// are typically generated by failed consistency checks, i.e. replica
// divergence, and only contain data for the inconsistent ranges.
func (c *clusterImpl) FetchPebbleCheckpoints(ctx context.Context, l *logger.Logger) error {
	// Don't grab checkpoints on empty clusters.
	if c.spec.NodeCount == 0 {
		return nil
	}

	// Checkpoints can be large. Bail out after 3 minutes.
	return timeutil.RunWithTimeout(ctx, "checkpoints", 3*time.Minute, func(ctx context.Context) error {
		numStores := 1
		if ssds := c.spec.SSDs; ssds > 1 {
			numStores = ssds
		}
		for storeIdx := 1; storeIdx <= numStores; storeIdx++ {
			// Find any checkpoints.
			checkpointsPath := fmt.Sprintf("{store-dir:%d}/auxiliary/checkpoints", storeIdx)
			var checkpointNodes option.NodeListOption
			results, err := c.RunWithDetails(ctx, l, option.WithNodes(c.All()), fmt.Sprintf("test -d %s", checkpointsPath))
			if err != nil {
				return err
			}
			for _, result := range results {
				if result.Err == nil && result.RemoteExitStatus == 0 {
					l.Printf("found Pebble checkpoints in n%d:%s", result.Node, checkpointsPath)
					checkpointNodes = append(checkpointNodes, int(result.Node))
				}
			}

			// Fetch checkpoints.
			if len(checkpointNodes) > 0 {
				dest := filepath.Join(c.t.ArtifactsDir(), "checkpoints", fmt.Sprintf("data%d", storeIdx))
				if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
					return err
				}
				if err := c.Get(ctx, c.l, checkpointsPath, dest, checkpointNodes); err != nil {
					return err
				}
			}
		}
		return nil
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

	if roachtestflags.ClusterWipe {
		if c.destroyState.owned {
			l.PrintfCtx(ctx, "destroying cluster %s...", c)
			c.status("destroying cluster")
			// We use a non-cancelable context for running this command. Once we got
			// here, the cluster cannot be destroyed again, so we really want this
			// command to succeed.
			if err := roachprod.Destroy(l, "" /* optionalUsername */, false, /* destroyAllMine */
				false, /* destroyAllLocal */
				c.name); err != nil {
				l.ErrorfCtx(ctx, "error destroying cluster %s: %s", c, err)
			} else {
				l.PrintfCtx(ctx, "destroying cluster %s... done", c)
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
		l.Printf("skipping cluster wipe")
	}
	c.r.unregisterCluster(c)
	c.destroyState.mu.Lock()
	ch := c.destroyState.mu.destroyed
	close(ch)
	c.destroyState.mu.Unlock()
	return ch
}

func (c *clusterImpl) addLabels(labels map[string]string) error {
	// N.B. we must sanitize the values; e.g., some test names can exceed the maximum length (63 chars in GCE).
	// N.B. we don't sanitize the keys; unlike values, they are typically _not_ (dynamically) generated.
	return roachprod.AddLabels(c.l, c.name, vm.SanitizeLabelValues(labels))
}

func (c *clusterImpl) removeLabels(labels []string) error {
	return roachprod.RemoveLabels(c.l, c.name, labels)
}

func (c *clusterImpl) ListSnapshots(
	ctx context.Context, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	return roachprod.ListSnapshots(ctx, c.l, c.Cloud().String(), vslo)
}

func (c *clusterImpl) DeleteSnapshots(ctx context.Context, snapshots ...vm.VolumeSnapshot) error {
	return roachprod.DeleteSnapshots(ctx, c.l, c.Cloud().String(), snapshots...)
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
		Type: c.spec.GCE.VolumeType, // TODO(irfansharif): This is only applicable to GCE. Change that.
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
		c.f.Fatal(err)
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

// PutCockroach uploads a binary with or without runtime assertions enabled,
// as determined by t.Cockroach(). Note that we upload to all nodes even if they
// don't use the binary, so that the test runner can always fetch logs.
func (c *clusterImpl) PutCockroach(ctx context.Context, l *logger.Logger, t *testImpl) error {
	return c.PutE(ctx, l, t.Cockroach(), test.DefaultCockroachPath, c.All())
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

	if err := c.RunE(ctx, option.WithNodes(c.All()), "mkdir", "-p", libraryDir); err != nil {
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

// PutDeprecatedWorkload checks if the test requires the deprecated
// workload and that it has a workload node provisioned. If it does,
// then it auto-uploads the workload binary to the workload node.
// If the test requires the binary but doesn't have a workload node,
// it should handle uploading it in the test itself.
func (c *clusterImpl) PutDeprecatedWorkload(
	ctx context.Context, l *logger.Logger, t *testImpl,
) error {
	if t.spec.RequiresDeprecatedWorkload && t.spec.Cluster.WorkloadNode {
		return c.PutE(ctx, l, t.DeprecatedWorkload(), test.DefaultDeprecatedWorkloadPath, c.WorkloadNode())
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

// PutString into the specified file on the remote(s).
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
	return errors.Wrap(c.RunE(ctx, option.WithNodes(nodes), cmd...), "cluster.GitClone")
}

func (c *clusterImpl) setStatusForClusterOpt(
	operation string, worker bool, nodesOptions ...option.Option,
) {
	nodes := selectedNodesOrDefault(nodesOptions, nil)

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

func (c *clusterImpl) configureClusterSettingOptions(
	defaultClusterSettings install.ClusterSettingsOption, settings install.ClusterSettings,
) []install.ClusterSettingOption {
	setUnlessExists := func(name string, value interface{}) {
		if !envExists(settings.Env, name) {
			settings.Env = append(settings.Env, fmt.Sprintf("%s=%s", name, fmt.Sprint(value)))
		}
	}
	// Set the same seed on every node, to be used by builds with
	// runtime assertions enabled.
	setUnlessExists("COCKROACH_RANDOM_SEED", c.cockroachRandomSeed())

	// Panic on span use-after-Finish, so we catch such bugs.
	setUnlessExists("COCKROACH_CRASH_ON_SPAN_USE_AFTER_FINISH", true)

	if c.goCoverDir != "" {
		settings.Env = append(settings.Env, fmt.Sprintf("BAZEL_COVER_DIR=%s", c.goCoverDir))
	}

	return []install.ClusterSettingOption{
		install.TagOption(settings.Tag),
		install.PGUrlCertsDirOption(settings.PGUrlCertsDir),
		install.SecureOption(settings.Secure),
		install.UseTreeDistOption(settings.UseTreeDist),
		install.EnvOption(settings.Env),
		install.NumRacksOption(settings.NumRacks),
		install.BinaryOption(settings.Binary),
		defaultClusterSettings,
		install.ClusterSettingsOption(settings.ClusterSettings),
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

	// Needed for backward-compat on crdb_internal.ranges{_no_leases}.
	// Remove in v23.2.
	if !envExists(settings.Env, "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR") {
		// This makes all roachtest use the new SHOW RANGES behavior,
		// regardless of cluster settings.
		settings.Env = append(settings.Env, "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR=false")
	}

	// Make crdb_internal.check_consistency() take storage checkpoints and
	// terminate nodes on inconsistencies. This is done in post-test assertions
	// across most roachtests, see CheckReplicaDivergenceOnDB().
	if !envExists(settings.Env, "COCKROACH_INTERNAL_CHECK_CONSISTENCY_FATAL") {
		settings.Env = append(settings.Env, "COCKROACH_INTERNAL_CHECK_CONSISTENCY_FATAL=true")
	}

	if roachtestflags.ForceCpuProfile {
		settings.ClusterSettings["server.cpu_profile.duration"] = "20s"
		settings.ClusterSettings["server.cpu_profile.interval"] = "1m"
		// NB: the docs say that the profiling becomes unconditional if
		// you set the threshold to 0. This is incorrect, the database
		// has no such functionality. We set it to 1 as we expect the
		// CPU usage % to be greater than 1% either way.
		settings.ClusterSettings["server.cpu_profile.cpu_usage_combined_threshold"] = "1"
		settings.ClusterSettings["server.cpu_profile.total_dump_size_limit"] = "256 MiB"
	}

	clusterSettingsOpts := c.configureClusterSettingOptions(c.clusterSettings, settings)

	nodes := selectedNodesOrDefault(opts, c.CRDBNodes())
	if err := roachprod.Start(ctx, l, c.MakeNodes(nodes), startOpts.RoachprodOpts, clusterSettingsOpts...); err != nil {
		return err
	}

	// Do not refetch certs if that step already happened once (i.e., we
	// are restarting a node).
	if settings.Secure && c.localCertsDir == "" {
		if err := c.RefetchCertsFromNode(ctx, 1); err != nil {
			return err
		}
	}
	// N.B. If `SkipInit` is set, we don't wait for SQL since node(s) may not join the cluster in any definite time.
	if !startOpts.RoachprodOpts.SkipInit {
		// Wait for SQL to be ready on all nodes, for 'system' tenant, only.
		for _, n := range nodes {
			conn, err := c.ConnE(ctx, l, nodes[0], option.VirtualClusterName(install.SystemInterfaceName))
			if err != nil {
				return errors.Wrapf(err, "failed to connect to n%d", n)
			}
			//nolint:deferloop TODO(#137605)
			defer conn.Close()

			// N.B. We must ensure SQL session is fully initialized before attempting to execute any SQL commands.
			if err := roachtestutil.WaitForSQLReady(ctx, conn); err != nil {
				return errors.Wrap(err, "failed to wait for SQL to be ready")
			}
		}
	}

	if startOpts.WaitForReplicationFactor > 0 {
		l.Printf("WaitForReplicationFactor: waiting for replication factor of at least %d", startOpts.WaitForReplicationFactor)
		// N.B. We must explicitly pass the virtual cluster name to `ConnE`, otherwise the default may turn out to be a
		// secondary tenant, in which case we would only check the tenant's key range, not the whole system's.
		// See "Unhidden Bug" in https://github.com/cockroachdb/cockroach/issues/137988
		conn, err := c.ConnE(ctx, l, nodes[0], option.VirtualClusterName(install.SystemInterfaceName))
		if err != nil {
			return errors.Wrapf(err, "failed to connect to n%d", nodes[0])
		}
		defer conn.Close()

		if err := roachtestutil.WaitForReplication(
			ctx, l, conn, startOpts.WaitForReplicationFactor, roachtestutil.AtLeastReplicationFactor,
		); err != nil {
			return errors.Wrap(err, "failed to wait for replication after starting cockroach")
		}
	}

	return nil
}

// StartServiceForVirtualClusterE can start either external or shared
// process virtual clusters. This can be specified by the `startOpts`
// passed. See the `option.Start*VirtualClusterOpts` functions.
func (c *clusterImpl) StartServiceForVirtualClusterE(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
) error {
	l.Printf("starting virtual cluster")
	clusterSettingsOpts := c.configureClusterSettingOptions(c.virtualClusterSettings, settings)

	// By default, we assume every node in the cluster is part of the
	// storage cluster the virtual cluster needs to connect to. If the
	// user customized the storage cluster in the `StartOpts`, we use
	// that.
	storageCluster := c.CRDBNodes()
	if len(startOpts.StorageNodes) > 0 {
		storageCluster = startOpts.StorageNodes
	}

	// If the user indicated nodes where the virtual cluster should be
	// started, we indicate that in the roachprod opts.
	if len(startOpts.SeparateProcessNodes) > 0 {
		startOpts.RoachprodOpts.VirtualClusterLocation = c.MakeNodes(startOpts.SeparateProcessNodes)
	}
	if err := roachprod.StartServiceForVirtualCluster(
		ctx, l, c.MakeNodes(storageCluster), startOpts.RoachprodOpts, clusterSettingsOpts...,
	); err != nil {
		return err
	}

	if settings.Secure {
		if err := c.RefetchCertsFromNode(ctx, 1); err != nil {
			return err
		}
	}
	return nil
}

func (c *clusterImpl) StartServiceForVirtualCluster(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
) {
	if err := c.StartServiceForVirtualClusterE(ctx, l, startOpts, settings); err != nil {
		c.f.Fatal(err)
	}
}

// StopServiceForVirtualClusterE stops the service associated with the
// virtual cluster identified in the `StopOpts` passed. For shared
// process virtual clusters, the corresponding service is stopped. For
// separate process, the OS process is killed.
func (c *clusterImpl) StopServiceForVirtualClusterE(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts,
) error {
	l.Printf("stoping virtual cluster")

	nodes := c.All()
	if len(stopOpts.SeparateProcessNodes) > 0 {
		nodes = stopOpts.SeparateProcessNodes
	}

	return roachprod.StopServiceForVirtualCluster(
		ctx, l, c.MakeNodes(nodes), c.IsSecure(), stopOpts.RoachprodOpts,
	)
}

func (c *clusterImpl) StopServiceForVirtualCluster(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts,
) {
	if err := c.StopServiceForVirtualClusterE(ctx, l, stopOpts); err != nil {
		c.f.Fatal(err)
	}
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
	c.localCertsDir = filepath.Join(c.localCertsDir, install.CockroachNodeCertsDir)
	// Get the certs from the first node.
	if err := c.Get(ctx, c.l, fmt.Sprintf("./%s", install.CockroachNodeCertsDir), c.localCertsDir, c.Node(node)); err != nil {
		return errors.Wrap(err, "cluster.StartE")
	}
	// Need to prevent world readable files or lib/pq will complain.
	return filepath.WalkDir(c.localCertsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return errors.Wrap(err, "walking localCertsDir failed")
		}
		if d.IsDir() {
			return nil
		}
		return os.Chmod(path, 0600)
	})
}

func (c *clusterImpl) SetDefaultVirtualCluster(name string) {
	c.defaultVirtualCluster = name
}

// SetRandomSeed sets the random seed to be used by the cluster. If
// not called, clusters generate a random seed from the global
// generator in the `rand` package. This function must be called
// before any nodes in the cluster start.
func (c *clusterImpl) SetRandomSeed(seed int64) {
	c.randomSeed.seed = &seed
}

// cockroachRandomSeed returns the `COCKROACH_RANDOM_SEED` to be used
// by this cluster. The seed may have been previously set by a
// previous call to `StartE`, or by the user via `SetRandomSeed`. If
// not set, this function will generate a seed and return it.
func (c *clusterImpl) cockroachRandomSeed() int64 {
	c.randomSeed.mu.Lock()
	defer c.randomSeed.mu.Unlock()

	// If the user provided a seed via environment variable, always use
	// that, even if the test attempts to set a different seed.
	if c.randomSeed.seed == nil {
		seed := rand.Int63()
		c.randomSeed.seed = &seed
	}

	return *c.randomSeed.seed
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
		c.f.Fatal(err)
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

	if c.goCoverDir != "" && stopOpts.RoachprodOpts.Sig == int(unix.SIGKILL) {
		// If we are trying to collect coverage, we first send a SIGUSR1
		// which dumps coverage data and exits. Note that Cockroach v23.1
		// and earlier ignore SIGUSR1, so we still want to send SIGKILL,
		// and that's the underlying behaviour of `Stop`.
		l.Printf("coverage mode: first trying to stop using SIGUSR1")
		stopOpts.RoachprodOpts.Sig = 10 // SIGUSR1
		stopOpts.RoachprodOpts.Wait = true
		stopOpts.RoachprodOpts.GracePeriod = 10
	}
	return errors.Wrap(roachprod.Stop(ctx, l, c.MakeNodes(nodes...), stopOpts.RoachprodOpts), "cluster.StopE")
}

// Stop is like StopE, except instead of returning an error, it does
// c.f.Fatal(). c.t needs to be set.
func (c *clusterImpl) Stop(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option,
) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.StopE(ctx, l, stopOpts, opts...); err != nil {
		c.f.Fatal(err)
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
// c.f.Fatal(). c.t needs to be set.
func (c *clusterImpl) Signal(
	ctx context.Context, l *logger.Logger, sig int, nodes ...option.Option,
) {
	if c.t.Failed() {
		// If the test has failed, don't try to limp along.
		return
	}
	if err := c.SignalE(ctx, l, sig, nodes...); err != nil {
		c.f.Fatal(err)
	}
}

// WipeE wipes a subset of the nodes in a cluster. See cluster.Start() for a
// description of the nodes parameter.
func (c *clusterImpl) WipeE(
	ctx context.Context, l *logger.Logger, nodes ...option.Option,
) (retErr error) {
	if ctx.Err() != nil {
		return errors.Wrap(ctx.Err(), "cluster.WipeE")
	}
	if c.spec.NodeCount == 0 {
		// For tests.
		return nil
	}
	c.setStatusForClusterOpt("wiping", false, nodes...)
	defer c.clearStatusForClusterOpt(false)
	return roachprod.Wipe(ctx, l, c.MakeNodes(nodes...), c.IsSecure())
}

// Wipe is like WipeE, except instead of returning an error, it does
// c.f.Fatal(). c.t needs to be set.
func (c *clusterImpl) Wipe(ctx context.Context, nodes ...option.Option) {
	if ctx.Err() != nil {
		return
	}
	if err := c.WipeE(ctx, c.l, nodes...); err != nil {
		c.f.Fatal(err)
	}
}

// Run a command on the specified nodes and call test.Fatal if there is an error.
func (c *clusterImpl) Run(ctx context.Context, options install.RunOptions, args ...string) {
	err := c.RunE(ctx, options, args...)
	if err != nil {
		c.f.Fatal(err)
	}
}

// RunE runs a command on the specified node, returning an error. The output
// will be redirected to a file which is logged via the cluster-wide logger in
// case of an error. Logs will sort chronologically. Failing invocations will
// have an additional marker file with a `.failed` extension instead of `.log`.
func (c *clusterImpl) RunE(ctx context.Context, options install.RunOptions, args ...string) error {
	if len(args) == 0 {
		return errors.New("No command passed")
	}
	nodes := option.FromInstallNodes(options.Nodes)
	l, logFile, err := c.loggerForCmd(nodes, args...)
	if err != nil {
		return err
	}
	defer l.Close()

	cmd := strings.Join(args, " ")
	c.f.L().Printf("running cmd `%s` on nodes [%v]", roachprod.TruncateString(cmd, 30), nodes)
	if c.l.File != nil {
		c.f.L().Printf("details in %s.log", logFile)
	}
	l.Printf("> %s", cmd)
	expanderCfg := install.ExpanderConfig{
		DefaultVirtualCluster: c.defaultVirtualCluster,
	}
	if err := roachprod.Run(
		ctx, l, c.MakeNodes(nodes), "", "", c.IsSecure(),
		l.Stdout, l.Stderr, args, options.WithExpanderConfig(expanderCfg).WithLogExpandedCommand(),
	); err != nil {
		if err := ctx.Err(); err != nil {
			l.Printf("(note: incoming context was canceled: %s)", err)
			return err
		}

		l.Printf("> result: %s", err)
		var logFileName string
		if l.File != nil {
			logFileName = l.File.Name()
		}
		createFailedFile(logFileName)
		if c.l.File != nil {
			return errors.Wrapf(err, "full command output in %s.log", logFile)
		} else {
			return err
		}
	}
	l.Printf("> result: <ok>")
	return nil
}

// RunWithDetailsSingleNode is just like RunWithDetails but used when 1) operating
// on a single node AND 2) an error from roachprod itself would be treated the same way
// you treat an error from the command. This makes error checking easier / friendlier
// and helps us avoid code replication.
func (c *clusterImpl) RunWithDetailsSingleNode(
	ctx context.Context, testLogger *logger.Logger, options install.RunOptions, args ...string,
) (install.RunResultDetails, error) {
	nodes := option.FromInstallNodes(options.Nodes)
	if len(nodes) != 1 {
		return install.RunResultDetails{}, errors.Newf("RunWithDetailsSingleNode received %d nodes. Use RunWithDetails if you need to run on multiple nodes.", len(nodes))
	}
	results, err := c.RunWithDetails(ctx, testLogger, options, args...)
	return results[0], errors.CombineErrors(err, results[0].Err)
}

// RunWithDetails runs a command on the specified nodes (option.WithNodes),
// returning the results details and a `roachprod` error. The output will be
// redirected to a file which is logged via the cluster-wide logger in case of
// an error. Failing invocations will have an additional marker file with a
// `.failed` extension instead of `.log`.
// See install.RunOptions for more details on available options.
func (c *clusterImpl) RunWithDetails(
	ctx context.Context, testLogger *logger.Logger, options install.RunOptions, args ...string,
) ([]install.RunResultDetails, error) {
	if len(args) == 0 {
		return nil, errors.New("No command passed")
	}
	nodes := option.FromInstallNodes(options.Nodes)
	l, logFile, err := c.loggerForCmd(nodes, args...)
	if err != nil {
		return nil, err
	}
	defer l.Close()

	cmd := strings.Join(args, " ")

	// This could probably be removed in favour of c.t.L() but it's used extensively in roachtests.
	if testLogger != nil {
		testLogger.Printf("running cmd `%s` on nodes [%v]; details in %s.log", roachprod.TruncateString(cmd, 30), nodes, logFile)
	}

	l.Printf("> %s", cmd)
	expanderCfg := install.ExpanderConfig{
		DefaultVirtualCluster: c.defaultVirtualCluster,
	}
	results, err := roachprod.RunWithDetails(
		ctx, l, c.MakeNodes(nodes), "" /* SSHOptions */, "", /* processTag */
		c.IsSecure(), args, options.WithExpanderConfig(expanderCfg).WithLogExpandedCommand(),
	)

	var logFileFull string
	if l.File != nil {
		logFileFull = l.File.Name()
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			l.Printf("(note: incoming context was canceled: %s)", err)
			return nil, ctxErr
		}

		l.Printf("> result: %s", err)
		createFailedFile(logFileFull)
		return nil, err
	}

	hasError := false
	for _, result := range results {
		if result.Err != nil {
			hasError = true
			l.Printf("> result: Error for Node %d: %+v", int(result.Node), result.Err)
		}
	}
	if hasError {
		createFailedFile(logFileFull)
	} else {
		l.Printf("> result: <ok>")
	}
	return results, nil
}

func createFailedFile(logFile string) {
	if logFile == "" {
		return
	}
	if file, err := os.Create(strings.TrimSuffix(logFile, ".log") + ".failed"); err == nil {
		file.Close()
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

// pgURLErr returns the Postgres endpoint for the specified nodes. It accepts a
// flag specifying whether the URL should include the node's internal or
// external IP address. In general, inter-cluster communication and should use
// internal IPs and communication from a test driver to nodes in a cluster
// should use external IPs.
func (c *clusterImpl) pgURLErr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions,
) ([]string, error) {
	opts.Secure = c.IsSecure()

	// Use CockroachNodeCertsDir if it's an internal url with access to the node.
	certsDir := install.CockroachNodeCertsDir
	if opts.External {
		certsDir = c.localCertsDir
	}
	opts.VirtualClusterName = c.virtualCluster(opts.VirtualClusterName)
	urls, err := roachprod.PgURL(ctx, l, c.MakeNodes(nodes), certsDir, opts)
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
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions,
) ([]string, error) {
	return c.pgURLErr(ctx, l, nodes, opts)
}

// Silence unused warning.
var _ = (&clusterImpl{}).InternalPGUrl

// ExternalPGUrl returns the external Postgres endpoint for the specified nodes.
func (c *clusterImpl) ExternalPGUrl(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts roachprod.PGURLOptions,
) ([]string, error) {
	opts.External = true
	return c.pgURLErr(ctx, l, nodes, opts)
}

func addrToAdminUIAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	webPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, webPort), nil
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
// for the specified nodes.
func (c *clusterImpl) InternalAdminUIAddr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts ...option.OptionFunc,
) ([]string, error) {
	var virtualClusterOptions option.VirtualClusterOptions
	if err := option.Apply(&virtualClusterOptions, opts...); err != nil {
		return nil, err
	}

	return c.adminUIAddr(ctx, l, nodes, virtualClusterOptions, false /* external */)
}

// ExternalAdminUIAddr returns the external Admin UI address in the form host:port
// for the specified nodes.
func (c *clusterImpl) ExternalAdminUIAddr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, opts ...option.OptionFunc,
) ([]string, error) {
	var virtualClusterOptions option.VirtualClusterOptions
	if err := option.Apply(&virtualClusterOptions, opts...); err != nil {
		return nil, err
	}

	return c.adminUIAddr(ctx, l, nodes, virtualClusterOptions, true /* external */)
}

func (c *clusterImpl) SQLPorts(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	tenant string,
	sqlInstance int,
) ([]int, error) {
	return roachprod.SQLPorts(
		ctx, l, c.MakeNodes(nodes), c.IsSecure(), c.virtualCluster(tenant), sqlInstance,
	)
}

func (c *clusterImpl) AdminUIPorts(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	tenant string,
	sqlInstance int,
) ([]int, error) {
	return roachprod.AdminPorts(
		ctx, l, c.MakeNodes(nodes), c.IsSecure(), c.virtualCluster(tenant), sqlInstance,
	)
}

// virtualCluster returns the name of the virtual cluster that we
// should use when the requested `tenant` name was passed by the
// user. When a specific virtual cluster was required, we use
// it. Otherwise, we fallback to the cluster's default virtual
// cluster, if any.
func (c *clusterImpl) virtualCluster(name string) string {
	if name != "" {
		return name
	}

	return c.defaultVirtualCluster
}

func (c *clusterImpl) adminUIAddr(
	ctx context.Context,
	l *logger.Logger,
	nodes option.NodeListOption,
	opts option.VirtualClusterOptions,
	external bool,
) ([]string, error) {
	var addrs []string
	adminURLs, err := roachprod.AdminURL(
		ctx,
		l,
		c.MakeNodes(nodes),
		c.virtualCluster(opts.VirtualClusterName),
		opts.SQLInstance,
		"", /* path */
		external,
		false,
		false,
	)
	if err != nil {
		return nil, err
	}
	for _, u := range adminURLs {
		addr, err := urlToAddr(u)
		if err != nil {
			return nil, err
		}
		adminUIAddr, err := addrToAdminUIAddr(addr)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, adminUIAddr)
	}
	return addrs, nil
}

// InternalIP returns the internal IP addresses for the specified nodes.
func (c *clusterImpl) InternalIP(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) ([]string, error) {
	return roachprod.IP(l, c.MakeNodes(nodes), false)
}

// InternalAddr returns the internal address in the form host:port for the
// specified nodes.
func (c *clusterImpl) InternalAddr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) ([]string, error) {
	return c.addr(ctx, l, nodes, false)
}

// ExternalAddr returns the external address in the form host:port for the
// specified nodes.
func (c *clusterImpl) ExternalAddr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) ([]string, error) {
	return c.addr(ctx, l, nodes, true)
}

func (c *clusterImpl) addr(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, external bool,
) ([]string, error) {
	var addrs []string
	urls, err := c.pgURLErr(ctx, l, nodes, roachprod.PGURLOptions{External: external})
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

// ExternalIP returns the external IP addresses for the specified nodes.
func (c *clusterImpl) ExternalIP(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption,
) ([]string, error) {
	var ips []string
	addrs, err := c.ExternalAddr(ctx, l, nodes)
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
	ctx context.Context, l *logger.Logger, node int, opts ...option.OptionFunc,
) *gosql.DB {
	db, err := c.ConnE(ctx, l, node, opts...)
	if err != nil {
		c.f.Fatal(err)
	}
	return db
}

// ConnE returns a SQL connection to the specified node.
func (c *clusterImpl) ConnE(
	ctx context.Context, l *logger.Logger, node int, opts ...option.OptionFunc,
) (_ *gosql.DB, retErr error) {
	// NB: errors.Wrap returns nil if err is nil.
	defer func() { retErr = errors.Wrapf(retErr, "connecting to node %d", node) }()

	var connOptions option.ConnOptions
	if err := option.Apply(&connOptions, opts...); err != nil {
		return nil, err
	}

	urls, err := c.ExternalPGUrl(ctx, l, c.Node(node), roachprod.PGURLOptions{
		VirtualClusterName: connOptions.VirtualClusterName,
		SQLInstance:        connOptions.SQLInstance,
		Auth:               connOptions.AuthMode,
	})
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(urls[0])
	if err != nil {
		return nil, err
	}

	if connOptions.User != "" {
		u.User = url.User(connOptions.User)
	}

	if connOptions.DBName != "" {
		u.Path = connOptions.DBName
	}
	dataSourceName := u.String()

	vals := make(url.Values)
	for k, v := range connOptions.ConnectionOptions {
		vals.Add(k, v)
	}

	if _, ok := vals["connect_timeout"]; !ok {
		// connect_timeout is a libpq-specific parameter for the maximum
		// wait for connection, in seconds. If the caller did not specify
		// a connection timeout, we set a default.
		vals.Add("connect_timeout", "60")
	}

	dataSourceName = dataSourceName + "&" + vals.Encode()
	db, err := gosql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	// When running roachtest locally, we set a max connection lifetime
	// to avoid errors like the following:
	//
	// `read tcp 127.0.0.1:63742 -> 127.0.0.1:26257: read: connection reset by peer`
	//
	// The pq issue below seems related. This was only observed in local
	// runs so the lifetime is only applied in that context intentionally;
	// for cloud runs, we use the connection pool's default behaviour.
	//
	// https://github.com/lib/pq/issues/835
	if c.Cloud() == spec.Local {
		localConnLifetime := 10 * time.Second
		db.SetConnMaxLifetime(localConnLifetime)
	}

	return db, nil
}

func (c *clusterImpl) MakeNodes(opts ...option.Option) string {
	return c.name + selectedNodesOrDefault(opts, nil).String()
}

func (c *clusterImpl) Cloud() spec.Cloud {
	return c.cloud
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

// AddGrafanaAnnotation creates a grafana annotation for the centralized grafana instance.
func (c *clusterImpl) AddGrafanaAnnotation(
	ctx context.Context, l *logger.Logger, req grafana.AddAnnotationRequest,
) error {
	if c.disableGrafanaAnnotations.Load() {
		return nil
	}

	// If grafanaTags is empty, then grafana is not set up for this
	// cluster. We return an annotated error and stop trying to add
	// annotations in the future. This is to avoid logging the same
	// error every time the test attempts to add an annotation, which
	// could add a lot of noise to the logs.
	if len(c.grafanaTags) == 0 {
		c.disableGrafanaAnnotations.Store(true)
		return errors.New("error adding grafana annotation: grafana is not available for this cluster (disabled for the rest of the test)")
	}
	// Add grafanaTags so we can filter annotations by test or by cluster.
	req.Tags = append(req.Tags, c.grafanaTags...)

	// CentralizedGrafanaHost is the host name for the centralized grafana instance that
	// is set up for every roachtest run on GCE.
	const CentralizedGrafanaHost = "grafana.testeng.crdb.io"

	// The centralized grafana instance requires auth through Google IDP.
	return errors.Wrap(roachprod.AddGrafanaAnnotation(ctx, CentralizedGrafanaHost, true /* secure */, req), "error adding grafana annotation")
}

// AddInternalGrafanaAnnotation creates a grafana annotation for the internal grafana
// instance spun up in roachtests through StartGrafana.
func (c *clusterImpl) AddInternalGrafanaAnnotation(
	ctx context.Context, l *logger.Logger, req grafana.AddAnnotationRequest,
) error {
	host, err := roachprod.GrafanaURL(ctx, l, c.name, false /* openInBrowser */)
	if err != nil {
		return err
	}
	// The internal grafana instance does not require auth.
	return roachprod.AddGrafanaAnnotation(ctx, host, false /* secure */, req)
}

func (c *clusterImpl) WipeForReuse(
	ctx context.Context, l *logger.Logger, newClusterSpec spec.ClusterSpec,
) error {
	if c.IsLocal() {
		return errors.New("cluster reuse is disabled for local clusters to guarantee a clean slate for each test")
	}
	l.PrintfCtx(ctx, "Using existing cluster: %s (arch=%q). Wiping", c.name, c.arch)
	if err := roachprod.Wipe(ctx, l, c.MakeNodes(c.All()), false /* preserveCerts */); err != nil {
		return err
	}
	// We remove the entire shared user directory between tests to ensure we aren't
	// reusing files from previous tests, i.e. cockroach binaries, perf artifacts.
	// N.B. we don't remove the entire home directory to safeguard against this ever
	// running locally and deleting someone's local directory.
	if err := c.RunE(ctx, option.WithNodes(c.All()), fmt.Sprintf("rm -rf /home/%s/*", config.SharedUser)); err != nil {
		return errors.Wrapf(err, "failed to remove home directory")
	}
	if c.localCertsDir != "" {
		if err := os.RemoveAll(c.localCertsDir); err != nil {
			return errors.Wrapf(err,
				"failed to remove local certs in %s", c.localCertsDir)
		}
		c.localCertsDir = ""
	}

	// Reset the disableGrafanaAnnotations field so that the warning
	// about Grafana not being available is always printed at least once
	// for every test.
	c.disableGrafanaAnnotations.Store(false)

	// Clear DNS records for the cluster.
	if err := c.DestroyDNS(ctx, l); err != nil {
		return err
	}
	// Overwrite the spec of the cluster with the one coming from the test. In
	// particular, this overwrites the reuse policy to reflect what the test
	// intends to do with it.
	c.spec = newClusterSpec
	// Reset the default virtual cluster before running a new test on
	// this cluster.
	c.defaultVirtualCluster = ""

	return nil
}

// DestroyDNS destroys the DNS records for the cluster.
func (c *clusterImpl) DestroyDNS(ctx context.Context, l *logger.Logger) error {
	return roachprod.DestroyDNS(ctx, l, c.name)
}

// MaybeExtendCluster checks if the cluster has enough life left for the
// test plus enough headroom after the test finishes so that the next test
// can be selected. If it doesn't, extend it.
func (c *clusterImpl) MaybeExtendCluster(
	ctx context.Context, l *logger.Logger, testSpec *registry.TestSpec,
) error {
	timeout := testTimeout(testSpec)
	minExp := timeutil.Now().Add(timeout + time.Hour)
	if c.expiration.Before(minExp) {
		extend := minExp.Sub(c.expiration)
		l.PrintfCtx(ctx, "cluster needs to survive until %s, but has expiration: %s. Extending.",
			minExp, c.expiration)
		if err := c.Extend(ctx, extend, l); err != nil {
			return err
		}
	}
	return nil
}

// StartSideEyeAgents starts the Side-Eye agent on all the nodes in the cluster.
// These agents are configured to allow monitoring of cockroach processes.
// app.side-eye.io will list all currently-running clusters. The test runner's
// Side-Eye client can be used to programmatically take snapshots of this
// cluster using the cluster's name; snapshots are taken when the test times
// out.
func (c *clusterImpl) StartSideEyeAgents(
	ctx context.Context, l *logger.Logger, apiToken string,
) error {
	envName := c.Name()
	err := roachprod.StartSideEyeAgents(ctx, l, c.Name(), envName, apiToken)
	if err != nil {
		return err
	}
	c.setSideEyeEnvName(envName)
	return nil
}

// UpdateSideEyeEnvironmentName updates the environment name used by the
// Side-Eye agents running on this cluster.
func (c *clusterImpl) UpdateSideEyeEnvironmentName(
	ctx context.Context, l *logger.Logger, newEnvName string,
) error {
	err := roachprod.UpdateSideEyeEnvironmentName(ctx, l, c.Name(), newEnvName)
	if err != nil {
		return err
	}
	c.setSideEyeEnvName(newEnvName)
	return nil
}

// CaptureSideEyeSnapshot asks the Side-Eye service to take a snapshot of the
// cockroach processes running on this cluster. All errors are logged and
// swallowed.
//
// Returns the URL of the captured snapshot, or "" if not successful.
func (c *clusterImpl) CaptureSideEyeSnapshot(ctx context.Context) string {
	l := c.t.L()
	l.PrintfCtx(ctx, "capturing snapshot of the cluster with Side-Eye...")

	if c.arch == vm.ArchARM64 {
		l.Printf("Side-Eye does not support ARM64 machines; skipping snapshot")
		return ""
	}

	if c.sideEyeClient == nil {
		l.Printf("WARNING: Side-Eye client is not configured")
		return ""
	}

	envName := c.sideEyeEnvName()
	if envName == "" {
		l.PrintfCtx(ctx, "cluster does not have Side-Eye agents set up; skipping snapshot")
		return ""
	}

	snapURL, ok := roachprod.CaptureSideEyeSnapshot(ctx, l, envName, c.sideEyeClient)
	if !ok {
		return ""
	}
	l.PrintfCtx(ctx, "captured Side-Eye snapshot: %s", snapURL)
	annotation := fmt.Sprintf("Captured Side-Eye snapshot: %s", snapURL)
	if err := c.AddGrafanaAnnotation(ctx, l, grafana.AddAnnotationRequest{Text: annotation}); err != nil {
		l.PrintfCtx(ctx, "error adding Grafana annotation for snapshot: %s", err)
	}
	return snapURL
}

// archForTest determines the CPU architecture to use for a test. If the test
// doesn't specify it, one is chosen randomly depending on flags.
func archForTest(ctx context.Context, l *logger.Logger, testSpec registry.TestSpec) vm.CPUArch {
	if testSpec.Cluster.Arch != "" {
		l.PrintfCtx(ctx, "Using specified arch=%q, %s", testSpec.Cluster.Arch, testSpec.Name)
		return testSpec.Cluster.Arch
	}

	// CPU architecture is unspecified, choose one according to the
	// probability distribution.
	var arch vm.CPUArch
	if prng.Float64() < roachtestflags.ARM64Probability {
		arch = vm.ArchARM64
	} else if prng.Float64() < roachtestflags.FIPSProbability {
		// N.B. branch is taken with probability
		//   (1 - arm64Probability) * fipsProbability
		// which is P(fips & amd64).
		// N.B. FIPS is only supported on 'amd64' at this time.
		arch = vm.ArchFIPS
	} else {
		arch = vm.ArchAMD64
	}
	if roachtestflags.Cloud == spec.GCE && arch == vm.ArchARM64 {
		// N.B. T2A support is rather limited, both in terms of supported
		// regions and no local SSDs. Thus, we must fall back to AMD64 in
		// those cases. See #122035.
		if testSpec.Cluster.GCE.Zones != "" &&
			!gce.IsSupportedT2AZone(strings.Split(testSpec.Cluster.GCE.Zones, ",")) {
			l.PrintfCtx(ctx, "%q specified one or more GCE regions unsupported by T2A, falling back to AMD64; see #122035", testSpec.Name)
			return vm.ArchAMD64
		}
		if roachtestflags.PreferLocalSSD && testSpec.Cluster.VolumeSize == 0 && testSpec.Cluster.SSDs > 1 {
			l.PrintfCtx(ctx, "%q specified multiple _local_ SSDs unsupported by T2A, falling back to AMD64; see #122035", testSpec.Name)
			return vm.ArchAMD64
		}
	}
	l.PrintfCtx(ctx, "Using randomly chosen arch=%q, %s", arch, testSpec.Name)

	return arch
}

// bucketVMsByProvider buckets cachedCluster.VMs by provider.
func bucketVMsByProvider(cachedCluster *cloud.Cluster) map[string][]vm.VM {
	providerToVMs := make(map[string][]vm.VM)
	for _, vm := range cachedCluster.VMs {
		providerToVMs[vm.Provider] = append(providerToVMs[vm.Provider], vm)
	}
	return providerToVMs
}

// getCachedCluster checks if the passed cluster name is present in cached clusters
// and returns an error if not found.
func getCachedCluster(clusterName string) (*cloud.Cluster, error) {
	cachedCluster, ok := roachprod.CachedCluster(clusterName)
	if !ok {
		var availableClusters []string
		roachprod.CachedClusters(func(name string, _ int) {
			availableClusters = append(availableClusters, name)
		})

		err := errors.Wrapf(errClusterNotFound, "%q", clusterName)
		return nil, errors.WithHintf(err, "\nAvailable clusters:\n%s", strings.Join(availableClusters, "\n"))
	}

	return cachedCluster, nil
}

// GetPreemptedVMs gets any VMs that were part of the cluster but preempted by cloud vendor.
func (c *clusterImpl) GetPreemptedVMs(
	ctx context.Context, l *logger.Logger,
) ([]vm.PreemptedVM, error) {
	if c.IsLocal() || !c.spec.UseSpotVMs {
		return nil, nil
	}

	cachedCluster, err := getCachedCluster(c.name)
	if err != nil {
		return nil, err
	}
	providerToVMs := bucketVMsByProvider(cachedCluster)

	var allPreemptedVMs []vm.PreemptedVM
	for provider, vms := range providerToVMs {
		p := vm.Providers[provider]
		if p.SupportsSpotVMs() {
			preemptedVMS, err := p.GetPreemptedSpotVMs(l, vms, cachedCluster.CreatedAt)
			if err != nil {
				l.Errorf("failed to get preempted VMs for provider %s: %s", provider, err)
				continue
			}
			allPreemptedVMs = append(allPreemptedVMs, preemptedVMS...)
		}
	}
	return allPreemptedVMs, nil
}

// GetHostErrorVMs gets any VMs that were part of the cluster but has a host error.
func (c *clusterImpl) GetHostErrorVMs(ctx context.Context, l *logger.Logger) ([]string, error) {
	if c.IsLocal() {
		return nil, nil
	}

	cachedCluster, err := getCachedCluster(c.name)
	if err != nil {
		return nil, err
	}
	providerToVMs := bucketVMsByProvider(cachedCluster)

	var allHostErrorVMs []string
	for provider, vms := range providerToVMs {
		p := vm.Providers[provider]
		hostErrorVMS, err := p.GetHostErrorVMs(l, vms, cachedCluster.CreatedAt)
		if err != nil {
			l.Errorf("failed to get hostError VMs for provider %s: %s", provider, err)
			continue
		}
		allHostErrorVMs = append(allHostErrorVMs, hostErrorVMS...)
	}
	return allHostErrorVMs, nil
}
