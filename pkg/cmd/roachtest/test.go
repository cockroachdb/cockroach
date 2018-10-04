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
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	// For the debug http handlers.
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/petermattis/goid"
	"github.com/pkg/errors"
)

var (
	count      = 1
	postIssues = true
	gceNameRE  = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
)

// testFilter holds the name and tag filters for filtering tests.
type testFilter struct {
	name *regexp.Regexp
	tag  *regexp.Regexp
	// rawTag is the string representation of the regexps in tag
	rawTag []string
}

func newFilter(filter []string) *testFilter {
	var name []string
	var tag []string
	var rawTag []string
	for _, v := range filter {
		if strings.HasPrefix(v, "tag:") {
			tag = append(tag, strings.TrimPrefix(v, "tag:"))
			rawTag = append(rawTag, v)
		} else {
			name = append(name, v)
		}
	}

	if len(tag) == 0 {
		tag = []string{"default"}
		rawTag = []string{"tag:default"}
	}

	makeRE := func(strs []string) *regexp.Regexp {
		switch len(strs) {
		case 0:
			return regexp.MustCompile(`.`)
		case 1:
			return regexp.MustCompile(strs[0])
		default:
			for i := range strs {
				strs[i] = "(" + strs[i] + ")"
			}
			return regexp.MustCompile(strings.Join(strs, "|"))
		}
	}

	return &testFilter{
		name:   makeRE(name),
		tag:    makeRE(tag),
		rawTag: rawTag,
	}
}

type reusePolicy int

const (
	// A sentinel so that we can validate that all the tests set a policy.
	_ reusePolicy = iota
	noReuse
	onlyTagged
	any
)

type testSpec struct {
	Skip string // if non-empty, test will be skipped
	// When Skip is set, this can contain more text to be printed in the logs
	// after the "--- SKIP" line.
	SkipDetails string

	Name string
	// The maximum duration the test is allowed to run before it is considered
	// failed. If not specified, the default timeout is 10m before the test's
	// associated cluster expires. The timeout is always truncated to 10m before
	// the test's cluster expires.
	Timeout time.Duration
	// MinVersion indicates the minimum cockroach version that is required for
	// the test to be run. If MinVersion is less than the version specified
	// --cockroach-version, Skip will be populated causing the test to be
	// skipped.
	MinVersion string
	minVersion *version.Version
	// Tags is a set of tags associated with the test that allow grouping
	// tests. If no tags are specified, the set ["default"] is automatically
	// given.
	Tags []string
	// Cluster provides the specification for the cluster to use for the test.
	Cluster clusterSpec

	// UseIOBarrier controls the local-ssd-no-ext4-barrier flag passed to
	// roachprod when creating a cluster. If set, the flag is not passed, and so
	// you get durable writes. If not set (the default!), the filesystem is
	// mounted without the barrier.
	//
	// The default (false) is chosen because it the no-barrier option is needed
	// explicitly by some tests (particularly benchmarks, ironically, since they'd
	// rather measure other things than I/O) and the vast majority of other tests
	// don't care - there's no durability across machine crashes that roachtests
	// care about.
	UseIOBarrier bool

	// Run is the test function.
	Run func(ctx context.Context, t *test, c *cluster)
}

// matchOrSkip returns true if the filter matches the test. If the filter does
// not match the test because the tag filter does not match, the test is
// matched, but marked as skipped.
func (t *testSpec) matchOrSkip(filter *testFilter) bool {
	if !filter.name.MatchString(t.Name) {
		return false
	}
	if len(t.Tags) == 0 {
		if !filter.tag.MatchString("default") {
			t.Skip = fmt.Sprintf("%s does not match [default]", filter.rawTag)
		}
		return true
	}
	for _, t := range t.Tags {
		if filter.tag.MatchString(t) {
			return true
		}
	}
	t.Skip = fmt.Sprintf("%s does not match %s", filter.rawTag, t.Tags)
	return true
}

type registry struct {
	m              map[string]*testSpec
	statusInterval time.Duration
	buildVersion   *version.Version

	config struct {
		// skipClusterValidationOnAttach skips validation on existing clusters that
		// the registry uses for running tests.
		skipClusterValidationOnAttach bool
		// skipClusterStopOnAttach skips stopping existing clusters that
		// the registry uses for running tests. It implies skipClusterWipeOnAttach.
		skipClusterStopOnAttach bool
		skipClusterWipeOnAttach bool
	}

	status struct {
		syncutil.Mutex
		running map[*test]struct{}
		pass    map[*test]struct{}
		fail    map[*test]struct{}
		skip    map[*test]struct{}
	}

	// cr keeps track of all live clusters.
	cr        *clusterRegistry
	workersMu struct {
		syncutil.Mutex
		workers map[string]*workerStatus
	}
	completedTestsMu struct {
		syncutil.Mutex
		completed []completedTestInfo
	}
}

func (r *registry) recordTestFinish(info completedTestInfo) {
	r.completedTestsMu.Lock()
	r.completedTestsMu.completed = append(r.completedTestsMu.completed, info)
	r.completedTestsMu.Unlock()
}

func (r *registry) getCompletedTests() []completedTestInfo {
	r.completedTestsMu.Lock()
	defer r.completedTestsMu.Unlock()
	res := make([]completedTestInfo, len(r.completedTestsMu.completed))
	copy(res, r.completedTestsMu.completed)
	return res
}

type completedTestInfo struct {
	test    string
	run     int
	start   time.Time
	end     time.Time
	pass    bool
	failure string
}

type registryOpt func(r *registry) error

var (
	// setBuildVersion sets the build version based on the flag variable or loads
	// the version from git if the flag is not set.
	setBuildVersion registryOpt = func(r *registry) error {
		if buildTag != "" {
			return r.setBuildVersion(buildTag)
		}
		return r.loadBuildVersion()
	}
)

// newRegistry constructs a registry and configures it with opts. If any opt
// returns an error then the function will log about the error and exit the
// process with os.Exit(1).
func newRegistry(cr *clusterRegistry, opts ...registryOpt) *registry {
	r := &registry{
		m:  make(map[string]*testSpec),
		cr: cr,
	}
	r.config.skipClusterWipeOnAttach = !clusterWipe
	for _, opt := range opts {
		if err := opt(r); err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v\n", err)
			os.Exit(1)
		}
	}
	r.workersMu.workers = make(map[string]*workerStatus)

	return r
}

// runHTTPServer starts a server running in the background.
//
// httpPort: The port on which to serve the web interface. Pass 0 for allocating
// 	 a port automatically (which will be printed to stdout).
func (r *registry) runHTTPServer(httpPort int, stdout io.Writer) error {
	http.HandleFunc("/", r.serveHTTP)
	// Run an http server in the background.
	// We handle the case where httpPort is 0, which means we automatically
	// allocate a port.
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", httpPort))
	if err != nil {
		return err
	}
	httpPort = listener.Addr().(*net.TCPAddr).Port
	go func() {
		if err := http.Serve(listener, nil /* handler */); err != nil {
			panic(err)
		}
	}()
	fmt.Fprintf(stdout, "HTTP server listening on all network interfaces, port %d.\n", httpPort)
	return nil
}

func (r *registry) addWorker(ctx context.Context, name string) *workerStatus {
	r.workersMu.Lock()
	defer r.workersMu.Unlock()
	w := &workerStatus{name: name}
	if _, ok := r.workersMu.workers[name]; ok {
		log.Fatalf(ctx, "worker %q already exists", name)
	}
	r.workersMu.workers[name] = w
	return w
}

func (r *registry) removeWorker(ctx context.Context, name string) {
	r.workersMu.Lock()
	delete(r.workersMu.workers, name)
	r.workersMu.Unlock()
}

func (r *registry) setBuildVersion(buildTag string) error {
	var err error
	r.buildVersion, err = version.Parse(buildTag)
	return err
}

func (r *registry) loadBuildVersion() error {
	getLatestTag := func() (string, error) {
		cmd := exec.Command("git", "describe", "--abbrev=0", "--tags", "--match=v[0-9]*")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return "", errors.Wrapf(err, "failed to get version tag from git. Are you running in the "+
				"cockroach repo directory? err=%s, out=%s", err, out)
		}
		return strings.TrimSpace(string(out)), nil
	}
	buildTag, err := getLatestTag()
	if err != nil {
		return err
	}
	return r.setBuildVersion(buildTag)
}

// PredecessorVersion returns a recent predecessor of the build version (i.e.
// the build tag of the main binary). For example, if the running binary is from
// the master branch prior to releasing 19.2.0, this will return a recent
// (ideally though not necessarily the latest) 19.1 patch release.
func (r *registry) PredecessorVersion() (string, error) {
	if r.buildVersion == nil {
		return "", errors.Errorf("buildVersion not set")
	}

	buildVersionMajorMinor := fmt.Sprintf("%d.%d", r.buildVersion.Major(), r.buildVersion.Minor())

	verMap := map[string]string{
		"19.2": "19.1.0-rc.4",
		"19.1": "2.1.6",
		"2.2":  "2.1.6",
		"2.1":  "2.0.7",
	}
	v, ok := verMap[buildVersionMajorMinor]
	if !ok {
		return "", errors.Errorf("prev version not set for version: %s", buildVersionMajorMinor)
	}
	return v, nil
}

// verifyValidClusterName checks that a cluster name is valid for GCE.
func (r *registry) verifyValidClusterName(name string) error {
	// Both the name of the cluster, and the names of the individual nodes in the
	// cluster, must be valid identifiers in GCE when running on TeamCity. An
	// identifier can be tested using a regular expression. Also note that, due to
	// the specifics of the regular expression, we cannot assume that a valid
	// cluster name implies valid node names, or vice-versa; we therefore
	// construct both a TeamCity cluster name and a TeamCity node name and
	// validate both.

	clusterName := makeGCEClusterName(name)
	if !gceNameRE.MatchString(clusterName) {
		return fmt.Errorf(name, "invalid cluster name: %s. Doesn't match regexp: %s",
			name, gceNameRE)
	}

	// The node names are constructed using the cluster name, plus a 4 digit node
	// ID.
	teamcityNodeName := makeGCEClusterName(name + "-1234")
	if !gceNameRE.MatchString(teamcityNodeName) {
		return fmt.Errorf("invalid node name: %s. Doesn't match regexp: %s",
			teamcityNodeName, gceNameRE)
	}
	return nil
}

func (r *registry) verifyTestClusterName(testName string) error {
	return nil
}

// !!! I've made a dummy version above. Figure out what we want and why
// GCEClusterNameLimit doesn't exist any more.
//
// // verifyTestClusterName verifies that the test name can be turned into a cluster
// // name when run by TeamCity. Outside of TeamCity runs, depending on the user
// // running it and the "cluster id" component of a cluster name, the name may
// // still be invalid; however, this method is designed to catch test names
// // that will cause errors on TeamCity but not in a developer's local test
// // environment.
// func (r *registry) verifyTestClusterName(testName string) error {
//   // The name of a cluster is constructed as "[cluster ID][test name]"
//   // In TeamCity runs, the cluster ID is currently a prefix with 6 digits, but
//   // we use 7 here for a bit of breathing room.
//   name := "teamcity-1234567-" + testName
//   // Truncate the name as the test runner does.
//   if len(name) > GCEClusterNameLimit {
//     name = name[:GCEClusterNameLimit]
//     if name[len(name)-1] == '-' {
//       name = name[:len(name)-1]
//     }
//   }
//   if err := r.verifyValidClusterName(name); err != nil {
//     return errors.Wrapf(err,
//       "test name '%s' results in invalid cluster name. "+
//         "The test name may be too long or have invalid characters",
//       testName,
//     )
//   }
//   return nil
// }

func (r *registry) prepareSpec(spec *testSpec) error {
	if spec.Run == nil {
		return fmt.Errorf("%s: must specify Run", spec.Name)
	}

	if spec.Cluster.ReusePolicy == (clusterReusePolicy{}) {
		return fmt.Errorf("%s: must specify a ClusterReusePolicy", spec.Name)
	}

	if spec.MinVersion != "" {
		v, err := version.Parse(spec.MinVersion)
		if err != nil {
			return fmt.Errorf("%s: unable to parse min-version: %s", spec.Name, err)
		}
		if v.PreRelease() != "" {
			// Specifying a prerelease version as a MinVersion is too confusing
			// to be useful. The comparison is not straightforward.
			return fmt.Errorf("invalid version %s, cannot specify a prerelease (-xxx)", v)
		}
		// We append "-0" to the min-version spec so that we capture all
		// prereleases of the specified version. Otherwise, "v2.1.0" would compare
		// greater than "v2.1.0-alpha.x".
		spec.minVersion = version.MustParse(spec.MinVersion + "-0")
	}
	return r.verifyTestClusterName(spec.Name)
}

func (r *registry) Add(spec testSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	if err := r.prepareSpec(&spec); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	r.m[spec.Name] = &spec
}

// GetTests returns all the tests that match the given regexp.
// Skipped tests are included, and tests that don't match their minVersion spec
// are also included but marked as skipped.
func (r *registry) GetTests(filter *testFilter) []testSpec {
	var tests []testSpec
	for _, t := range r.m {
		if !t.matchOrSkip(filter) {
			continue
		}
		if t.Skip == "" && t.minVersion != nil {
			log.Infof(context.TODO(), "%s at least %s", r.buildVersion, t.minVersion)
			if !r.buildVersion.AtLeast(t.minVersion) {
				t.Skip = fmt.Sprintf("build-version (%s) < min-version (%s)",
					r.buildVersion, t.minVersion)
			}
		}
		tests = append(tests, *t)
	}
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name < tests[j].Name
	})
	return tests
}

// List lists tests that match one of the filters.
func (r *registry) List(filters []string) []string {
	filter := newFilter(filters)
	tests := r.GetTests(filter)
	var names []string
	for _, t := range tests {
		name := t.Name
		if t.Skip != "" {
			name += " (skipped: " + t.Skip + ")"
		}

		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type testStatus struct {
	msg      string
	time     time.Time
	progress float64
}

type test struct {
	spec     *testSpec
	registry *registry

	// l is the logger that the test will use for its output.
	l *logger

	runner   string
	runnerID int64
	start    time.Time
	end      time.Time

	// debugEnabled is a test scoped value which enables automated tests to
	// enable debugging without enabling debugging for all tests.
	// It is a bit of a hack added to help debug #34458.
	debugEnabled bool

	// artifactsDir is the path to the directory holding all the artifacts for
	// this test. It will contain a test.log file and cluster logs.
	artifactsDir string
	mu           struct {
		syncutil.RWMutex
		done   bool
		failed bool
		// cancel, if set, is called from the t.Fatal() family of functions when the
		// test is being marked as failed (i.e. when the failed field above is also
		// set). This is used to cancel the context passed to t.spec.Run(), so async
		// test goroutines can be notified.
		cancel  func()
		failLoc struct {
			file string
			line int
		}
		failureMsg string
		status     map[int64]testStatus
		output     []byte
	}
}

func (t *test) Name() string {
	return t.spec.Name
}

func (t *test) logger() *logger {
	return t.l
}

func (t *test) status(id int64, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.status == nil {
		t.mu.status = make(map[int64]testStatus)
	}
	if len(args) == 0 {
		delete(t.mu.status, id)
		return
	}
	t.mu.status[id] = testStatus{
		msg:  fmt.Sprint(args...),
		time: timeutil.Now(),
	}
}

// Status sets the main status message for the test. When called from the main
// test goroutine (i.e. the goroutine on which testSpec.Run is invoked), this
// is equivalent to calling WorkerStatus. If no arguments are specified, the
// status message is erased.
func (t *test) Status(args ...interface{}) {
	t.status(t.runnerID, args...)
}

// WorkerStatus sets the status message for a worker goroutine associated with
// the test. The status message should be cleared before the goroutine exits by
// calling WorkerStatus with no arguments.
func (t *test) WorkerStatus(args ...interface{}) {
	t.status(goid.Get(), args...)
}

func (t *test) progress(id int64, frac float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.status == nil {
		t.mu.status = make(map[int64]testStatus)
	}
	status := t.mu.status[id]
	status.progress = frac
	t.mu.status[id] = status
}

// Progress sets the progress (a fraction in the range [0,1]) associated with
// the main test status messasge. When called from the main test goroutine
// (i.e. the goroutine on which testSpec.Run is invoked), this is equivalent to
// calling WorkerProgress.
func (t *test) Progress(frac float64) {
	t.progress(t.runnerID, frac)
}

// WorkerProgress sets the progress (a fraction in the range [0,1]) associated
// with the a worker status messasge.
func (t *test) WorkerProgress(frac float64) {
	t.progress(goid.Get(), frac)
}

// Skip records msg into t.spec.Skip and calls runtime.Goexit() - thus
// interrupting the running of the test.
func (t *test) Skip(msg string, details string) {
	t.spec.Skip = msg
	t.spec.SkipDetails = details
	runtime.Goexit()
}

// Fatal marks the test as failed, prints the args to t.l, and calls
// runtime.GoExit(). It can be called multiple times.
func (t *test) Fatal(args ...interface{}) {
	t.fatalfInner("" /* format */, args...)
}

func (t *test) Fatalf(format string, args ...interface{}) {
	t.fatalfInner(format, args...)
}

func (t *test) fatalfInner(format string, args ...interface{}) {
	// Skip two frames: our own and the caller.
	if format != "" {
		t.printfAndFail(2 /* skip */, format, args...)
	} else {
		t.printAndFail(2 /* skip */, args...)
	}
	runtime.Goexit()
}

// FatalIfErr calls t.Fatal() if err != nil.
func FatalIfErr(t *test, err error) {
	if err != nil {
		t.fatalfInner("" /* format */, err)
	}
}

func (t *test) printAndFail(skip int, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	msg := fmt.Sprint(args...)
	t.l.PrintfCtxDepth(context.TODO(), skip+2, "test failure: %s", msg)
	t.mu.output = append(t.mu.output, t.decorate(skip+1, msg)...)
	t.mu.failed = true
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

func (t *test) printfAndFail(skip int, format string, args ...interface{}) {
	msg := t.decorate(skip+1, fmt.Sprintf(format, args...))

	t.mu.Lock()
	defer t.mu.Unlock()

	prefix := ""
	if t.mu.failed {
		prefix = "[not the first failure] "
	}
	t.l.Printf("%stest failure: %s", prefix, msg)

	if t.mu.failed {
		return
	}

	t.mu.failed = true
	t.mu.failureMsg = msg
	t.mu.output = append(t.mu.output, msg...)
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

// Args:
// skip: The number of stack frames to exclude from the result. 0 means that
//   the caller will be the first frame identified. 1 means the caller's caller
//   will be the first, etc.
func (t *test) decorate(skip int, s string) string {
	// Skip two extra frames to account for this function and runtime.Callers
	// itself.
	var pc [50]uintptr
	n := runtime.Callers(2+skip, pc[:])
	if n == 0 {
		panic("zero callers found")
	}

	buf := new(bytes.Buffer)
	frames := runtime.CallersFrames(pc[:n])
	sep := "\t"
	runnerFound := false
	for {
		if runnerFound {
			break
		}

		frame, more := frames.Next()
		if !more {
			break
		}
		if frame.Function == t.runner {
			runnerFound = true

			// Handle the special case of the runner function being the caller of
			// t.Fatal(). In that case, that's the line to be used for issue creation.
			if t.mu.failLoc.file == "" {
				t.mu.failLoc.file = frame.File
				t.mu.failLoc.line = frame.Line
			}
		}
		if !t.mu.failed && !runnerFound {
			// Keep track of the highest stack frame that is lower than the t.runner
			// stack frame. This is used to determine the author of that line of code
			// and issue assignment.
			t.mu.failLoc.file = frame.File
			t.mu.failLoc.line = frame.Line
		}
		file := frame.File
		if index := strings.LastIndexByte(file, '/'); index >= 0 {
			file = file[index+1:]
		}
		fmt.Fprintf(buf, "%s%s:%d", sep, file, frame.Line)
		sep = ","
	}
	buf.WriteString(": ")

	lines := strings.Split(s, "\n")
	if l := len(lines); l > 1 && lines[l-1] == "" {
		lines = lines[:l-1]
	}
	for i, line := range lines {
		if i > 0 {
			buf.WriteString("\n\t\t")
		}
		buf.WriteString(line)
	}
	buf.WriteByte('\n')
	return buf.String()
}

func (t *test) duration() time.Duration {
	return t.end.Sub(t.start)
}

func (t *test) Failed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failed
}

func (t *test) FailureMsg() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failureMsg
}

func (t *test) MarkFailed() {
	t.mu.Lock()
	t.mu.failed = true
	t.mu.Unlock()
}

func (t *test) ArtifactsDir() string {
	return t.artifactsDir
}

// IsBuildVersion returns true if the build version is greater than or equal to
// minVersion. This allows a test to optionally perform additional checks
// depending on the cockroach version it is running against. Note that the
// versions are Cockroach build tag version numbers, not the internal cluster
// version number.
func (t *test) IsBuildVersion(minVersion string) bool {
	vers, err := version.Parse(minVersion)
	if err != nil {
		t.Fatal(err)
	}
	if p := vers.PreRelease(); p != "" {
		panic("cannot specify a prerelease: " + p)
	}
	// We append "-0" to the min-version spec so that we capture all
	// prereleases of the specified version. Otherwise, "v2.1.0" would compare
	// greater than "v2.1.0-alpha.x".
	vers = version.MustParse(minVersion + "-0")
	return t.registry.buildVersion.AtLeast(vers)
}

// errTestFailureAndDebugEnabled represents a test failure when --debug was
// specified.
type errTestFailureAndDebugEnabled struct {
	test        string
	c           *cluster
	testFailure string
}

var _ = (*test)(nil).IsBuildVersion // avoid unused lint

func newErrTestFailureAndDebugEnabled(test string, c *cluster, failure string) error {
	return errTestFailureAndDebugEnabled{test: test, c: c, testFailure: failure}
}

// Error implements the error interface.
func (e errTestFailureAndDebugEnabled) Error() string {
	return fmt.Sprintf(
		"%s: test failure and debug enabled. Leaving cluster around for debugging: %s. Failure: %s",
		e.test, e.c.name, e.testFailure)
}

// teamCityEscape escapes a string for use as <value> in a key='<value>' attribute
// in TeamCity build output marker.
// Documentation here: https://confluence.jetbrains.com/display/TCD10/Build+Script+Interaction+with+TeamCity#BuildScriptInteractionwithTeamCity-Escapedvalues
func teamCityEscape(s string) string {
	r := strings.NewReplacer(
		"\n", "|n",
		"'", "|'",
		"|", "||",
		"[", "|[",
		"]", "|]",
	)
	return r.Replace(s)
}

func teamCityNameEscape(name string) string {
	return strings.Replace(name, ",", "_", -1)
}

// getAuthorEmail retrieves the author of a line of code. Returns the empty
// string if the author cannot be determined.
func getAuthorEmail(file string, line int) string {
	const repo = "github.com/cockroachdb/cockroach/"
	i := strings.Index(file, repo)
	if i == -1 {
		return ""
	}
	file = file[i+len(repo):]

	cmd := exec.Command(`/bin/bash`, `-c`,
		fmt.Sprintf(`git blame --porcelain -L%d,+1 $(git rev-parse --show-toplevel)/%s | grep author-mail`,
			line, file))
	// This command returns output such as:
	// author-mail <jordan@cockroachlabs.com>
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	re := regexp.MustCompile("author-mail <(.*)>")
	matches := re.FindSubmatch(out)
	if matches == nil {
		return ""
	}
	return string(matches[1])
}

type workerErrors struct {
	mu struct {
		syncutil.Mutex
		errs []error
	}
}

func (we *workerErrors) AddErr(err error) {
	we.mu.Lock()
	defer we.mu.Unlock()
	we.mu.errs = append(we.mu.errs, err)
}

func (we *workerErrors) Err() error {
	we.mu.Lock()
	defer we.mu.Unlock()
	if len(we.mu.errs) == 0 {
		return nil
	}
	// TODO(andrei): Maybe we should do something other than return the first
	// error...
	return we.mu.errs[0]
}

type testWithCount struct {
	spec testSpec
	// count maintains the number of runs remaining for a test.
	count int
}

type workPool struct {
	// count is the total number of times each test has to run. It is constant.
	// Not to be confused with the count inside mu.tests, which tracks remaining
	// runs.
	count int
	mu    struct {
		syncutil.Mutex
		// tests with remaining run count.
		tests []testWithCount
		// clusters keeps track of how many tagged clusters there are, for each tag.
		clusters taggedClusters
	}
}

func newWorkPool(tests []testSpec, count int) *workPool {
	p := &workPool{count: count}
	p.mu.clusters = make(taggedClusters)
	for _, spec := range tests {
		p.mu.tests = append(p.mu.tests, testWithCount{spec: spec, count: count})
	}
	return p
}

// registerTestStartLocked decrements a test's remaining count and removes it
// from the workPool if it was exhausted. It also adjusts the bookkeeping
// related to tagged clusters inside the workPool.
func (p *workPool) registerTestStartLocked(
	name string, existingClusterTag string, clusterReuse bool,
) {
	idx := -1
	for idx = range p.mu.tests {
		if p.mu.tests[idx].spec.Name == name {
			break
		}
	}
	if idx == -1 {
		log.Fatalf(context.TODO(), "failed to find test: %s", name)
	}
	tc := &p.mu.tests[idx]
	tc.count--
	spec := tc.spec
	if tc.count == 0 {
		// We've selected the last run for a test. Take that test out of the pool.
		p.mu.tests = append(p.mu.tests[:idx], p.mu.tests[idx+1:]...)
	}
	// Adjust the tag clusters bookkeeping.
	if clusterReuse {
		if existingClusterTag == "" && spec.Cluster.ReusePolicy.policy == onlyTagged {
			// We had an untagged cluster and we're about to tag it.
			tag := spec.Cluster.ReusePolicy.tag
			if _, ok := p.mu.clusters[tag]; ok {
				p.mu.clusters[tag]++
			} else {
				p.mu.clusters[tag] = 1
			}
		} else if existingClusterTag != "" && spec.Cluster.ReusePolicy == NoReuse {
			// We had a tagged cluster and we're about to destroy it after running this
			// test.
			p.mu.clusters[existingClusterTag]--
		}
	} else {
		if existingClusterTag != "" {
			// We had a tagged cluster and we're about to destroy it before running this
			// test.
			p.mu.clusters[existingClusterTag]--
		}
		tag := spec.Cluster.ReusePolicy.tag
		if tag != "" {
			// We're about to create a new tagged cluster.
			if _, ok := p.mu.clusters[tag]; ok {
				p.mu.clusters[tag]++
			} else {
				p.mu.clusters[tag] = 1
			}
		}
	}
}

// findCompatibleTestsLocked returns a list of tests compatible with a cluster spec
// and, if tag is not empty, with a tagged cluster.
func (p *workPool) findCompatibleTestsLocked(clusterSpec clusterSpec, tag string) []testWithCount {
	var tests []testWithCount
	for _, tc := range p.mu.tests {
		if clustersCompatible(clusterSpec, tc.spec.Cluster) {
			tests = append(tests, tc)
		}
	}
	return tests
}

func clustersCompatible(s1, s2 clusterSpec) bool {
	return s1 == s2
}

type testToRunRes struct {
	// noWork is set if the work pool was empty and thus no test was selected.
	noWork bool
	// spec is the selected test.
	spec testSpec
	// runNum is run number. 1 if --count was not used.
	runNum int
	// canReuseCluster is true if the selected test can reuse the cluster passed
	// to testToRun(). Will be false if noWork is set.
	canReuseCluster bool

	tc testCommitter
}

// confirm means that the test represented by the receiver will be run. The
// respective workPool will update its bookkeeping accordingly.
func (t *testToRunRes) confirm() {
	t.tc.commitLocked()
}

// confirm means that the test represented by the receiver will not be run.
// rollback is a no-op if called after a previous confirm/rollback.
func (t *testToRunRes) rollback() {
	t.tc.rollbackLocked()
}

// testCommitter is used to specify if a test selected from a workPool will be
// run or not, and unlock the pool at that point.
type testCommitter struct {
	p            *workPool
	spec         testSpec
	clusterReuse bool
	tag          string
}

// commitLocked make the workPool account for a test run. The caller is saying
// that it will indeed run the test that was previously returned by the
// workPool.
func (t *testCommitter) commitLocked() {
	t.p.registerTestStartLocked(t.spec.Name, t.tag, t.clusterReuse)
	t.p.mu.Unlock()
	t.p = nil
}

// rollbackLocked is supposed to be called if a testToRunRes that was previously
// obtained from a workPool will not be "used" - so the caller is saying that
// it's not going to run the respective test.
func (t *testCommitter) rollbackLocked() {
	if t.p == nil {
		return
	}
	t.p.mu.Unlock()
	t.p = nil
}

// testToRun selects the best test to run.
// The workPool's bookkeeping is not automatically updated to reflect that the
// selected test will be run; the caller is under no obligation to run it (in
// fact, the caller is expected to first check that there are enough resources
// to run it). Instead, the workPool is kept in a locked state and:
// ATTENTION: If a test is returned (i.e. testToRunRes.noWork is not set), the
// caller need to call commit() or rollback() on the returned testToRunRes.
//
// Args:
// spec: The spec of an existing cluster that can be reused. clusterSpec{} if no
//   such cluster.
// tag: The tag of the existing cluster. Empty if not such cluster or the
//   cluster isn't tagged.
func (p *workPool) testToRun(ctx context.Context, spec clusterSpec, tag string) testToRunRes {
	if tag != "" && (spec == clusterSpec{}) {
		log.Fatalf(ctx, "can't specify a tag (%q) if a spec is not passed in", tag)
	}

	// Short-circuit if there's no more work.
	p.mu.Lock()
	if len(p.mu.tests) == 0 {
		p.mu.Unlock()
		return testToRunRes{noWork: true}
	}
	p.mu.Unlock()

	if spec == (clusterSpec{}) {
		// If we don't have a cluster, we pick the test with the highest number of
		// runs left.
		// TODO(andrei): We could be smarter in guessing what kind of cluster is
		// best to allocate.
		var candidateIdx int
		candidateCount := 0
		// Responsibility for unlocking is passed to the caller through the returned
		// testToRunRes.
		p.mu.Lock()
		for i, t := range p.mu.tests {
			if t.count > candidateCount {
				candidateIdx = i
				candidateCount = t.count
			}
		}
		tc := p.mu.tests[candidateIdx]
		runNum := p.count - tc.count + 1
		return testToRunRes{
			spec:            tc.spec,
			runNum:          runNum,
			canReuseCluster: false,
			tc:              testCommitter{p: p, spec: tc.spec, clusterReuse: false},
		}
	}

	// We've been given a cluster and we need to choose the best test to run on
	// this cluster. If no test matches the cluster, we're going to call ourselves
	// recursively, but this time by not specifying the cluster any more (and so
	// the cluster will be thrown away).
	// Among clusters that match the spec, we do the following:
	// - If the cluster is already tagged, we only look at tests with the same
	// tag.
	// - Otherwise, we'll choose in the following order of preference:
	// 1) tests that leave the cluster usable by anybody afterwards
	// 2) tests that leave the cluster usable by some other tests
	// 	2.1) within this OnlyTagged<foo> category, we'll prefer the tag with the
	// 			 fewest existing clusters.
	// 3) tests that leave the cluster unusable by anybody
	//
	// Within each of the categories, we'll give preference to tests with fewer
	// runs.

	p.mu.Lock()
	// We're only looking at tests that match the spec and the tag (if our cluster
	// has a tag).
	testsWithCounts := p.findCompatibleTestsLocked(spec, tag)
	if len(testsWithCounts) == 0 {
		p.mu.Unlock()
		// Throw away the cluster and look again.
		return p.testToRun(ctx, clusterSpec{}, "" /* tag */)
	}

	// NOTE: Responsibility for unlocking p.mu is passed to the caller through the
	// returned testToRunRes.

	candidateScore := 0
	var candidate testWithCount
	for _, tc := range testsWithCounts {
		score := scoreTestAgainstCluster(tc, tag, p.mu.clusters)
		if score > candidateScore {
			candidateScore = score
			candidate = tc
		}
	}
	runNum := p.count - candidate.count + 1
	return testToRunRes{
		spec:            candidate.spec,
		runNum:          runNum,
		canReuseCluster: true,
		tc:              testCommitter{p: p, spec: candidate.spec, clusterReuse: true, tag: tag},
	}
}

type taggedClusters map[string]int

func scoreTestAgainstCluster(tc testWithCount, tag string, clusters taggedClusters) int {
	t := tc.spec
	if tag != "" && t.Cluster.ReusePolicy != OnlyTagged(tag) {
		log.Fatalf(context.TODO(),
			"incompatible test and cluster. Cluster tag: %s. Test policy: %+v",
			tag, t.Cluster.ReusePolicy)
	}
	score := 0
	if t.Cluster.ReusePolicy == Any {
		score = 1000000
	} else if t.Cluster.ReusePolicy.policy == onlyTagged {
		score = 500000
		if tag == "" {
			// We have an untagged cluster and a tagged test. Within this category of
			// tests, we prefer the tags with the fewest existing clusters.
			score -= 1000 * clusters[t.Cluster.ReusePolicy.tag]
		}
	} else { // NoReuse policy
		score = 0
	}

	// We prefer tests that have run fewer times (so, that have more runs left).
	score += tc.count

	return score
}

type clusterType int

const (
	localCluster clusterType = iota
	roachprodCluster
)

// clustersOpt groups option for the clusters to use by tests.
type clustersOpt struct {
	// The type of cluster to use. If localCluster, then no other fields can be
	// set.
	typ clusterType

	// If set, all the tests will run against this roachprod cluster.
	clusterName string
	// If set, all the clusters will use this ID as part of their name. When
	// roachtests is invoked by TeamCity, this will be the build id.
	clusterID string

	// cpuQuota specifies how many CPUs can be used concurrently by the roachprod
	// clusters. While there's no quota available for creating a new cluster, the
	// test runner will wait for other tests to finish and their cluster to be
	// destroyed (or reused). Note that this limit is global, not per zone.
	cpuQuota int
	// If not 0, this value will be used to override testSpec.Nodes[0].Lifetime
	// for all tests. Useful, for example, when running a large number of tests
	// that can reuse clusters and the default 12h destruction time for those
	// clusters is not desirable.
	// !!! do something else instead of this param
	lifetimeOverride time.Duration
	// If set, clusters will not be wiped or destroyed when a test using the
	// respective cluster fails. These cluster will linger around and they'll
	// continue counting towards the cpuQuota.
	keepClustersOnTestFailure bool
}

func (c clustersOpt) validate() error {
	if c.typ == localCluster {
		// No other field can be set.
		if c != (clustersOpt{typ: localCluster}) {
			return fmt.Errorf("local cluster incompatible with spec: %+v", c)
		}
	}
	return nil
}

type loggingOpt struct {
	// l is the test runner logger.
	// Note that individual test runs will use a different logger.
	l *logger
	// tee controls whether test logs (not test runner logs) also go to stdout or
	// not.
	tee            teeOptType
	stdout, stderr io.Writer
	artifactsDir   string
}

// Run runs tests.
//
// Args:
// tests: The tests to run.
// count: How many times to run each test selected by filter.
// parallelism: How many workers to use for running tests. Tests are run
//   locally (although generally they run against remote roachprod clusters).
//   parallelism bounds the maximum number of tests that run concurrently. Note
//   that the concurrency is also affected by cpuQuota.
// clusterOpt: Options for the clusters to use by tests.
// lopt: Options for logging.
func (r *registry) Run(
	ctx context.Context,
	tests []testSpec,
	count int,
	parallelism int,
	clustersOpt clustersOpt,
	artifactsDir string,
	user string,
	lopt loggingOpt,
) error {
	// Validate options.
	if err := clustersOpt.validate(); err != nil {
		return err
	}
	if parallelism != 1 {
		if clustersOpt.clusterName != "" {
			return fmt.Errorf("--cluster incompatible with --parallelism. Use --parallelism=1.")
		}
		if clustersOpt.typ == localCluster {
			return fmt.Errorf("--local incompatible with --parallelism. Use --parallelism=1.")
		}
	}

	// Seed the default rand source so that different runs get different cluster
	// IDs.
	rand.Seed(timeutil.Now().UnixNano())

	if clustersOpt.lifetimeOverride != 0 {
		for _, t := range tests {
			t.Cluster.Lifetime = clustersOpt.lifetimeOverride
		}
	}

	n := len(tests)
	if n == 0 {
		return fmt.Errorf("no test matched filters")
	}
	if n*count < parallelism {
		// Don't spin up more workers than necessary.
		parallelism = n * count
	}

	r.status.running = make(map[*test]struct{})
	r.status.pass = make(map[*test]struct{})
	r.status.fail = make(map[*test]struct{})
	r.status.skip = make(map[*test]struct{})

	work := newWorkPool(tests, count)
	stopper := stop.NewStopper()
	errs := &workerErrors{}

	var rg *resourceGovernor
	if !local && clusterName == "" {
		rg = newResourceGovernor(clustersOpt.cpuQuota)
		// Close rg when the context is closed. This will unblock workers blocked on
		// waiting for resources (which waiting doesn't otherwise react to any ctx
		// cancelation).
		go func() {
			<-ctx.Done()
			rg.Close("context canceled")
		}()

	}

	l := lopt.l

	var numConcurrentClusterCreations int
	if cloud == "aws" {
		// AWS has ridiculous API calls limits, so we're going to create one cluster
		// at a time. Internally, roachprod has throttling for the calls required to
		// create a single cluster.
		numConcurrentClusterCreations = 1
	} else {
		numConcurrentClusterCreations = 1000
	}
	clusterFactory := newClusterFactory(
		numConcurrentClusterCreations, user, clustersOpt.clusterID, artifactsDir, r.cr)
	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		i := i // Copy for closure.
		wg.Add(1)
		stopper.RunWorker(ctx, func(ctx context.Context) {
			if err := r.runWorker(
				ctx, fmt.Sprintf("w%d", i) /* name */, work, rg, clusterFactory,
				clusterName, local,
				stopper.ShouldQuiesce(),
				clustersOpt.keepClustersOnTestFailure,
				lopt.artifactsDir, user, lopt.tee, lopt.stdout, l,
			); err != nil {
				// A worker returned an error. We don't want to run more tests since
				// something funky must have happened and the state of our cloud
				// resources might be arbitrarily bad (e.g. we might have leaked quota).
				shout(ctx, l, lopt.stdout, "Worker %d returned with error. Quiescing. Error: %s", i, err)
				errs.AddErr(err)
				// Quiesce the stopper. This will cause all workers to not pick up more
				// tests after finishing the currently running one.
				stopper.Quiesce(ctx)
				// Interrupt everybody waiting for resources.
				if rg != nil {
					rg.Close("a worker errored; not running more tests")
				}
			}
			wg.Done()
		})
	}

	// Wait for all the workers to finish. They individually are supposed to react
	// to context cancelation.
	wg.Wait()
	r.cr.destroyAllClusters(ctx, l)

	if errs.Err() != nil {
		shout(ctx, l, lopt.stdout, "FAIL (err: %s)", errs.Err())
		return errs.Err()
	}
	// !!! this can produce an empty line; figure out how
	report, success, err := r.generateReport()
	if err != nil {
		log.Fatal(ctx, err)
	}
	shout(ctx, l, lopt.stdout, report)
	if success {
		return nil
	}
	return fmt.Errorf("some tests failed")
}

type workerStatus struct {
	// name is the worker's identifier.
	name string
	mu   struct {
		syncutil.Mutex

		// status is presented in the HTML progress page.
		status string

		t testToRunRes
		c *cluster
	}
}

func (w *workerStatus) Status() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.status
}

func (w *workerStatus) SetStatus(status string) {
	w.mu.Lock()
	w.mu.status = status
	w.mu.Unlock()
}

func (w *workerStatus) Cluster() *cluster {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.c
}

func (w *workerStatus) SetCluster(c *cluster) {
	w.mu.Lock()
	w.mu.c = c
	w.mu.Unlock()
}

func (w *workerStatus) Test() testToRunRes {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.t
}

func (w *workerStatus) SetTest(t testToRunRes) {
	w.mu.Lock()
	w.mu.t = t
	w.mu.Unlock()
}

// runWorker runs tests in a loop until work is exhausted.
//
// Errors are returned in exceptional circumstances. If an error is returned,
// resources might have leaked out of the resourceGovernor. As such, higher
// layers are expected to not try to allocate resources any more and terminate.
// Upon return, the clusters used by this worker will have been destroyed, even
// in the error case (unless errTestFailureAndDebugEnabled is returned).
// !!! ^ is not true; we leak clusters that are in the process of being created.
// Figure something out.
//
// Args:
// name: The worker's name, to be used as a prefix for log messages.
// rg: The resourceGovernor that will be asked for resources every time a new
//   cluster needs to be created. If nil, no resourceGovernor is consulted. Nil
//   has to be passed when clusterName is passed.
// clusterName: If not empty, the name of a cluster to use for all tests. This
//   worker is presumed to have exclusive access to the cluster, since it will
//   wipe it every after test. If the cluster is not compatible with any test,
//   an error will be returned.
// artifactsDir: The artifacts dir. Each test's logs are going to be under a
//   run_<n> dir. If empty, test log files will not be created.
// stdout: The Writer to use for messages that need to go to stdout (e.g. the
// 	 "=== RUN" and "--- FAIL" lines).
// teeOpt: The teeing option for future test loggers.
// l: The logger to use for more verbose messages.
func (r *registry) runWorker(
	ctx context.Context,
	name string,
	work *workPool,
	rg *resourceGovernor,
	clusterFactory *clusterFactory,
	clusterName string,
	local bool,
	interrupt <-chan struct{},
	debug bool,
	artifactsDir string,
	user string,
	teeOpt teeOptType,
	stdout io.Writer,
	l *logger,
) (retErr error) {
	ctx = logtags.AddTag(ctx, name, nil /* value */)
	wStatus := r.addWorker(ctx, name)
	defer func() {
		r.removeWorker(ctx, name)
	}()

	var c *cluster     // The cluster currently being used.
	var cs clusterSpec // The spec of the current cluster.
	var tag string     // The tag of the current cluster.
	// Set if the current cluster needs to be destroyed after the current test.
	var needDestroy bool

	allocateCluster := func(
		local bool,
		existingClusterName string,
		artifactsDir string,
		t testSpec,
		alloc resourceAllocation,
	) (*cluster, error) {
		wStatus.SetStatus("creating cluster")
		defer wStatus.SetStatus("")

		if existingClusterName != "" {
			// Logs for attaching to a cluster go to an dedicated log file.
			logPath := filepath.Join(
				artifactsDir, "cluster-create", strings.Replace(name, "/", "-", -1)+".log")
			clusterL, err := rootLogger(logPath, teeOpt)
			if err != nil {
				log.Fatal(ctx, err)
			}
			defer clusterL.close()
			opt := attachOpt{
				skipValidation: r.config.skipClusterValidationOnAttach,
				skipStop:       r.config.skipClusterStopOnAttach,
				skipWipe:       r.config.skipClusterWipeOnAttach,
			}
			return attachToExistingCluster(
				ctx, existingClusterName, clusterL, t.Cluster, opt, clusterFactory.r)
		}
		l.PrintfCtx(ctx, "Creating new cluster for test: %s. Cluster: %s\n", t.Name, name)

		cfg := clusterConfig{
			nodes:        t.Cluster,
			artifactsDir: artifactsDir,
			localCluster: local,
			alloc:        alloc,
		}
		return clusterFactory.newCluster(ctx, cfg, wStatus.SetStatus, teeOpt)
	}

	// When this method returns we'll destroy the cluster we had at the time,
	// unless we're exiting with errTestFailureAndDebugEnabled; in that case we
	// want the cluster to stick around.
	defer func() {
		if c != nil {
			l.Printf("Worker exiting; destroying cluster.")
			// We use a context that can't be canceled for the Destroy().
			c.Destroy(context.Background(), closeLogger, l)
		}
	}()

	// Loop until there's no more work in the pool, we get interrupted, or an
	// error occurs.
	for {
		select {
		case <-interrupt:
			return
		default:
		}

		if needDestroy && c != nil {
			wStatus.SetStatus("destroying cluster")
			// We use a context that can't be canceled for the Destroy().
			c.Destroy(context.Background(), closeLogger, l)
			c = nil
			cs = clusterSpec{}
			tag = ""
			needDestroy = false
		} else if c != nil {
			wStatus.SetStatus("wiping cluster")
			// We wipe clusters before reusing.
			if err := c.WipeE(ctx, l); err != nil {
				return errors.Wrapf(err, "failed to wipe cluster for reuse")
			}
		}

		oldCluster := c
		var testToRun testToRunRes
		var err error
		wStatus.SetStatus("getting work")
		// !!! make sure the case when there's not enough resources available to run
		// some tests (in particular because we've saved a bunch of clusters) is
		// handled fine.
		testToRun, c, cs, err = r.getWork(
			ctx, work, c, cs, tag, rg, interrupt, l,
			func(t testSpec, alloc resourceAllocation) (*cluster, error) {
				return allocateCluster(local, clusterName, artifactsDir, t, alloc)
			})
		if err != nil || testToRun.noWork {
			return err
		}
		wStatus.SetCluster(c)
		wStatus.SetTest(testToRun)
		wStatus.SetStatus("running test")

		// If the cluster changed, reset some state.
		if oldCluster != nil && oldCluster != c {
			needDestroy = false
			tag = ""
		}
		if testToRun.spec.Cluster.ReusePolicy == NoReuse {
			needDestroy = true
		} else if testToRun.spec.Cluster.ReusePolicy.policy == onlyTagged {
			// Our cluster is now tagged.
			tag = testToRun.spec.Cluster.ReusePolicy.tag
		}

		// Prepare the test's logger.
		logPath := ""
		if artifactsDir != "" {
			artifactsSuffix := "run_" + strconv.Itoa(testToRun.runNum)
			artifactsDir = filepath.Join(
				artifactsDir, teamCityNameEscape(testToRun.spec.Name), artifactsSuffix)
			logPath = filepath.Join(artifactsDir, "test.log")
		}
		testL, err := rootLogger(logPath, teeOpt)
		if err != nil {
			return err
		}
		t := &test{
			spec:         &testToRun.spec,
			registry:     r,
			artifactsDir: artifactsDir,
			l:            testL,
		}
		// Tell the cluster that, from now on, it will be run "on behalf of this
		// test".
		c.setTest(t)

		// Now run the test.
		l.PrintfCtx(ctx, "starting test: %s:%d", testToRun.spec.Name, testToRun.runNum)
		var success bool
		success, err = r.runTest(ctx, t, testToRun.runNum, c, artifactsDir, stdout, testL)
		testL.close()
		if err != nil {
			shout(ctx, l, stdout, "test returned error: %s: %s", t.Name(), err)
			// Mark the test as failed if it isn't already.
			if !t.Failed() {
				t.printAndFail(0 /* skip */, err)
			}
		} else {
			msg := "test passed"
			if !success {
				msg = fmt.Sprintf("test failed: %s:%d", t.Name(), testToRun.runNum)
			}
			l.PrintfCtx(ctx, msg)
		}
		// If a test failed and debug was set, we bail.
		if (err != nil || t.Failed()) && debug {
			failureMsg := fmt.Sprintf("%s (%d) - ", testToRun.spec.Name, testToRun.runNum)
			if err != nil {
				failureMsg += err.Error()
			} else {
				failureMsg += t.FailureMsg()
			}
			// Save the cluster for future debugging.
			c.Save(failureMsg)
			// Continue with a fresh cluster.
			c = nil
			if err != nil {
				return err
			}
		}
	}
}

// An error is returned in exceptional situations. The cluster cannot be reused
// if an error is returned.
// Returns true if the test is considered to have passed, false otherwise.
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy
//    the cluster.
func (r *registry) runTest(
	ctx context.Context,
	t *test,
	runNum int,
	c *cluster,
	artifactsDir string,
	stdout io.Writer,
	l *logger,
) (bool, error) {
	if t.spec.Skip != "" {
		return false, fmt.Errorf("Can't run skipped test: %s: %s", t.Name(), t.spec.Skip)
	}

	if teamCity {
		shout(ctx, l, stdout, "##teamcity[testStarted name='%s' flowId='%s']", t.Name(), t.Name())
	} else {
		var details []string
		var detail string
		if len(details) > 0 {
			detail = fmt.Sprintf(" [%s]", strings.Join(details, ","))
		}
		shout(ctx, l, stdout, "=== RUN   %s%s", t.Name(), detail)
	}

	r.status.Lock()
	r.status.running[t] = struct{}{}
	r.status.Unlock()

	callerName := func() string {
		// Make room for the skip PC.
		var pc [2]uintptr
		n := runtime.Callers(2, pc[:]) // skip + runtime.Callers + callerName
		if n == 0 {
			panic("zero callers found")
		}
		frames := runtime.CallersFrames(pc[:n])
		frame, _ := frames.Next()
		return frame.Function
	}

	t.runner = callerName()
	t.runnerID = goid.Get()

	defer func() {
		t.end = timeutil.Now()

		if err := recover(); err != nil {
			t.mu.Lock()
			t.mu.failed = true
			t.mu.output = append(t.mu.output, t.decorate(0 /* skip */, fmt.Sprint(err))...)
			t.mu.Unlock()
		}

		t.mu.Lock()
		t.mu.done = true
		t.mu.Unlock()

		dstr := fmt.Sprintf("%.2fs", t.duration().Seconds())

		if t.Failed() {
			t.mu.Lock()
			output := t.mu.output
			failLoc := t.mu.failLoc
			t.mu.Unlock()

			if teamCity {
				shout(ctx, l, stdout, "##teamcity[testFailed name='%s' details='%s' flowId='%s']",
					t.Name(), teamCityEscape(string(output)), t.Name(),
				)
			}

			shout(ctx, l, stdout, "--- FAIL: %s %s\n%s", t.Name(), dstr, output)
			if postIssues && issues.CanPost() && t.spec.Run != nil {
				authorEmail := getAuthorEmail(failLoc.file, failLoc.line)
				branch := "<unknown branch>"
				if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
					branch = b
				}
				msg := fmt.Sprintf("The test failed on branch=%s, cloud=%s:\n%s",
					branch, cloud, output)

				if err := issues.Post(
					context.Background(),
					fmt.Sprintf("roachtest: %s failed", t.Name()),
					"roachtest", t.Name(), msg, authorEmail, []string{"O-roachtest"},
				); err != nil {
					shout(ctx, l, stdout, "failed to post issue: %s", err)
				}
			}
		} else {
			shout(ctx, l, stdout, "--- PASS: %s %s", t.Name(), dstr)
			// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
			// TeamCity regards the test as successful.
		}

		if teamCity {
			shout(ctx, l, stdout, "##teamcity[testFinished name='%s' flowId='%s']", t.Name(), t.Name())

			escapedTestName := teamCityNameEscape(t.Name())
			artifactsGlobPath := filepath.Join(artifactsDir, escapedTestName, "**")
			artifactsSpec := fmt.Sprintf("%s => %s", artifactsGlobPath, escapedTestName)
			shout(ctx, l, stdout, "##teamcity[publishArtifacts '%s']", artifactsSpec)
		}

		r.recordTestFinish(completedTestInfo{
			test:    t.Name(),
			run:     runNum,
			start:   t.start,
			end:     t.end,
			pass:    !t.Failed(),
			failure: t.FailureMsg(),
		})
		r.status.Lock()
		delete(r.status.running, t)
		// Only include tests with a Run function in the summary output.
		if t.spec.Run != nil {
			if t.Failed() {
				r.status.fail[t] = struct{}{}
			} else if t.spec.Skip == "" {
				r.status.pass[t] = struct{}{}
			} else {
				r.status.skip[t] = struct{}{}
			}
		}
		r.status.Unlock()
	}()

	t.start = timeutil.Now()

	defer func() {
		if err := c.FetchLogs(ctx); err != nil {
			c.l.PrintfCtx(ctx, "failed to download logs: %s\n", err)
		}
	}()

	timeout := 12 * time.Hour
	if t.spec.Timeout != 0 {
		timeout = t.spec.Timeout
	}
	// Make sure the cluster has enough life left for the test plus enough headroom
	// after the test finishes so that the next test can be selected. If it
	// doesn't, extend it.
	// !!! there was a report that when cluster expiration is reached, the test
	// seems to fail in a messy way (in `registry.runTest`). it prints "test timed
	// out" then "test unresponsive after time out".
	minExp := time.Now().Add(timeout + time.Hour)
	if c.expiration.Before(minExp) {
		if err := c.Extend(ctx, timeout+time.Hour, l); err != nil {
			t.Fatalf("failed to extent cluster")
			return false, err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.mu.Lock()
	// t.Fatal() will cancel this context.
	t.mu.cancel = cancel
	t.mu.Unlock()

	// We run the actual test in a different goroutine because it might call
	// t.Fatal() which kills the goroutine, and also because we want to enforce a
	// timeout.
	success := false
	done := make(chan struct{})
	go func() {
		defer close(done) // closed only after we've grabbed the debug info below

		defer func() {
			// Detect replica divergence (i.e. ranges in which replicas have arrived
			// at the same log position with different states).
			c.FailOnReplicaDivergence(ctx, t)
			// Detect dead nodes in an inner defer. Note that this will call t.Fatal
			// when appropriate, which will cause the closure above to enter the
			// t.Failed() branch.
			c.FailOnDeadNodes(ctx, t)

			if t.Failed() {
				if err := c.FetchDebugZip(ctx); err != nil {
					t.l.Printf("failed to download logs: %s", err)
				}
				if err := c.FetchDmesg(ctx); err != nil {
					c.l.Printf("failed to fetch dmesg: %s", err)
				}
				if err := c.FetchJournalctl(ctx); err != nil {
					c.l.Printf("failed to fetch journalctl: %s", err)
				}
				if err := c.FetchCores(ctx); err != nil {
					c.l.Printf("failed to fetch cores: %s", err)
				}
				if err := c.CopyRoachprodState(ctx); err != nil {
					c.l.Printf("failed to copy roachprod state: %s", err)
				}
			} else {
				success = true
			}
		}()

		// This is the call to actually run the test.
		t.spec.Run(runCtx, t, c)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		// We hit a timeout. We're going to mark the test as failed (which will also
		// cancel its context).  Otherwise, we'll wait another 5 minutes in the hope
		// that the test reacts either to the ctx cancelation or to the fact that it
		// was marked as failed. If that happens, great - we return normally and so
		// the cluster can be reused. It the test does not react to anything, then
		// we return an error, which will cause the caller to stop everything and
		// destroy this cluster (as well as all the others). The cluster
		// cannot be reused since we have a runaway test goroutine that's presumably
		// going to continue using the cluster case.

		msg := fmt.Sprintf("test timed out (%s)", timeout)
		t.printfAndFail(0 /* skip */, "%s\n", msg)
	}
	return success, nil
}

// generateReport writes the final pass/fail line, produces a slack report if
// configured, and return true if the run is to considered successful or false
// otherwise.
func (r *registry) generateReport() (string, bool, error) {
	r.status.Lock()
	defer r.status.Unlock()
	postSlackReport(r.status.pass, r.status.fail, r.status.skip)

	fails := len(r.status.fail)
	var b strings.Builder
	var msg string
	if fails > 0 {
		msg = fmt.Sprintf("FAIL (%d fails)\n", fails)
	}
	if _, err := b.WriteString(msg); err != nil {
		return "", false, err
	}
	return b.String(), fails == 0, nil
}

// getWork selects the next test to run and creates a suitable cluster for it if
// need be. If a new cluster needs to be created, the method blocks until there
// are enough resources available to run it.
// getWork takes in a cluster; if not nil, tests that can reuse it are
// preferred. If a test that can reuse it is not found (or if there's no more
// work), the cluster is destroyed (and so its resources are released).
//
// Args:
// createCluster: a cluster factory.
func (r *registry) getWork(
	ctx context.Context,
	work *workPool,
	c *cluster,
	cs clusterSpec,
	tag string,
	rg *resourceGovernor,
	interrupt <-chan struct{},
	l *logger,
	createCluster func(t testSpec, res resourceAllocation) (*cluster, error),
) (_ testToRunRes, _ *cluster, _ clusterSpec, retErr error) {

	destroy := func() {
		c.Destroy(ctx, closeLogger, l)
		c = nil
		cs = clusterSpec{}
		tag = ""
	}

	// Loop until we get resources to run a test, there's no more work in the pool
	// or we get interrupted.
	// NOTE: What we're doing here is quite simplistic - we first select a test to
	// run and then block the worker until there are enough resources to run it.
	// There might be resources to run smaller tests, and that might be beneficial
	// (although not always - we don't want small tests to starve large tests), so
	// more complex schemes could be useful.

	// resLock starts as nil, but is set on further iterations if we looped around
	// because we were waiting for resources and were notified that there might be
	// new resources (after a resLock.Wait()). In those cases, resLock will be
	// locked as we loop around.
	first := true
	var resLock *resourceLock
	for {
		select {
		case <-interrupt:
			return testToRunRes{}, nil, clusterSpec{}, fmt.Errorf("interrupted")
		default:
		}

		testToRun := work.testToRun(ctx, cs, tag)
		if !testToRun.noWork {
			l.PrintfCtx(ctx, "Selected test: %s run: %d\n", testToRun.spec.Name, testToRun.runNum)
		}
		// If we have a cluster but can't reuse it for the test we've selected,
		// destroy it.
		if (!testToRun.canReuseCluster) && c != nil {
			l.PrintfCtx(ctx, "Cluster %s cannot be reused. Destroying.\n", c.name)
			destroy()
		}
		if testToRun.noWork {
			return testToRun, nil, clusterSpec{}, nil
		}
		defer func() {
			// Make sure we dispose of testToRun properly in case of early return.
			if retErr != nil {
				testToRun.rollback()
			}
		}()

		// Create a cluster, if we no longer have one.
		if c != nil {
			l.PrintfCtx(ctx, "Using existing cluster: %s\n", c.name)
			testToRun.confirm()
		} else {
			var err error
			var resAlloc resourceAllocation
			// Acquire resources for the cluster we're about to create.
			if rg != nil {
				cs := testToRun.spec.Cluster
				cpu := cs.NodeCount * cs.CPUs

				if resLock == nil {
					var err error
					resLock, err = rg.Lock()
					if err != nil {
						return testToRunRes{}, nil, clusterSpec{}, err
					}
					defer resLock.Unlock()
				}
				resAlloc, err = resLock.AllocateCPU(cpu)
				if err != nil {
					return testToRunRes{}, nil, clusterSpec{}, err
				}
				if resAlloc == (resourceAllocation{}) {
					if first {
						first = false
						l.PrintfCtx(ctx,
							"Insufficient resources for running %s. CPUs: %d. Waiting.\n",
							testToRun.spec.Name, cpu)
					}
					// We're not running this test at the moment. We'll wait for more
					// resources and loop around.
					testToRun.rollback()
					if err := resLock.Wait(); err != nil {
						return testToRunRes{}, nil, clusterSpec{}, err
					}
					continue
				}
				resLock.Unlock()
			}
			resLock = nil
			testToRun.confirm()
			c, err = createCluster(testToRun.spec, resAlloc)
			if err != nil {
				return testToRunRes{}, nil, clusterSpec{}, err
			}
		}
		return testToRun, c, testToRun.spec.Cluster, nil
	}
}

// shout logs a message both to a logger and to an io.Writer.
// If format doesn't end with a new line, one will be automatically added.
func shout(ctx context.Context, l *logger, stdout io.Writer, format string, args ...interface{}) {
	if len(format) == 0 || format[len(format)-1] != '\n' {
		format += "\n"
	}
	msg := fmt.Sprintf(format, args...)
	l.PrintfCtxDepth(ctx, 2 /* depth */, msg)
	fmt.Fprint(stdout, msg)
}

func (r *registry) serveHTTP(wr http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(wr, "<html><body>")
	fmt.Fprintf(wr, "<a href='debug/pprof'>pprof</a>")
	fmt.Fprintf(wr, "<p>")
	// Print the workers report.
	fmt.Fprintf(wr, "<h2>Workers:</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Worker</th>
	<th>Status</th>
	<th>Test</th>
	<th>Cluster</th>
	<th>Cluster reused</th>
	</tr>`)
	r.workersMu.Lock()
	workers := make([]*workerStatus, len(r.workersMu.workers))
	i := 0
	for _, w := range r.workersMu.workers {
		workers[i] = w
		i++
	}
	r.workersMu.Unlock()
	sort.Slice(workers, func(i int, j int) bool {
		l := workers[i]
		r := workers[j]
		return strings.Compare(l.name, r.name) < 0
	})
	for _, w := range workers {
		var testName string
		t := w.Test()
		if t.noWork {
			testName = "no work"
		} else {
			testName = fmt.Sprintf("%s (run %d)", t.spec.Name, t.runNum)
		}
		var clusterName string
		if w.Cluster() != nil {
			clusterName = w.Cluster().name
		}
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%t</td></tr>\n",
			w.name, w.Status(), testName, clusterName, t.canReuseCluster)
	}
	fmt.Fprintf(wr, "</table>")

	// Print the finished tests report.
	fmt.Fprintf(wr, "<p>")
	fmt.Fprintf(wr, "<h2>Finished tests:</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Test</th>
	<th>Status</th>
	<th>Duration</th>
	</tr>`)
	for _, t := range r.getCompletedTests() {
		name := fmt.Sprintf("%s (run %d)", t.test, t.run)
		status := "PASS"
		if !t.pass {
			status = "FAIL " + t.failure
		}
		duration := fmt.Sprintf("%s (%s - %s)", t.end.Sub(t.start), t.start, t.end)
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><td>%s</td><tr/>", name, status, duration)
	}
	fmt.Fprintf(wr, "</table>")

	// Print the saved clusters report.
	fmt.Fprintf(wr, "<p>")
	fmt.Fprintf(wr, "<h2>Clusters left alive for further debugging "+
		"(if --debug was specified):</h2>")
	fmt.Fprintf(wr, `<table border='1'>
	<tr><th>Cluster</th>
	<th>Test</th>
	</tr>`)
	for c, msg := range r.cr.savedClusters() {
		fmt.Fprintf(wr, "<tr><td>%s</td><td>%s</td><tr/>", c.name, msg)
	}
	fmt.Fprintf(wr, "</table>")

	fmt.Fprintf(wr, "</body></html>")
}
