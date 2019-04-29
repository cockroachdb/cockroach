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
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/petermattis/goid"
	"github.com/pkg/errors"
)

var (
	count        = 1
	debugEnabled = false
	postIssues   = true
	gceNameRE    = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
)

// testFilter holds the name and tag filters for filtering tests.
type testFilter struct {
	name   *regexp.Regexp
	tag    *regexp.Regexp
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

type testSpec struct {
	Skip string // if non-empty, test will be skipped
	// When Skip is set, this can contain more text to be printed in the logs
	// after the "--- SKIP" line.
	SkipDetails string
	// For subtests, Name is supposed to originally be assigned to the name of the
	// subtest when constructing the spec and then, once added to the registry, it
	// will automatically be expanded to contain all the parents' names. At that
	// point, subtestName will be populated to the original value of Name.
	Name        string
	subtestName string
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
	// Cluster provides the specification for the cluster to use for the test. Only
	// a top-level testSpec may contain a nodes specification. The cluster is
	// shared by all subtests.
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

	// A testSpec must specify only one of Run or SubTests. All subtests run in
	// the same cluster, without concurrency between them. Subtest should not
	// assume any particular state for the cluster as the SubTest may be run in
	// isolation.
	Run      func(ctx context.Context, t *test, c *cluster)
	SubTests []testSpec
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

// matchRegex returns true if the regex matches the test's name or any of the
// subtest names.
func (t *testSpec) matchRegex(filter *testFilter) bool {
	if t.matchOrSkip(filter) {
		return true
	}
	for i := range t.SubTests {
		if t.SubTests[i].matchRegex(filter) {
			return true
		}
	}
	return false
}

func (t *testSpec) matchRegexRecursive(filter *testFilter) []testSpec {
	var res []testSpec
	if t.matchOrSkip(filter) {
		res = append(res, *t)
	}
	for i := range t.SubTests {
		res = append(res, t.SubTests[i].matchRegexRecursive(filter)...)
	}
	return res
}

type registry struct {
	m              map[string]*testSpec
	clusters       map[string]string
	out            io.Writer
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
func newRegistry(opts ...registryOpt) *registry {
	r := &registry{
		m:        make(map[string]*testSpec),
		clusters: make(map[string]string),
		out:      os.Stdout,
	}
	r.config.skipClusterWipeOnAttach = !clusterWipe
	for _, opt := range opts {
		if err := opt(r); err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v\n", err)
			os.Exit(1)
		}
	}
	return r
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

// verifyValidClusterName verifies that the test name can be turned into a cluster
// name when run by TeamCity. Outside of TeamCity runs, depending on the user
// running it and the "cluster id" component of a cluster name, the name may
// still be invalid; however, this method is designed to catch test names
// that will cause errors on TeamCity but not in a developer's local test
// environment.
func (r *registry) verifyValidClusterName(testName string) error {
	// Both the name of the cluster, and the names of the individual nodes in the
	// cluster, must be valid identifiers in GCE when running on TeamCity. An
	// identifier can be tested using a regular expression. Also note that, due to
	// the specifics of the regular expression, we cannot assume that a valid
	// cluster name implies valid node names, or vice-versa; we therefore
	// construct both a TeamCity cluster name and a TeamCity node name and
	// validate both.

	// The name of a cluster is constructed as "[cluster ID][test name]"
	// In TeamCity runs, the cluster ID is currently a prefix with 6 digits, but
	// we use 7 here for a bit of breathing room.
	teamcityClusterName := makeGCEClusterName("teamcity-1234567-" + testName)
	if !gceNameRE.MatchString(teamcityClusterName) {
		return fmt.Errorf(
			"test name '%s' results in invalid cluster name"+
				" (generated cluster name '%s' must match regex '%s')."+
				" The test name may be too long or have invalid characters",
			testName,
			teamcityClusterName,
			gceNameRE,
		)
	}

	// The node names are constructed using the cluster name, plus a 4 digit node
	// ID.
	teamcityNodeName := makeGCEClusterName("teamcity-1234567-" + testName + "-1234")
	if !gceNameRE.MatchString(teamcityNodeName) {
		return fmt.Errorf(
			"test name '%s' results in invalid cluster node names"+
				" (generated node name '%s' must match regex '%s')."+
				" The test name may be too long or have invalid characters",
			testName,
			teamcityNodeName,
			gceNameRE,
		)
	}

	// Verify that the cluster name is not shared with an existing test.
	if t, ok := r.clusters[teamcityClusterName]; ok {
		return fmt.Errorf("test %s and test %s have equivalent nightly cluster names: %s",
			testName, t, teamcityClusterName)
	}
	r.clusters[teamcityClusterName] = testName
	return nil
}

func (r *registry) prepareSpec(spec *testSpec, depth int) error {
	if depth == 0 {
		spec.subtestName = spec.Name
		// Only top-level tests can create clusters, so those are the only ones for
		// which we need to verify the cluster name.
		if err := r.verifyValidClusterName(spec.Name); err != nil {
			return err
		}
	}

	if (spec.Run != nil) == (len(spec.SubTests) > 0) {
		return fmt.Errorf("%s: must specify only one of Run or SubTests", spec.Name)
	}

	if spec.Run == nil && spec.Timeout > 0 {
		return fmt.Errorf("%s: timeouts only apply to tests specifying Run", spec.Name)
	}

	if depth > 0 && spec.Cluster.NodeCount > 0 {
		return fmt.Errorf("%s: subtest may not provide cluster specification", spec.Name)
	}

	for i := range spec.SubTests {
		spec.SubTests[i].subtestName = spec.SubTests[i].Name
		spec.SubTests[i].Name = spec.Name + "/" + spec.SubTests[i].Name
		if err := r.prepareSpec(&spec.SubTests[i], depth+1); err != nil {
			return err
		}
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
	return nil
}

func (r *registry) Add(spec testSpec) {
	if _, ok := r.m[spec.Name]; ok {
		fmt.Fprintf(os.Stderr, "test %s already registered\n", spec.Name)
		os.Exit(1)
	}
	if err := r.prepareSpec(&spec, 0); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	r.m[spec.Name] = &spec
}

// ListTopLevel lists the top level tests that match re, or that have a subtests
// that matches re.
func (r *registry) ListTopLevel(filter *testFilter) []*testSpec {
	var results []*testSpec
	for _, t := range r.m {
		if t.matchRegex(filter) {
			results = append(results, t)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	return results
}

// ListAll lists all tests that match one of the filters. If a subtest matches
// but a parent doesn't, only the subtest is returned. If a parent matches, all
// subtests are returned.
func (r *registry) ListAll(filters []string) []string {
	filter := newFilter(filters)
	var tests []testSpec
	for _, t := range r.m {
		tests = append(tests, t.matchRegexRecursive(filter)...)
	}
	var names []string
	for _, t := range tests {
		if t.Skip == "" && t.minVersion != nil {
			if !r.buildVersion.AtLeast(t.minVersion) {
				t.Skip = fmt.Sprintf("build-version (%s) < min-version (%s)",
					r.buildVersion, t.minVersion)
			}
		}
		name := t.Name
		if t.Skip != "" {
			name += " (skipped: " + t.Skip + ")"
		}

		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Run runs the tests that match the filter.
//
// Args:
// artifactsDir: The path to the dir where log files will be put. If empty, all
//   logging will go to stdout/stderr.
func (r *registry) Run(filters []string, parallelism int, artifactsDir string, user string) int {
	filter := newFilter(filters)
	// Find the top-level tests to run.
	tests := r.ListTopLevel(filter)
	if len(tests) == 0 {
		fmt.Fprintf(r.out, "warning: no tests to run %s\n", filters)
		fmt.Fprintf(r.out, "FAIL\n")
		return 1
	}

	// Skip any tests for which the min-version is less than the build-version.
	for _, t := range tests {
		if t.Skip == "" && t.minVersion != nil {
			if !r.buildVersion.AtLeast(t.minVersion) {
				t.Skip = fmt.Sprintf("build-version (%s) < min-version (%s)",
					r.buildVersion, t.minVersion)
			}
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(count * len(tests))

	// We can't run tests in parallel on local clusters or on an existing
	// cluster.
	if local || clusterName != "" {
		parallelism = 1
	}
	// Limit the parallelism to the number of tests. The primary effect this has
	// is that we'll log to stdout/stderr if only one test is being run.
	if parallelism > len(tests) {
		parallelism = len(tests)
	}

	r.status.running = make(map[*test]struct{})
	r.status.pass = make(map[*test]struct{})
	r.status.fail = make(map[*test]struct{})
	r.status.skip = make(map[*test]struct{})

	cr := newClusterRegistry()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sem := make(chan struct{}, parallelism)
		for j := 0; j < count; j++ {
			for i := range tests {
				sem <- struct{}{}
				runNum := j + 1
				if count == 1 {
					runNum = 0
				}
				// Log to stdout/stderr if we're not running tests in parallel.
				teeOpt := noTee
				if parallelism == 1 {
					teeOpt = teeToStdout
				}

				artifactsSuffix := ""
				if runNum != 0 {
					artifactsSuffix = "run_" + strconv.Itoa(runNum)
				}
				var runDir string
				if artifactsDir != "" {
					runDir = filepath.Join(
						artifactsDir, teamCityNameEscape(tests[i].subtestName), artifactsSuffix)
				}

				r.runAsync(
					ctx, tests[i], filter, nil /* parent */, nil, /* cluster */
					runNum, teeOpt, runDir, user, cr, func(failed bool) {
						wg.Done()
						<-sem
					})
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Periodically output test status to give an indication of progress.
	if r.statusInterval == 0 {
		r.statusInterval = time.Minute
	}
	ticker := time.NewTicker(r.statusInterval)
	defer ticker.Stop()

	// Shut down test clusters when interrupted (for example CTRL+C).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)

	for i := 1; ; i++ {
		select {
		case <-done:
			r.status.Lock()
			defer r.status.Unlock()
			postSlackReport(r.status.pass, r.status.fail, r.status.skip)

			if len(r.status.fail) > 0 {
				fmt.Fprintln(r.out, "FAIL")
				return 1
			}
			fmt.Fprintf(r.out, "PASS\n")
			return 0

		case <-ticker.C:
			r.status.Lock()
			runningTests := make([]*test, 0, len(r.status.running))
			for t := range r.status.running {
				runningTests = append(runningTests, t)
			}
			sort.Slice(runningTests, func(i, j int) bool {
				return runningTests[i].Name() < runningTests[j].Name()
			})
			var buf bytes.Buffer
			for _, t := range runningTests {
				if t.spec.Run == nil {
					// Ignore tests with subtests.
					continue
				}

				t.mu.Lock()
				done := t.mu.done
				var status map[int64]testStatus
				if !done {
					status = make(map[int64]testStatus, len(t.mu.status))
					for k, v := range t.mu.status {
						status[k] = v
					}
					if len(status) == 0 {
						// If we have no other status messages display this unknown state.
						status[0] = testStatus{
							msg:  "???",
							time: timeutil.Now(),
						}
					}
				}
				t.mu.Unlock()
				if !done {
					ids := make([]int64, 0, len(status))
					for id := range status {
						ids = append(ids, id)
					}
					sort.Slice(ids, func(i, j int) bool {
						// Force the goroutine ID for the main test goroutine to sort to
						// the front. NB: goroutine IDs are not monotonically increasing
						// because each thread has a small cache of IDs for allocation.
						if ids[j] == t.runnerID {
							return false
						}
						if ids[i] == t.runnerID {
							return true
						}
						return ids[i] < ids[j]
					})

					fmt.Fprintf(&buf, "[%4d] %s: ", i, t.Name())

					for j := range ids {
						s := status[ids[j]]
						duration := timeutil.Now().Sub(s.time)
						progStr := ""
						if s.progress > 0 {
							progStr = fmt.Sprintf("%.1f%%|", 100*s.progress)
						}
						if j > 0 {
							buf.WriteString(", ")
						}
						fmt.Fprintf(&buf, "%s (%s%s)", s.msg, progStr,
							time.Duration(duration.Seconds()+0.5)*time.Second)
					}

					fmt.Fprintf(&buf, "\n")
				}
			}
			fmt.Fprint(r.out, buf.String())
			r.status.Unlock()

		case <-sig:
			if !debugEnabled {
				cancel()
				// Destroy all clusters. Don't wait more than 5 min for that though.
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				cr.destroyAllClusters(ctx)
				cancel()
			}
		}
	}
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
	l        *logger
	runner   string
	runnerID int64
	start    time.Time
	end      time.Time

	// debugEnabled is a test scoped value which enables automated tests to
	// enable debugging without enabling debugging for all tests.
	// It is a bit of a hack added to help debug #34458.
	debugEnabled bool

	// artifactsDir is the path to the directory holding all the artifacts for
	// this test. It will contain a test.log file, cluster logs, and
	// subdirectories for subtests.
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
		status map[int64]testStatus
		output []byte
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
	t.mu.output = append(t.mu.output, t.decorate(skip+1, fmt.Sprint(args...))...)
	t.mu.failed = true
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

func (t *test) printfAndFail(skip int, format string, args ...interface{}) {
	msg := t.decorate(skip+1, fmt.Sprintf(format, args...))
	t.l.Printf("test failure: " + msg)

	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.output = append(t.mu.output, msg...)
	t.mu.failed = true
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
	failed := t.mu.failed
	t.mu.RUnlock()
	return failed
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

var _ = (*test)(nil).IsBuildVersion // avoid unused lint

// runAsync starts a goroutine that runs a test. If the test has subtests,
// runAsync will be invoked recursively, but in a blocking manner.
//
// Args:
// parent: The test's parent. Nil if the test is not a subtest.
// c: The cluster on which the test (and all subtests) will run. If nil, a new
//    cluster will be created.
// runNum: The 1-based index of this test run, if --count > 1. Otherwise (if
// 		   there's a single run), runNum is 0.
func (r *registry) runAsync(
	ctx context.Context,
	spec *testSpec,
	filter *testFilter,
	parent *test,
	c *cluster,
	runNum int,
	teeOpt teeOptType,
	artifactsDir string,
	user string,
	cr *clusterRegistry,
	done func(failed bool),
) {
	t := &test{
		spec:         spec,
		registry:     r,
		artifactsDir: artifactsDir,
	}
	var logPath string
	if artifactsDir != "" {
		logPath = filepath.Join(artifactsDir, "test.log")
	}
	l, err := rootLogger(logPath, teeOpt)
	FatalIfErr(t, err)
	t.l = l
	out := io.MultiWriter(r.out, t.l.file)

	if teamCity {
		fmt.Printf("##teamcity[testStarted name='%s' flowId='%s']\n", t.Name(), t.Name())
	} else {
		var details []string
		if t.spec.Skip != "" {
			details = append(details, "skip")
		}
		var detail string
		if len(details) > 0 {
			detail = fmt.Sprintf(" [%s]", strings.Join(details, ","))
		}
		fmt.Fprintf(out, "=== RUN   %s%s\n", t.Name(), detail)
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

	go func() {
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
					fmt.Fprintf(
						r.out, "##teamcity[testFailed name='%s' details='%s' flowId='%s']\n",
						t.Name(), teamCityEscape(string(output)), t.Name(),
					)
				}

				fmt.Fprintf(out, "--- FAIL: %s (%s)\n%s", t.Name(), dstr, output)
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
						"roachtest", t.Name(), msg, authorEmail,
						[]string{"O-roachtest"},
					); err != nil {
						fmt.Fprintf(out, "failed to post issue: %s\n", err)
					}
				}
			} else if t.spec.Skip == "" {
				fmt.Fprintf(out, "--- PASS: %s (%s)\n", t.Name(), dstr)
				// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
				// TeamCity regards the test as successful.
			} else {
				if teamCity {
					fmt.Fprintf(r.out, "##teamcity[testIgnored name='%s' message='%s']\n",
						t.Name(), teamCityEscape(t.spec.Skip))
				}
				fmt.Fprintf(out, "--- SKIP: %s (%s)\n\t%s\n", t.Name(), dstr, t.spec.Skip)
				if t.spec.SkipDetails != "" {
					fmt.Fprintf(out, "Details: %s\n", t.spec.SkipDetails)
				}
			}

			if teamCity {
				fmt.Fprintf(r.out, "##teamcity[testFinished name='%s' flowId='%s']\n", t.Name(), t.Name())

				// Only publish artifacts for failed tests. At the time of writing, a full roachtest
				// suite results in ~6gb of artifacts which we can't retain for more than a few days
				// (and this in turn delays the resolution of failures).
				if t.Failed() && artifactsDir != "" {
					escapedTestName := teamCityNameEscape(t.Name())
					artifactsGlobPath := filepath.Join(artifactsDir, "**")
					artifactsSpec := fmt.Sprintf("%s => %s", artifactsGlobPath, escapedTestName)
					fmt.Fprintf(r.out, "##teamcity[publishArtifacts '%s']\n", artifactsSpec)
				}
			}

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

			done(t.Failed())
		}()

		t.start = timeutil.Now()

		if t.spec.Skip != "" {
			return
		}

		if c == nil {
			if clusterName == "" {
				var name string
				if !local {
					name = clusterID
					if name == "" {
						name = fmt.Sprintf("%d", timeutil.Now().Unix())
					}
					name += "-" + t.Name()
				}
				cfg := clusterConfig{
					name:         name,
					nodes:        t.spec.Cluster,
					useIOBarrier: t.spec.UseIOBarrier,
					artifactsDir: t.ArtifactsDir(),
					localCluster: local,
					teeOpt:       teeOpt,
					user:         user,
				}
				var err error
				c, err = newCluster(ctx, t.l, cfg, cr)
				if err != nil {
					t.Skip("failed to created cluster", err.Error())
				}
			} else {
				opt := attachOpt{
					skipValidation: r.config.skipClusterValidationOnAttach,
					skipStop:       r.config.skipClusterStopOnAttach,
					skipWipe:       r.config.skipClusterWipeOnAttach,
				}
				var err error
				c, err = attachToExistingCluster(ctx, clusterName, t.l, t.spec.Cluster, opt, cr)
				FatalIfErr(t, err)
			}
			if c != nil {
				defer func() {
					if (!debugEnabled && !t.debugEnabled) || !t.Failed() {
						c.Destroy(ctx, closeLogger)
					} else {
						c.l.Printf("not destroying cluster to allow debugging\n")
					}
				}()
			}
		} else {
			c = c.clone()
		}
		c.setTest(t)

		// If we have subtests, handle them here and return.
		if t.spec.Run == nil {
			for i := range t.spec.SubTests {
				childSpec := t.spec.SubTests[i]
				if childSpec.matchRegex(filter) {
					var wg sync.WaitGroup
					wg.Add(1)

					// Each subtest gets its own subdir in the parent's artifacts dir.
					var childDir string
					if t.ArtifactsDir() != "" {
						childDir = filepath.Join(t.ArtifactsDir(), teamCityNameEscape(childSpec.subtestName))
					}

					r.runAsync(ctx, &childSpec, filter, t, c,
						runNum, teeOpt, childDir, user, cr, func(failed bool) {
							if failed {
								// Mark the parent test as failed since one of the subtests
								// failed.
								t.mu.Lock()
								t.mu.failed = true
								t.mu.Unlock()
							}
							if failed && debugEnabled {
								// The test failed and debugging is enabled. Don't try to stumble
								// forward running another test or subtest, just exit
								// immediately.
								os.Exit(1)
							}
							wg.Done()
						})
					wg.Wait()
				}
			}
			return
		}

		// No subtests, so this is a leaf test.

		timeout := c.expiration.Add(-10 * time.Minute).Sub(timeutil.Now())
		if timeout <= 0 {
			t.spec.Skip = fmt.Sprintf("cluster expired (%s)", timeout)
			return
		}

		if t.spec.Timeout > 0 && timeout > t.spec.Timeout {
			timeout = t.spec.Timeout
		}

		done := make(chan struct{})
		defer close(done) // closed only after we've grabbed the debug info below

		defer func() {
			if t.Failed() {
				if err := c.FetchDebugZip(ctx); err != nil {
					c.l.Printf("failed to download debug zip: %s", err)
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
			}
			// NB: fetch the logs even when we have a debug zip because
			// debug zip can't ever get the logs for down nodes.
			// We only save artifacts for failed tests in CI, so this
			// duplication is acceptable.
			if err := c.FetchLogs(ctx); err != nil {
				c.l.Printf("failed to download logs: %s", err)
			}
		}()
		// Detect replica divergence (i.e. ranges in which replicas have arrived
		// at the same log position with different states).
		defer c.FailOnReplicaDivergence(ctx, t)
		// Detect dead nodes in an inner defer. Note that this will call t.Fatal
		// when appropriate, which will cause the closure above to enter the
		// t.Failed() branch.
		defer c.FailOnDeadNodes(ctx, t)

		runCtx, cancel := context.WithCancel(ctx)
		t.mu.Lock()
		// t.Fatal() will cancel this context.
		t.mu.cancel = cancel
		t.mu.Unlock()

		go func() {
			defer cancel()

			select {
			case <-time.After(timeout):
				t.printfAndFail(0 /* skip */, "test timed out (%s)\n", timeout)
				if err := c.FetchDebugZip(ctx); err != nil {
					c.l.Printf("failed to download logs: %s", err)
				}
				// NB: c.destroyState is nil for cloned clusters (i.e. in subtests).
				if !debugEnabled && c.destroyState != nil {
					// We don't close the logger here because the cluster may still be in
					// use by the test.
					c.Destroy(ctx, dontCloseLogger)
				}
			case <-done:
			}
		}()

		t.spec.Run(runCtx, t, c)
	}()
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
