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
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	version "github.com/hashicorp/go-version"
	"github.com/petermattis/goid"

	"github.com/cockroachdb/cockroach/pkg/cmd/internal/issues"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	GCEClusterNameLimit = 63
)

var (
	count        = 1
	debugEnabled = false
	postIssues   = true
	// 63 alpha-num chars, plus dashes.
	clusterNameRE = regexp.MustCompile(`^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
)

func makeFilterRE(filter []string) *regexp.Regexp {
	if len(filter) == 0 {
		return regexp.MustCompile(`.`)
	}
	for i := range filter {
		filter[i] = "(" + filter[i] + ")"
	}
	return regexp.MustCompile(strings.Join(filter, "|"))
}

type reusePolicy int

const (
	// A sentinel so that we can validate that all the tests set a policy.
	_ reusePolicy = iota
	noReuse
	onlyTagged
	any
)

// ClusterReusePolicy indicates what other tests, if any, can reuse the cluster
// after a particular test has finished running (either passing or failing).
// NOTE that only tests whose cluster spec matches can ever run on the same
// cluster, regardless of this policy.
type ClusterReusePolicy struct {
	policy reusePolicy
	// Only for the onlyTagged policy.
	tag string
}

// NoReuse means the cluster cannot be reused.
var NoReuse = ClusterReusePolicy{policy: noReuse}

// Any means that the cluster can be used by any other test.
var Any = ClusterReusePolicy{policy: any}

// OnlyTagged means that the cluster can only be reused by tests with the same
// policy and tag.
func OnlyTagged(tag string) ClusterReusePolicy {
	return ClusterReusePolicy{policy: onlyTagged, tag: tag}
}

type testSpec struct {
	Skip string // if non-empty, test will be skipped
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
	// Stable indicates whether failure of the test will result in failure of the
	// test run. Tests should be added initially as unstable, and only converted
	// to stable once they have passed successfully on multiple nightly (not
	// local) runs.
	Stable bool
	// Nodes provides the specification for the cluster to use for the test.
	Nodes []nodeSpec
	// Run is the test function.
	Run                func(ctx context.Context, t *test, c *cluster)
	ClusterReusePolicy ClusterReusePolicy
}

// matchRegex returns true if the regex matches the test's name.
func (t *testSpec) matchRegex(re *regexp.Regexp) bool {
	return re.MatchString(t.Name)
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

		// If set, a cluster will not be wiped before starting a (sub)test. For
		// testing.
		// !!! still needed?
		skipClusterWipeOnTestStart bool
	}

	status struct {
		syncutil.Mutex
		running map[*test]struct{}
		pass    map[*test]struct{}
		fail    map[*test]struct{}
		skip    map[*test]struct{}
	}
}

func newRegistry() *registry {
	r := &registry{
		m:        make(map[string]*testSpec),
		clusters: make(map[string]string),
		out:      os.Stdout,
	}
	r.config.skipClusterWipeOnAttach = !clusterWipe
	return r
}

func (r *registry) setBuildVersion(buildTag string) error {
	var err error
	r.buildVersion, err = version.NewVersion(buildTag)
	return err
}

func (r *registry) loadBuildVersion() {
	getLatestTag := func() (string, error) {
		cmd := exec.Command("git", "describe", "--abbrev=0", "--tags")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(out)), nil
	}

	buildTag, err := getLatestTag()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
	if err := r.setBuildVersion(buildTag); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

// verifyClusterName verifies that the test name can be turned into a cluster
// name when run by TeamCity. Outside of TeamCity runs, depending on the user
// running it and the "cluster id" component of a cluster name, the name might
// be too long when running in different configurations.
func (r *registry) verifyClusterName(testName string) error {
	// TeamCity build IDs are currently 6 digits, but we use 7 here for a bit of
	// breathing room.
	name := makeGCEClusterName("teamcity-1234567-" + testName)
	if !clusterNameRE.MatchString(name) {
		return fmt.Errorf("cluster name '%s' must match regex '%s'",
			name, clusterNameRE)
	}
	if t, ok := r.clusters[name]; ok {
		return fmt.Errorf("test %s and test %s have equivalent nightly cluster names: %s",
			testName, t, name)
	}
	r.clusters[name] = testName
	return nil
}

func (r *registry) prepareSpec(spec *testSpec) error {
	if spec.Run == nil {
		return fmt.Errorf("%s: must specify Run", spec.Name)
	}

	if spec.ClusterReusePolicy == (ClusterReusePolicy{}) {
		return fmt.Errorf("%s: must specify a ClusterReusePolicy", spec.Name)
	}

	if spec.MinVersion != "" {
		// We append "-0" to the min-version spec so that we capture all
		// prereleases of the specified version. Otherwise, "v2.1.0" would compare
		// greater than "v2.1.0-alpha.x".
		var err error
		spec.minVersion, err = version.NewVersion(spec.MinVersion + "-0")
		if err != nil {
			return fmt.Errorf("%s: unable to parse min-version: %s: %+v",
				spec.Name, spec.MinVersion, err)
		}
	}
	return nil
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
func (r *registry) GetTests(re *regexp.Regexp) []testSpec {
	var tests []testSpec
	for _, t := range r.m {
		if !t.matchRegex(re) {
			continue
		}
		if t.Skip == "" && t.minVersion != nil {
			if r.buildVersion.LessThan(t.minVersion) {
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
func (r *registry) List(filter []string) []string {
	filterRE := makeFilterRE(filter)
	tests := r.GetTests(filterRE)
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
	l        *logger
	runner   string
	runnerID int64
	start    time.Time
	end      time.Time
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

func (t *test) Fatal(args ...interface{}) {
	t.printAndFail(args...)
	runtime.Goexit()
}

func (t *test) Fatalf(format string, args ...interface{}) {
	t.printfAndFail(format, args...)
	runtime.Goexit()
}

// FatalIfErr calls t.Fatal() if err != nil.
func FatalIfErr(t *test, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func (t *test) printAndFail(args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.output = append(t.mu.output, t.decorate(fmt.Sprint(args...))...)
	t.mu.failed = true
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

func (t *test) printfAndFail(format string, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.output = append(t.mu.output, t.decorate(fmt.Sprintf(format, args...))...)
	t.mu.failed = true
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

func (t *test) decorate(s string) string {
	// Skip two extra frames to account for this function
	// and runtime.Callers itself.
	var pc [50]uintptr
	n := runtime.Callers(3, pc[:])
	if n == 0 {
		panic("zero callers found")
	}

	buf := new(bytes.Buffer)
	frames := runtime.CallersFrames(pc[:n])
	sep := "\t"
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		if frame.Function == t.runner {
			break
		}
		if !t.mu.failed {
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
	// We append "-0" to the min-version spec so that we capture all
	// prereleases of the specified version. Otherwise, "v2.1.0" would compare
	// greater than "v2.1.0-alpha.x".
	vers, err := version.NewVersion(minVersion + "-0")
	if err != nil {
		t.Fatal(err)
	}
	return !t.registry.buildVersion.LessThan(vers)
}

// errTestFailureAndDebugEnabled represents a test failure when --debug was
// specified.
type errTestFailureAndDebugEnabled struct {
	test        string
	testFailure string
}

func newErrTestFailureAndDebugEnabled(test string, failure string) error {
	return errTestFailureAndDebugEnabled{test: test, testFailure: failure}
}

// Error implements the error interface.
func (e errTestFailureAndDebugEnabled) Error() string {
	return fmt.Sprintf(
		"%s: test failure and debug enabled. Leaving cluster around for debugging: %s. Failure: %s",
		e.test, e.testFailure)
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
	we.mu.errs = append(we.mu.errs, err)
	defer we.mu.Unlock()
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
	spec  testSpec
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
	var tc testWithCount
	for idx, tc = range p.mu.tests {
		if tc.spec.Name == name {
			break
		}
	}
	if idx == -1 {
		log.Fatalf(context.TODO(), "failed to find test: %s", name)
	}

	tc.count--
	spec := tc.spec
	if tc.count == 0 {
		// We've selected the last run for a test. Take that test out of the pool.
		p.mu.tests = append(p.mu.tests[:idx], p.mu.tests[idx+1:]...)
	}
	// Adjust the tag clusters bookkeeping.
	if clusterReuse {
		if existingClusterTag == "" && spec.ClusterReusePolicy.policy == onlyTagged {
			// We had an untagged cluster and we're about to tag it.
			tag := spec.ClusterReusePolicy.tag
			if _, ok := p.mu.clusters[tag]; ok {
				p.mu.clusters[tag]++
			} else {
				p.mu.clusters[tag] = 1
			}
		} else if existingClusterTag != "" && spec.ClusterReusePolicy == NoReuse {
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
		tag := spec.ClusterReusePolicy.tag
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
func (p *workPool) findCompatibleTestsLocked(clusterSpec []nodeSpec, tag string) []testWithCount {
	var tests []testWithCount
	for _, tc := range p.mu.tests {
		if clusterCompatibleWithTest(clusterSpec, tc.spec) {
			tests = append(tests, tc)
		}
	}
	return tests
}

func clusterCompatibleWithTest(clusterSpec []nodeSpec, t testSpec) bool {
	nodes := t.Nodes
	if len(clusterSpec) != len(nodes) {
		return false
	}
	for i := range clusterSpec {
		l := clusterSpec[i]
		r := nodes[i]
		if l != r {
			return false
		}
	}
	return true
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
}

// testToRun selects the best test to run and adjusts the bookkeeping in the
// workPool assuming that the test will be started.
//
// Args:
// spec: The spec of an existing cluster that can be reused. Nil if no such
//   cluster.
// tag: The tag of the existing cluster. Empty if not such cluster or the
//   cluster isn't tagged.
func (p *workPool) testToRun(spec []nodeSpec, tag string) testToRunRes {
	// Short-circuit if there's no more work.
	p.mu.Lock()
	if len(p.mu.tests) == 0 {
		p.mu.Unlock()
		return testToRunRes{noWork: true}
	}
	p.mu.Unlock()

	if spec == nil {
		// If we don't have a cluster, we pick the test with the highest number of
		// runs left.
		// TODO(andrei): We could be smarter in guessing what kind of cluster is
		// best to allocate.
		var candidateIdx int
		candidateCount := 0
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, t := range p.mu.tests {
			if t.count > candidateCount {
				candidateIdx = i
				candidateCount = t.count
			}
		}
		tc := p.mu.tests[candidateIdx]
		runNum := p.count - tc.count + 1
		p.registerTestStartLocked(tc.spec.Name, "" /* existingClusterTag */, false /* clusterReuse */)
		return testToRunRes{spec: tc.spec, runNum: runNum, canReuseCluster: false}
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
		return p.testToRun(nil /* spec */, tag)
	}
	defer p.mu.Unlock()

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
	p.registerTestStartLocked(candidate.spec.Name, tag, true /* clusterReuse */)
	return testToRunRes{spec: candidate.spec, runNum: runNum, canReuseCluster: true}
}

type taggedClusters map[string]int

func scoreTestAgainstCluster(tc testWithCount, tag string, clusters taggedClusters) int {
	t := tc.spec
	if tag != "" && t.ClusterReusePolicy != OnlyTagged("tag") {
		log.Fatal(context.TODO(), "incompatible test and cluster. Cluster tag: %s. Test policy: %+v", tag, t.ClusterReusePolicy)
	}
	score := 0
	if t.ClusterReusePolicy == Any {
		score = 1000000
	} else if t.ClusterReusePolicy.policy == onlyTagged {
		score = 500000
		if tag == "" {
			// We have an untagged cluster and a tagged test. Within this category of
			// tests, we prefer the tags with the fewest existing clusters.
			score -= 1000 * clusters[t.ClusterReusePolicy.tag]
		}
	} else { // NoReuse policy
		score = 0
	}

	// We prefer tests that have run fewer times (so, that have more runs left).
	score += tc.count

	return score
}

func (r *registry) Run(
	ctx context.Context,
	filter []string,
	count int,
	parallelism int,
	cpuQuota int,
	clusterName string,
	local bool,
	artifactsDir string,
	debug bool,
) int {

	// If asked to use an existing cluster, attach to it.
	if clusterName != "" && parallelism != 1 {
		fmt.Fprintf(os.Stderr,
			"--cluster incompatible with --parallelism. Use --parallelism=1.\n")
		return 1
	}
	if local && parallelism != 1 {
		fmt.Fprintf(os.Stderr,
			"--local incompatible with --parallelism. Use --parallelism=1.\n")
		return 1
	}

	// !!! to bring back:
	// - ctrl-c handling
	// - periodic progress output

	filterRE := makeFilterRE(filter)
	tests := r.GetTests(filterRE)

	var skipped, notSkipped []testSpec
	for _, s := range tests {
		if s.Skip == "" {
			notSkipped = append(notSkipped, s)
		} else {
			skipped = append(skipped, s)

			if teamCity {
				fmt.Fprintf(r.out, "##teamcity[testIgnored name='%s' message='%s']\n",
					s.Name, s.Skip)
			}
			fmt.Fprintf(r.out, "--- SKIP: %s (%s)\n\t%s\n", s.Name, "0.00s", s.Skip)
		}
	}
	tests = notSkipped

	n := len(tests)
	if n == 0 {
		// NOTE: Arguably we should return an error here, or find a way to indicate
		// that no tests have run.
		return 0
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
		rg = newResourceGovernor(cpuQuota)
	}

	// Log to stdout/stderr if we're not running tests in parallel.
	teeOpt := noTee
	if parallelism == 1 {
		teeOpt = teeToStdout
	}

	var wg sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		stopper.RunWorker(ctx, func(ctx context.Context) {
			if err := r.runWorker(
				ctx, work, rg, clusterName, local,
				stopper.ShouldQuiesce(), debug, artifactsDir, teeOpt,
			); err != nil {
				errs.AddErr(err)
				stopper.Quiesce(ctx)
			}
			wg.Done()
		})
	}
	wg.Wait()
	if errs.Err() != nil {
		return 1
	}
	return 0
}

// runWorker runs tests in a loop until work is exhausted.
//
// Errors are returned in exceptional circumstances. If an error is returned,
// resources might have leaked out of the resourceGovernor. As such, higher
// layers are expected to not try to allocate resources any more and terminate.
//
// Args:
// rg: The resourceGovernor that will be asked for resources every time a new
//   cluster needs to be created. If nil, no resourceGovernor is consulted. Nil
//   has to be passed when clusterName is passed.
// clusterName: If not empty, the name of a cluster to use for all tests. This
//   worker is presumed to have exclusive access to the cluster, since it will
//   wipe it every after test. If the cluster is not compatible with any test,
//   an error will be returned.
func (r *registry) runWorker(
	ctx context.Context,
	work *workPool,
	rg *resourceGovernor,
	clusterName string,
	local bool,
	interrupt <-chan struct{},
	debug bool,
	artifactsDir string,
	teeOpt teeOptType,
) (retErr error) {
	var c *cluster             // The cluster currently being used.
	var clusterSpec []nodeSpec // The spec of the current cluster.
	var tag string             // The tag of the current cluster.
	// Set if the current cluster needs to be destroyed after the current test.
	var needDestroy bool
	var resAlloc resourceAllocation // The resource allocation associated with the current cluster.

	allocateCluster := func(
		local bool, existingClusterName string, artifactsDir string, t testSpec,
	) (*cluster, error) {
		var name string
		if existingClusterName != "" {
			name = existingClusterName
		} else {
			// If local is set, the name needs to be empty.
			if !local {
				name = clusterID
				if name == "" {
					name = fmt.Sprintf("%d", timeutil.Now().Unix())
				}
				name += "-" + t.Name
				if len(name) > GCEClusterNameLimit {
					name = name[:GCEClusterNameLimit]
					if name[len(name)-1] == '-' {
						name = name[:len(name)-1]
					}
				}
				if err := r.verifyClusterName(name); err != nil {
					return nil, err
				}
			}
		}

		// Logs for creating a new cluster or attaching to a cluster go to an
		// dedicated log file.
		logPath := filepath.Join(artifactsDir, fmt.Sprintf("cluster-create-%s.log", name))
		l, err := rootLogger(logPath, teeOpt)
		if err != nil {
			log.Fatal(ctx, err)
		}

		if existingClusterName != "" {
			opt := attachOpt{
				skipValidation: r.config.skipClusterValidationOnAttach,
				skipStop:       r.config.skipClusterStopOnAttach,
				skipWipe:       r.config.skipClusterWipeOnAttach,
			}
			return attachToExistingCluster(ctx, existingClusterName, l, t.Nodes, opt)
		}
		cfg := clusterConfig{
			name:         name,
			nodes:        t.Nodes,
			artifactsDir: artifactsDir,
			localCluster: local,
		}
		return newCluster(ctx, l, cfg)
	}

	destroy := func() {
		if c == nil {
			return
		}
		c.Destroy(ctx)
		if resAlloc != (resourceAllocation{}) {
			resAlloc.Release()
		}
		c = nil
		clusterSpec = nil
		tag = ""
		needDestroy = false
	}

	// When this method returns we'll destroy the cluster we had at the time,
	// unless we're exiting with errTestFailureAndDebugEnabled; in that case we
	// want the cluster to stick around.
	defer func() {
		if _, ok := retErr.(errTestFailureAndDebugEnabled); !ok {
			destroy()
		} else {
			log.Error(ctx, retErr.Error())
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

		if needDestroy {
			destroy()
		} else if c != nil {
			// We wipe clusters before reusing.
			c.Wipe(ctx)
		}
		var resLock resourceLock
		if rg != nil {
			resLock = rg.Lock()
		}
		testToRun := work.testToRun(clusterSpec, tag)
		// If we have a cluster but can't reuse it for the test we've selected,
		// destroy it.
		if !testToRun.canReuseCluster && c != nil {
			destroy()
		}
		if testToRun.noWork {
			return nil
		}
		// Create a cluster, if we no longer have one.
		if c == nil {
			var err error
			// Acquire resources for the cluster we're about to create.
			if rg != nil {
				cpu := 0
				for _, n := range testToRun.spec.Nodes {
					cpu += n.Count * n.CPUs
				}
				// Loop until the resource acquisition succeeds.
				// NOTE: What we're doing here is quite simplistic - we first select a
				// test to run and then block the worker until there are enough
				// resources to run it. There might be resources to run smaller tests,
				// and that might be beneficial, so more complex schemes could be
				// useful.
				for {
					resAlloc, err = resLock.AllocateCPU(cpu)
					if err != nil {
						return err
					}
					if resAlloc == (resourceAllocation{}) {
						resLock.Wait()
					} else {
						// The resources will be released when the cluster is destroyed.
						break
					}
				}
				resLock.Unlock()
			}
			c, err = allocateCluster(local, clusterName, artifactsDir, testToRun.spec)
			if err != nil {
				return err
			}
		}
		// Look at the test we're about to run and remember what's supposed to
		// happen to our cluster after.
		if testToRun.spec.ClusterReusePolicy == NoReuse {
			needDestroy = true
		} else if testToRun.spec.ClusterReusePolicy.policy == onlyTagged {
			// Our cluster is now tagged.
			tag = testToRun.spec.ClusterReusePolicy.tag
		}

		// Prepare the test's logger.
		artifactsSuffix := "run_" + strconv.Itoa(testToRun.runNum)
		artifactsDir := filepath.Join(
			artifactsDir, teamCityNameEscape(testToRun.spec.Name), artifactsSuffix)
		logPath := filepath.Join(artifactsDir, "test.log")
		l, err := rootLogger(logPath, teeOpt)
		if err != nil {
			return err
		}
		t := &test{
			spec:         &testToRun.spec,
			registry:     r,
			artifactsDir: artifactsDir,
			l:            l,
		}
		// Tell the cluster that, from now on, it will be run "on behalf of this
		// test".
		c.setTest(t)

		// Now run the test.
		err = r.runTest(ctx, t, c, artifactsDir, teeOpt)
		if err != nil {
			log.Errorf(ctx, "test returned error: %s: %s", t.Name(), err)
			// Mark the test as failed if it isn't already.
			if !t.Failed() {
				t.printAndFail(err)
			}
		}
		// If a test failed and debug was set, we bail.
		if (err != nil || t.Failed()) && debug {
			// TODO(andrei): Get a failure message for the t.Failed() case.
			failureMsg := ""
			if err != nil {
				failureMsg = err.Error()
			}
			// TODO(andrei): debug is specified, so we have to leak a cluster. Does
			// that mean that we need to stop running tests? Probably not, at least
			// not unless we get really low on resources because of these leaks. We
			// should implement a better policy.
			return newErrTestFailureAndDebugEnabled(t.Name(), failureMsg)
		}
	}
}

// An error is returned in exceptional situations. The cluster cannot be reused
// if an error is returned.
//
// Args:
// c: The cluster on which the test will run. runTest() does not wipe or destroy
//    the cluster.
func (r *registry) runTest(
	ctx context.Context, t *test, c *cluster, artifactsDir string, teeOpt teeOptType,
) error {
	if t.spec.Skip != "" {
		return fmt.Errorf("Can't run skipped test: %s: %s", t.Name(), t.spec.Skip)
	}

	if teamCity {
		fmt.Printf("##teamcity[testStarted name='%s' flowId='%s']\n", t.Name(), t.Name())
	} else {
		var details []string
		if !t.spec.Stable {
			details = append(details, "unstable")
		}
		var detail string
		if len(details) > 0 {
			detail = fmt.Sprintf(" [%s]", strings.Join(details, ","))
		}
		fmt.Fprintf(r.out, "=== RUN   %s%s\n", t.Name(), detail)
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
			t.mu.output = append(t.mu.output, t.decorate(fmt.Sprint(err))...)
			t.mu.Unlock()
		}

		t.mu.Lock()
		t.mu.done = true
		t.mu.Unlock()

		dstr := fmt.Sprintf("%.2fs", t.duration().Seconds())
		stability := ""
		if !t.spec.Stable {
			stability = "[unstable] "
		}

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

			fmt.Fprintf(r.out, "--- FAIL: %s %s(%s)\n%s", t.Name(), stability, dstr, output)
			if postIssues && issues.CanPost() && t.spec.Run != nil {
				authorEmail := getAuthorEmail(failLoc.file, failLoc.line)
				branch := "<unknown branch>"
				if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
					branch = b
				}
				if err := issues.Post(
					context.Background(),
					fmt.Sprintf("roachtest: %s failed", t.Name()),
					"roachtest", t.Name(), "The test failed on "+branch+":\n"+string(output), authorEmail,
				); err != nil {
					fmt.Fprintf(r.out, "failed to post issue: %s\n", err)
				}
			}
		} else {
			fmt.Fprintf(r.out, "--- PASS: %s %s(%s)\n", t.Name(), stability, dstr)
			// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
			// TeamCity regards the test as successful.
		}

		if teamCity {
			fmt.Fprintf(r.out, "##teamcity[testFinished name='%s' flowId='%s']\n", t.Name(), t.Name())

			escapedTestName := teamCityNameEscape(t.Name())
			artifactsGlobPath := filepath.Join(artifactsDir, escapedTestName, "**")
			artifactsSpec := fmt.Sprintf("%s => %s", artifactsGlobPath, escapedTestName)
			fmt.Fprintf(r.out, "##teamcity[publishArtifacts '%s']\n", artifactsSpec)
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
	}()

	t.start = timeutil.Now()

	timeout := time.Hour
	defer func() {
		if err := c.FetchLogs(ctx); err != nil {
			c.l.Printf("failed to download logs: %s", err)
		}
	}()

	timeout = c.expiration.Add(-10 * time.Minute).Sub(timeutil.Now())
	if timeout <= 0 {
		err := fmt.Errorf("cluster expired (%s)", timeout)
		return err
	}

	if t.spec.Timeout > 0 && timeout > t.spec.Timeout {
		timeout = t.spec.Timeout
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
	done := make(chan struct{})
	go func() {
		t.spec.Run(runCtx, t, c)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		// We hit a timeout. We're going to mark the test as failed (which will
		// also cancel its context). If --debug was specified, we return an error
		// telling the caller to leave the cluster alone.
		// Otherwise, we'll wait another 5 minutes in the hope that the test
		// reacts either to the ctx cancelation or to the fact that it was marked
		// as failed. If that happens, great - we return normally and so the
		// cluster can be reused. It the test does not react to anything, then we
		// return an error, which will cause the caller to stop everything and
		// this destroy this cluster (as well as all the others). Since the
		// cluster cannot be reused in this case, it'd be awkward for the caller
		// to continue.

		msg := fmt.Sprintf("test timed out (%s)", timeout)
		t.printfAndFail("%s\n", msg)
		if debugEnabled {
			return newErrTestFailureAndDebugEnabled(t.Name(), msg)
		}

		select {
		case <-done:
		case <-time.After(5 * time.Minute):
			return fmt.Errorf("test unresponsive after timeout")
		}
	}
	return nil
}
