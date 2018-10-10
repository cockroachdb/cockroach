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
	version "github.com/hashicorp/go-version"
	"github.com/petermattis/goid"
)

var (
	parallelism   = 10
	count         = 1
	debugEnabled  = false
	postIssues    = true
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

type testSpec struct {
	Skip string // if non-empty, test will be skipped
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
	// Stable indicates whether failure of the test will result in failure of the
	// test run. Tests should be added initially as unstable, and only converted
	// to stable once they have passed successfully on multiple nightly (not
	// local) runs.
	Stable bool
	// Nodes provides the specification for the cluster to use for the test. Only
	// a top-level testSpec may contain a nodes specification. The cluster is
	// shared by all subtests.
	Nodes []nodeSpec
	// A testSpec must specify only one of Run or SubTests. All subtests run in
	// the same cluster, without concurrency between them. Subtest should not
	// assume any particular state for the cluster as the SubTest may be run in
	// isolation.
	Run      func(ctx context.Context, t *test, c *cluster)
	SubTests []testSpec
}

// matchRegex returns true if the regex matches the test's name or any of the
// subtest names.
func (t *testSpec) matchRegex(re *regexp.Regexp) bool {
	if re.MatchString(t.Name) {
		return true
	}
	for i := range t.SubTests {
		if t.SubTests[i].matchRegex(re) {
			return true
		}
	}
	return false
}

func (t *testSpec) matchRegexRecursive(re *regexp.Regexp) []testSpec {
	var res []testSpec
	if re.MatchString(t.Name) {
		res = append(res, *t)
	}
	for i := range t.SubTests {
		res = append(res, t.SubTests[i].matchRegexRecursive(re)...)
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

func (r *registry) verifyClusterName(testName string) error {
	// TeamCity build IDs are currently 6 digits, but we use 7 here for a bit of
	// breathing room.
	name := makeGCEClusterName(testName, "xxxxxxx", "teamcity")
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

func (r *registry) prepareSpec(spec *testSpec, depth int) error {
	if depth == 0 {
		spec.subtestName = spec.Name
		// Only top-level tests can create clusters, so those are the only ones for
		// which we need to verify the cluster name.
		if err := r.verifyClusterName(spec.Name); err != nil {
			return err
		}
	}

	if (spec.Run != nil) == (len(spec.SubTests) > 0) {
		return fmt.Errorf("%s: must specify only one of Run or SubTests", spec.Name)
	}

	if spec.Run == nil && spec.Timeout > 0 {
		return fmt.Errorf("%s: timeouts only apply to tests specifying Run", spec.Name)
	}

	if depth > 0 && len(spec.Nodes) > 0 {
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
	if err := r.prepareSpec(&spec, 0); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
	r.m[spec.Name] = &spec
}

// ListTopLevel lists the top level tests that match re, or that have a subtests
// that matches re.
func (r *registry) ListTopLevel(re *regexp.Regexp) []*testSpec {
	var results []*testSpec
	for _, t := range r.m {
		if t.matchRegex(re) {
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
func (r *registry) ListAll(filter []string) []string {
	filterRE := makeFilterRE(filter)
	var tests []testSpec
	for _, t := range r.m {
		tests = append(tests, t.matchRegexRecursive(filterRE)...)
	}
	var names []string
	for _, t := range tests {
		if t.Skip == "" && t.minVersion != nil {
			if r.buildVersion.LessThan(t.minVersion) {
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

func (r *registry) Run(filter []string, parallelism int) int {
	filterRE := makeFilterRE(filter)
	// Find the top-level tests to run.
	tests := r.ListTopLevel(filterRE)

	// Skip any tests for which the min-version is less than the build-version.
	for _, t := range tests {
		if t.Skip == "" && t.minVersion != nil {
			if r.buildVersion.LessThan(t.minVersion) {
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
				r.runAsync(
					ctx, tests[i], filterRE, nil /* parent */, nil /* cluster */, runNum, func(failed bool) {
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

			stableFails := 0
			for t := range r.status.fail {
				if t.spec.Stable {
					stableFails++
				}
			}
			if stableFails > 0 {
				fmt.Fprintln(r.out, "FAIL")
				return 1
			}
			unstableFails := ""
			if n := len(r.status.fail) - stableFails; n > 0 {
				unstableFails = fmt.Sprintf(" (%d unstable FAIL)", n)
			}
			fmt.Fprintf(r.out, "PASS%s\n", unstableFails)
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
				destroyAllClusters()
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
	runner   string
	runnerID int64
	start    time.Time
	end      time.Time
	// artifactsDir is the path to the directory holding all the artifacts for
	// this test. It will contain a test.log file, cluster logs, and
	// subdirectories for subtests.
	artifactsDir string
	mu           struct {
		syncutil.RWMutex
		done    bool
		failed  bool
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
}

func (t *test) printfAndFail(format string, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.output = append(t.mu.output, t.decorate(fmt.Sprintf(format, args...))...)
	t.mu.failed = true
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
	filter *regexp.Regexp,
	parent *test,
	c *cluster,
	runNum int,
	done func(failed bool),
) {
	artifactsSuffix := ""
	// Only roots get a "run_<n>" suffix to their artifacts dir; the subtests will
	// write in the parent's dir.
	if parent == nil && runNum != 0 {
		artifactsSuffix = "run_" + strconv.Itoa(runNum)
	}
	parentDir := artifacts
	if parent != nil {
		parentDir = parent.ArtifactsDir()
	}
	// Each subtest gets its own subdir in the parent's artifacts dir.
	artifactsDir := filepath.Join(parentDir, teamCityNameEscape(spec.subtestName), artifactsSuffix)

	t := &test{
		spec:         spec,
		registry:     r,
		artifactsDir: artifactsDir,
	}

	if teamCity {
		fmt.Printf("##teamcity[testStarted name='%s' flowId='%s']\n", t.Name(), t.Name())
	} else {
		var details []string
		if !t.spec.Stable {
			details = append(details, "unstable")
		}
		if t.spec.Skip != "" {
			details = append(details, "skip")
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

	go func() {
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
			} else if t.spec.Skip == "" {
				fmt.Fprintf(r.out, "--- PASS: %s %s(%s)\n", t.Name(), stability, dstr)
				// If `##teamcity[testFailed ...]` is not present before `##teamCity[testFinished ...]`,
				// TeamCity regards the test as successful.
			} else {
				if teamCity {
					fmt.Fprintf(r.out, "##teamcity[testIgnored name='%s' message='%s']\n",
						t.Name(), t.spec.Skip)
				}
				fmt.Fprintf(r.out, "--- SKIP: %s (%s)\n\t%s\n", t.Name(), dstr, t.spec.Skip)
			}

			if teamCity {
				fmt.Fprintf(r.out, "##teamcity[testFinished name='%s' flowId='%s']\n", t.Name(), t.Name())

				escapedTestName := teamCityNameEscape(t.Name())
				artifactsGlobPath := filepath.Join(artifacts, escapedTestName, "**")
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

			done(t.Failed())
		}()

		t.start = timeutil.Now()

		if t.spec.Skip != "" {
			return
		}

		if c == nil {
			if clusterName == "" {
				opt := remoteCluster
				if local {
					opt = localCluster
				}
				var err error
				c, err = newCluster(ctx, t, t.spec.Nodes, opt)
				FatalIfErr(t, err)
			} else {
				opt := attachOpt{
					skipValidation: r.config.skipClusterValidationOnAttach,
					skipStop:       r.config.skipClusterStopOnAttach,
					skipWipe:       r.config.skipClusterWipeOnAttach,
				}
				c = attachToExistingCluster(ctx, clusterName, t, t.spec.Nodes, opt)
			}
			if c != nil {
				defer func() {
					if !debugEnabled || !t.Failed() {
						c.Destroy(ctx)
					} else {
						c.l.Printf("not destroying cluster to allow debugging\n")
					}
				}()
			}
		} else {
			c = c.clone(t)
		}

		// If we have subtests, handle them here and return.
		if t.spec.Run == nil {
			for i := range t.spec.SubTests {
				if t.spec.SubTests[i].matchRegex(filter) {
					var wg sync.WaitGroup
					wg.Add(1)
					r.runAsync(ctx, &t.spec.SubTests[i], filter, t, c, runNum, func(failed bool) {
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

		timeout := time.Hour
		defer func() {
			if err := c.FetchLogs(ctx); err != nil {
				c.l.Printf("failed to download logs: %s", err)
			}
		}()

		timeout = c.expiration.Add(-10 * time.Minute).Sub(timeutil.Now())
		if timeout <= 0 {
			t.spec.Skip = fmt.Sprintf("cluster expired (%s)", timeout)
			return
		}

		if t.spec.Timeout > 0 && timeout > t.spec.Timeout {
			timeout = t.spec.Timeout
		}

		done := make(chan struct{})
		defer func() {
			close(done)
		}()

		runCtx, cancel := context.WithCancel(ctx)

		go func() {
			defer cancel()

			select {
			case <-time.After(timeout):
				t.printfAndFail("test timed out (%s)\n", timeout)
				if err := c.FetchLogs(ctx); err != nil {
					c.l.Printf("failed to download logs: %s", err)
				}
				// NB: c.destroyed is nil for cloned clusters (i.e. in subtests).
				if !debugEnabled && c.destroyed != nil {
					c.Destroy(ctx)
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
