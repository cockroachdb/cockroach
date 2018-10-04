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
	// For the debug http handlers.
	_ "net/http/pprof"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/petermattis/goid"
)

// !!!
//type reusePolicy int
//
//const (
//	// A sentinel so that we can validate that all the tests set a policy.
//	_ reusePolicy = iota
//	noReuse
//	onlyTagged
//	any
//)

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

type testStatus struct {
	msg      string
	time     time.Time
	progress float64
}

type test struct {
	spec *testSpec

	// buildVersion is the version of the Cockroach binary that the test will run
	// against.
	buildVersion version.Version

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
//
// ATTENTION: Since this calls runtime.GoExit(), it should only be called from a
// test's closure. The test runner itself should never call this.
func (t *test) Fatal(args ...interface{}) {
	t.fatalfInner("" /* format */, args...)
}

// Fatalf is like Fatal, but takes a format string.
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
	return t.buildVersion.AtLeast(vers)
}

// errTestFailureAndDebugEnabled represents a test failure when --debug was
// specified.
type errTestFailureAndDebugEnabled struct {
	test        string
	c           *cluster
	testFailure string
}

var _ = (*test)(nil).IsBuildVersion // avoid unused lint

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

// decTest decrements a test's remaining count and removes it
// from the workPool if it was exhausted.
func (p *workPool) decTestLocked(ctx context.Context, name string) {
	idx := -1
	for idx = range p.mu.tests {
		if p.mu.tests[idx].spec.Name == name {
			break
		}
	}
	if idx == -1 {
		log.Fatalf(ctx, "failed to find test: %s", name)
	}
	tc := &p.mu.tests[idx]
	tc.count--
	if tc.count == 0 {
		// We've selected the last run for a test. Take that test out of the pool.
		p.mu.tests = append(p.mu.tests[:idx], p.mu.tests[idx+1:]...)
	}
}

type clusterType int

const (
	localCluster clusterType = iota
	roachprodCluster
)

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
