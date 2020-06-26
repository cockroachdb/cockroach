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
	"fmt"
	"io"
	// For the debug http handlers.
	_ "net/http/pprof"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/petermattis/goid"
)

type testSpec struct {
	Skip string // if non-empty, test will be skipped
	// When Skip is set, this can contain more text to be printed in the logs
	// after the "--- SKIP" line.
	SkipDetails string

	Name string
	// Owner is the name of the team responsible for signing off on failures of
	// this test that happen in the release process. This must be one of a limited
	// set of values (the keys in the roachtestTeams map).
	Owner Owner
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

// perfArtifactsDir is the directory on cluster nodes in which perf artifacts
// reside. Upon success this directory is copied into test artifactsDir from
// each node in the cluster.
const perfArtifactsDir = "perf"

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

	// artifactsDir is the path to the directory holding all the artifacts for
	// this test. It will contain a test.log file and cluster logs.
	artifactsDir string
	// artifactsSpec is a TeamCity artifacts spec used to publish this test's
	// artifacts. See:
	// https://www.jetbrains.com/help/teamcity/2019.1/configuring-general-settings.html#Artifact-Paths
	artifactsSpec string

	mu struct {
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

func (t *test) status(ctx context.Context, id int64, args ...interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mu.status == nil {
		t.mu.status = make(map[int64]testStatus)
	}
	if len(args) == 0 {
		delete(t.mu.status, id)
		return
	}
	msg := fmt.Sprint(args...)
	t.mu.status[id] = testStatus{
		msg:  msg,
		time: timeutil.Now(),
	}
	if !t.l.closed() {
		if id == t.runnerID {
			t.l.PrintfCtxDepth(ctx, 2, "test status: %s", msg)
		} else {
			t.l.PrintfCtxDepth(ctx, 2, "test worker status: %s", msg)
		}
	}
}

// Status sets the main status message for the test. When called from the main
// test goroutine (i.e. the goroutine on which testSpec.Run is invoked), this
// is equivalent to calling WorkerStatus. If no arguments are specified, the
// status message is erased.
func (t *test) Status(args ...interface{}) {
	t.status(context.TODO(), t.runnerID, args...)
}

// GetStatus returns the status of the tests's main goroutine.
func (t *test) GetStatus() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, ok := t.mu.status[t.runnerID]
	if ok {
		return fmt.Sprintf("%s (set %s ago)", status.msg, timeutil.Now().Sub(status.time).Round(time.Second))
	}
	return "N/A"
}

// WorkerStatus sets the status message for a worker goroutine associated with
// the test. The status message should be cleared before the goroutine exits by
// calling WorkerStatus with no arguments.
func (t *test) WorkerStatus(args ...interface{}) {
	t.status(context.TODO(), goid.Get(), args...)
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

// Skip records msg into t.spec.Skip and calls panic(errTestFatal) - thus
// interrupting the running of the test.
func (t *test) Skip(msg string, details string) {
	t.spec.Skip = msg
	t.spec.SkipDetails = details
	panic(errTestFatal)
}

// Fatal marks the test as failed, prints the args to t.l, and calls
// panic(errTestFatal). It can be called multiple times.
//
// If the only argument is an error, it is formatted by "%+v", so it will show
// stack traces and such.
//
// ATTENTION: Since this calls panic(errTestFatal), it should only be called
// from a test's closure. The test runner itself should never call this.
func (t *test) Fatal(args ...interface{}) {
	t.fatalfInner("" /* format */, args...)
}

// Fatalf is like Fatal, but takes a format string.
func (t *test) Fatalf(format string, args ...interface{}) {
	t.fatalfInner(format, args...)
}

// FailNow implements the TestingT interface.
func (t *test) FailNow() {
	t.Fatal()
}

// Errorf implements the TestingT interface.
func (t *test) Errorf(format string, args ...interface{}) {
	t.Fatalf(format, args...)
}

func (t *test) fatalfInner(format string, args ...interface{}) {
	// Skip two frames: our own and the caller.
	if format != "" {
		t.printfAndFail(2 /* skip */, format, args...)
	} else {
		t.printAndFail(2 /* skip */, args...)
	}
	panic(errTestFatal)
}

// FatalIfErr calls t.Fatal() if err != nil.
func FatalIfErr(t *test, err error) {
	if err != nil {
		t.fatalfInner("" /* format */, err)
	}
}

func (t *test) printAndFail(skip int, args ...interface{}) {
	var msg string
	if len(args) == 1 {
		// If we were passed only an error, then format it with "%+v" in order to
		// get any stack traces.
		if err, ok := args[0].(error); ok {
			msg = fmt.Sprintf("%+v", err)
		}
	}
	if msg == "" {
		msg = fmt.Sprint(args...)
	}
	t.failWithMsg(t.decorate(skip+1, msg))
}

func (t *test) printfAndFail(skip int, format string, args ...interface{}) {
	if format == "" {
		panic(fmt.Sprintf("invalid empty format. args: %s", args))
	}
	t.failWithMsg(t.decorate(skip+1, fmt.Sprintf(format, args...)))
}

func (t *test) failWithMsg(msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	prefix := ""
	if t.mu.failed {
		prefix = "[not the first failure] "
		// NB: the first failure is not always the relevant one due to:
		// https://github.com/cockroachdb/cockroach/issues/44436
		//
		// So we chain all failures together in the order in which we see
		// them.
		msg = "\n" + msg
	}
	t.l.Printf("%stest failure: %s", prefix, msg)

	t.mu.failed = true
	t.mu.failureMsg += msg
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
// string if the author cannot be determined. Some test tags override this
// behavior and have a hardcoded author email.
func getAuthorEmail(tags []string, file string, line int) string {
	for _, tag := range tags {
		if tag == `orm` || tag == `driver` {
			return `rafi@cockroachlabs.com`
		}
	}
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

type testWithCount struct {
	spec testSpec
	// count maintains the number of runs remaining for a test.
	count int
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
	// artifactsDir is that path to the dir that will contain the artifacts for
	// all the tests.
	artifactsDir string
	// runnerLogPath is that path to the runner's log file.
	runnerLogPath string
}

type workerStatus struct {
	// name is the worker's identifier.
	name string
	mu   struct {
		syncutil.Mutex

		// status is presented in the HTML progress page.
		status string

		ttr testToRunRes
		t   *test
		c   *cluster
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

func (w *workerStatus) TestToRun() testToRunRes {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.ttr
}

func (w *workerStatus) Test() *test {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.t
}

func (w *workerStatus) SetTest(t *test, ttr testToRunRes) {
	w.mu.Lock()
	w.mu.t = t
	w.mu.ttr = ttr
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
