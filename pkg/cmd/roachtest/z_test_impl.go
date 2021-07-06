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
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/petermattis/goid"
)

// perfArtifactsDir is the directory on cluster nodes in which perf artifacts
// reside. Upon success this directory is copied into test artifactsDir from
// each node in the cluster.
const perfArtifactsDir = "perf"

type testStatus struct {
	msg      string
	time     time.Time
	progress float64
}

type testImpl struct {
	spec *registry.TestSpec

	cockroach          string // path to main cockroach binary
	deprecatedWorkload string // path to workload binary
	// buildVersion is the version of the Cockroach binary that the test will run
	// against.
	buildVersion version.Version

	// l is the logger that the test will use for its output.
	l *logger.Logger

	runner string
	// runnerID is the test's main goroutine ID.
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
		// status is a map from goroutine id to status set by that goroutine. A
		// special goroutine is indicated by runnerID; that one provides the test's
		// "main status".
		status map[int64]testStatus
		output []byte
	}
	// Map from version to path to the cockroach binary to be used when
	// mixed-version test wants a binary for that binary. If a particular version
	// <ver> is found in this map, it is used instead of the binary coming from
	// `roachprod stage release <ver>`. See the --version-binary-override flags.
	//
	// Version strings look like "20.1.4".
	versionsBinaryOverride map[string]string
}

// BuildVersion exposes the build version of the cluster
// in this test.
func (t *testImpl) BuildVersion() *version.Version {
	return &t.buildVersion
}

func (t *testImpl) Cockroach() string {
	return t.cockroach
}

func (t *testImpl) DeprecatedWorkload() string {
	return t.deprecatedWorkload
}

func (t *testImpl) VersionsBinaryOverride() map[string]string {
	return t.versionsBinaryOverride
}

// Spec returns the TestSpec.
func (t *testImpl) Spec() interface{} {
	return t.spec
}

func (t *testImpl) Helper() {}

func (t *testImpl) Name() string {
	return t.spec.Name
}

// L returns the test's logger.
func (t *testImpl) L() *logger.Logger {
	return t.l
}

// ReplaceL replaces the test's logger.
func (t *testImpl) ReplaceL(l *logger.Logger) {
	// TODO(tbg): get rid of this, this is racy & hacky.
	t.l = l
}

func (t *testImpl) status(ctx context.Context, id int64, args ...interface{}) {
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
	if !t.L().Closed() {
		if id == t.runnerID {
			t.L().PrintfCtxDepth(ctx, 3, "test status: %s", msg)
		} else {
			t.L().PrintfCtxDepth(ctx, 3, "test worker status: %s", msg)
		}
	}
}

// Status sets the main status message for the test. When called from the main
// test goroutine (i.e. the goroutine on which TestSpec.Run is invoked), this
// is equivalent to calling WorkerStatus. If no arguments are specified, the
// status message is erased.
func (t *testImpl) Status(args ...interface{}) {
	t.status(context.TODO(), t.runnerID, args...)
}

// GetStatus returns the status of the tests's main goroutine.
func (t *testImpl) GetStatus() string {
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
func (t *testImpl) WorkerStatus(args ...interface{}) {
	t.status(context.TODO(), goid.Get(), args...)
}

func (t *testImpl) progress(id int64, frac float64) {
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
// (i.e. the goroutine on which TestSpec.Run is invoked), this is equivalent to
// calling WorkerProgress.
func (t *testImpl) Progress(frac float64) {
	t.progress(t.runnerID, frac)
}

// WorkerProgress sets the progress (a fraction in the range [0,1]) associated
// with the a worker status messasge.
func (t *testImpl) WorkerProgress(frac float64) {
	t.progress(goid.Get(), frac)
}

var _ skip.SkippableTest = (*testImpl)(nil)

// Skip skips the test. The first argument if any is the main message.
// The remaining argument, if any, form the details.
// This implements the skip.SkippableTest interface.
func (t *testImpl) Skip(args ...interface{}) {
	if len(args) > 0 {
		t.spec.Skip = fmt.Sprint(args[0])
		args = args[1:]
	}
	t.spec.SkipDetails = fmt.Sprint(args...)
	panic(errTestFatal)
}

// Skipf skips the test. The formatted message becomes the skip reason.
// This implements the skip.SkippableTest interface.
func (t *testImpl) Skipf(format string, args ...interface{}) {
	t.spec.Skip = fmt.Sprintf(format, args...)
	panic(errTestFatal)
}

// Fatal marks the test as failed, prints the args to t.L(), and calls
// panic(errTestFatal). It can be called multiple times.
//
// If the only argument is an error, it is formatted by "%+v", so it will show
// stack traces and such.
//
// ATTENTION: Since this calls panic(errTestFatal), it should only be called
// from a test's closure. The test runner itself should never call this.
func (t *testImpl) Fatal(args ...interface{}) {
	t.fatalfInner("" /* format */, args...)
}

// Fatalf is like Fatal, but takes a format string.
func (t *testImpl) Fatalf(format string, args ...interface{}) {
	t.fatalfInner(format, args...)
}

// FailNow implements the TestingT interface.
func (t *testImpl) FailNow() {
	t.Fatal()
}

// Errorf implements the TestingT interface.
func (t *testImpl) Errorf(format string, args ...interface{}) {
	t.Fatalf(format, args...)
}

func (t *testImpl) fatalfInner(format string, args ...interface{}) {
	// Skip two frames: our own and the caller.
	if format != "" {
		t.printfAndFail(2 /* skip */, format, args...)
	} else {
		t.printAndFail(2 /* skip */, args...)
	}
	panic(errTestFatal)
}

func (t *testImpl) printAndFail(skip int, args ...interface{}) {
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

func (t *testImpl) printfAndFail(skip int, format string, args ...interface{}) {
	if format == "" {
		panic(fmt.Sprintf("invalid empty format. args: %s", args))
	}
	t.failWithMsg(t.decorate(skip+1, fmt.Sprintf(format, args...)))
}

func (t *testImpl) failWithMsg(msg string) {
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
	t.L().Printf("%stest failure: %s", prefix, msg)

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
func (t *testImpl) decorate(skip int, s string) string {
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

func (t *testImpl) duration() time.Duration {
	return t.end.Sub(t.start)
}

func (t *testImpl) Failed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failed
}

func (t *testImpl) FailureMsg() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failureMsg
}

func (t *testImpl) ArtifactsDir() string {
	return t.artifactsDir
}

func (t *testImpl) PerfArtifactsDir() string {
	return perfArtifactsDir
}

// IsBuildVersion returns true if the build version is greater than or equal to
// minVersion. This allows a test to optionally perform additional checks
// depending on the cockroach version it is running against. Note that the
// versions are Cockroach build tag version numbers, not the internal cluster
// version number.
func (t *testImpl) IsBuildVersion(minVersion string) bool {
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
	return t.BuildVersion().AtLeast(vers)
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

type testWithCount struct {
	spec registry.TestSpec
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
	l *logger.Logger
	// tee controls whether test logs (not test runner logs) also go to stdout or
	// not.
	tee            logger.TeeOptType
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
		t   *testImpl
		c   *clusterImpl
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

func (w *workerStatus) Cluster() *clusterImpl {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.c
}

func (w *workerStatus) SetCluster(c *clusterImpl) {
	w.mu.Lock()
	w.mu.c = c
	w.mu.Unlock()
}

func (w *workerStatus) TestToRun() testToRunRes {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.ttr
}

func (w *workerStatus) Test() *testImpl {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mu.t
}

func (w *workerStatus) SetTest(t *testImpl, ttr testToRunRes) {
	w.mu.Lock()
	w.mu.t = t
	w.mu.ttr = ttr
	w.mu.Unlock()
}

// shout logs a message both to a logger and to an io.Writer.
// If format doesn't end with a new line, one will be automatically added.
func shout(
	ctx context.Context, l *logger.Logger, stdout io.Writer, format string, args ...interface{},
) {
	if len(format) == 0 || format[len(format)-1] != '\n' {
		format += "\n"
	}
	msg := fmt.Sprintf(format, args...)
	l.PrintfCtxDepth(ctx, 2 /* depth */, msg)
	fmt.Fprint(stdout, msg)
}
