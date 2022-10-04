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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
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
		done bool

		// cancel, if set, is called from the t.Fatal() family of functions when the
		// test is being marked as failed (i.e. when the failed field above is also
		// set). This is used to cancel the context passed to t.spec.Run(), so async
		// test goroutines can be notified.
		cancel  func()
		failLoc struct {
			file string
			line int
		}

		// Errors are all the errors passed to `addFailure`, in order of
		// these calls.
		//
		// NB: the first failure is not always the relevant one due to:
		// https://github.com/cockroachdb/cockroach/issues/44436
		errors []error

		// status is a map from goroutine id to status set by that goroutine. A
		// special goroutine is indicated by runnerID; that one provides the test's
		// "main status".
		status map[int64]testStatus

		// TODO(test-eng): this should just be an in-mem (ring) buffer attached to
		// `t.L()`.
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
		return fmt.Sprintf("%s (set %s ago)", status.msg, timeutil.Since(status.time).Round(time.Second))
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
// the main test status message. When called from the main test goroutine
// (i.e. the goroutine on which TestSpec.Run is invoked), this is equivalent to
// calling WorkerProgress.
func (t *testImpl) Progress(frac float64) {
	t.progress(t.runnerID, frac)
}

// WorkerProgress sets the progress (a fraction in the range [0,1]) associated
// with the a worker status message.
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

// This creates an error from the first arg, and adds each subsequent arg
// as error detail
func argsToErr(depth int, args ...interface{}) error {
	// NB: we'd probably not allow multiple arguments here and we'd want
	// the one remaining arg to be an `error`, but we are trying to be
	// compatible with `(*testing.T).Fatal`.
	var err error
	for _, arg := range args {
		if err == nil {
			err = errors.NewWithDepthf(depth+1, "%v", arg)
			continue
		}
		err = errors.WithDetailf(err, "%v", arg)
	}
	return err
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
	t.addFailure(argsToErr(1, args...))
	panic(errTestFatal)
}

// Fatalf is like Fatal, but takes a format string.
func (t *testImpl) Fatalf(format string, args ...interface{}) {
	t.addFailure(errors.NewWithDepthf(1, format, args...))
	panic(errTestFatal)
}

// FailNow implements the TestingT interface.
func (t *testImpl) FailNow() {
	t.addFailure(errors.NewWithDepthf(1, "FailNow called"))
	panic(errTestFatal)
}

func (t *testImpl) Error(args ...interface{}) {
	t.addFailure(argsToErr(1, args...))
}

// Errorf implements the TestingT interface.
func (t *testImpl) Errorf(format string, args ...interface{}) {
	t.addFailure(errors.NewWithDepthf(1, format, args...))
}

func formatFailure(b *strings.Builder, errs ...error) {
	for i, err := range errs {
		if i > 0 {
			fmt.Fprintln(b)
		}
		file, line, fn, ok := errors.GetOneLineSource(err)
		if !ok {
			file, line, fn = "<unknown>", 0, "unknown"
		}
		fmt.Fprintf(b, "(%s:%d).%s: %v", file, line, fn, err)
	}
}

func (t *testImpl) addFailure(reportErr error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.mu.errors = append(t.mu.errors, reportErr)

	var b strings.Builder
	formatFailure(&b, reportErr)
	msg := b.String()

	t.L().Printf("test failure #%d: %s", len(t.mu.errors), msg)
	// Also dump the verbose error (incl. all stack traces) to a log file, in case
	// we need it. The stacks are sometimes helpful, but we don't want them in the
	// main log as they are highly verbose.
	{
		cl, err := t.L().ChildLogger(
			fmt.Sprintf("failure_%d", len(t.mu.errors)),
			logger.QuietStderr, logger.QuietStdout,
		)
		if err == nil {
			// We don't actually log through this logger since it adds an unrelated
			// file:line caller (namely ours). The error already has stack traces
			// so it's better to write only it to the file to avoid confusion.
			path := cl.File.Name()
			cl.Close() // we just wanted the filename
			_ = os.WriteFile(path, []byte(fmt.Sprintf("%+v", reportErr)), 0644)
		}
	}

	t.mu.output = append(t.mu.output, msg...)
	t.mu.output = append(t.mu.output, '\n')
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

func (t *testImpl) duration() time.Duration {
	return t.end.Sub(t.start)
}

func (t *testImpl) Failed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.failedRLocked()
}

func (t *testImpl) failedRLocked() bool {
	return len(t.mu.errors) > 0
}

func (t *testImpl) FailureMsg() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var b strings.Builder
	formatFailure(&b, t.mu.errors...)
	return b.String()
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
	// path to the literal on-agent directory where artifacts are stored. May
	// be different from artifactsDir since the roachtest may be running in
	// a container.
	literalArtifactsDir string
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
