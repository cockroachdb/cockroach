// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// perfArtifactsDir is the directory on cluster nodes in which perf artifacts
// reside. Upon success this directory is copied into the test's ArtifactsDir() from
// each node in the cluster.
const perfArtifactsDir = "perf"

// goCoverArtifactsDir is the directory on cluster nodes in which go coverage
// profiles are dumped. At the end of a test this directory is copied into the
// test's ArtifactsDir() from each node in the cluster.
const goCoverArtifactsDir = "gocover"

// cpuProfilesDir is the directory on cluster nodes in which pprof (CPU) profiles
// are dumped. At the end of a test, this directory is copied into the test's
// ArtifactsDir() from each node in the cluster if --force-cpu-profile is set.
const cpuProfilesDir = "pprof_dump"

type testStatus struct {
	msg      string
	time     time.Time
	progress float64
}

// Holds all error information from a single invocation of t.{Fatal,Error}{,f} to
// preserve any structured errors
// e.g. t.Fatalf("foo %s %s %s", "hello", err1, err2) would mean that
// failure.errors == [err1, err2], with all args (including the non error "hello")
// being captured in the squashedErr
type failure struct {
	// This is the single error created from variadic args passed to t.{Fatal,Error}{,f}
	squashedErr error
	// errors are all the `errors` present in the variadic args
	errors []error
}

type testImpl struct {
	spec *registry.TestSpec

	cockroach   string // path to main cockroach binary
	cockroachEA string // path to cockroach binary compiled with --crdb_test build tag

	randomCockroachOnce sync.Once
	randomizedCockroach string // either `cockroach` or `cockroach-ea`, picked randomly

	deprecatedWorkload string // path to workload binary
	debug              bool   // whether the test is in debug mode.
	// buildVersion is the version of the Cockroach binary that the test will run
	// against.
	buildVersion *version.Version

	// l is the logger that the test will use for its output.
	//
	// N.B. We need to use an atomic pointer here since the test
	// runner can swap the logger out when running post test assertions
	// and artifacts collection.
	l atomic.Pointer[logger.Logger]

	// taskManager manages tasks (goroutines) for tests.
	taskManager task.Manager

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
		cancel func()

		// failures added via addFailures, in order
		// A test can have multiple calls to t.Fail()/Error(), with each call
		// referencing 0+ errors. failure captures all the errors
		failures []failure

		// failuresSuppressed indicates if further failures should be added to mu.failures.
		failuresSuppressed bool

		// numFailures is the number of failures that have been added via addFailures.
		// This can deviate from len(failures) if failures have been suppressed.
		numFailures int

		// status is a map from goroutine id to status set by that goroutine. A
		// special goroutine is indicated by runnerID; that one provides the test's
		// "main status".
		status map[int64]testStatus

		// TODO(test-eng): this should just be an in-mem (ring) buffer attached to
		// `t.L()`.
		output []byte

		// extraParams are test-specific parameters that will be added to the Github issue as
		// parameters if there is a failure. They will additionally be logged in the test itself
		// in case github issue posting is disabled.
		extraParams map[string]string
	}
	// Map from version to path to the cockroach binary to be used when
	// mixed-version test wants a binary for that binary. If a particular version
	// <ver> is found in this map, it is used instead of the binary coming from
	// `roachprod stage release <ver>`. See the --versions-binary-override flags.
	//
	// Version strings look like "20.1.4".
	versionsBinaryOverride map[string]string
	skipInit               bool
	// If true, go coverage is enabled and the BAZEL_COVER_DIR env var will be set
	// when starting nodes.
	goCoverEnabled bool
	// If true, the stats exporter will export metrics in openmetrics format.
	// else the exporter will export in the JSON format.
	exportOpenmetrics bool
	// If set, this is used for adding labels to the benchmark metrics
	runID string
}

func newFailure(squashedErr error, errs []error) failure {
	return failure{squashedErr: squashedErr, errors: errs}
}

// BuildVersion exposes the build version of the cluster
// in this test.
func (t *testImpl) BuildVersion() *version.Version {
	return t.buildVersion
}

// Cockroach will return either `RuntimeAssertionsCockroach()` or
// StandardCockroach(), picked based off of the test spec. Once a choice
// has been made, the same binary will be returned on every call to
// Cockroach, to avoid errors that may arise from binaries having a
// different value for metamorphic constants.
func (t *testImpl) Cockroach() string {
	t.randomCockroachOnce.Do(func() {
		switch t.Spec().(*registry.TestSpec).CockroachBinary {
		case registry.RandomizedCockroach:
			// If the test is a benchmark test, we don't want to enable assertions
			// as it will slow down performance.
			if t.spec.Benchmark {
				t.L().Printf("Benchmark test, running with standard cockroach")
				t.randomizedCockroach = t.StandardCockroach()
				return
			}

			if rand.Float64() < roachtestflags.CockroachEAProbability {
				// The build with runtime assertions should exist in every nightly
				// CI build, but we can't assume it exists in every roachtest call.
				if path := t.RuntimeAssertionsCockroach(); path != "" {
					t.L().Printf("Runtime assertions enabled")
					t.randomizedCockroach = path
					return
				} else {
					t.L().Printf("WARNING: running without runtime assertions since the corresponding binary was not specified")
				}
			}
			t.L().Printf("Runtime assertions disabled")
			t.randomizedCockroach = t.StandardCockroach()
		case registry.StandardCockroach:
			t.L().Printf("Runtime assertions disabled: registry.StandardCockroach set")
			t.randomizedCockroach = t.StandardCockroach()
		case registry.RuntimeAssertionsCockroach:
			t.L().Printf("Runtime assertions enabled: registry.RuntimeAssertionsCockroach set")
			t.randomizedCockroach = t.RuntimeAssertionsCockroach()
		default:
			t.Fatal("Specified cockroach binary does not exist.")
		}
	})

	return t.randomizedCockroach
}

func (t *testImpl) ExportOpenmetrics() bool {
	return t.exportOpenmetrics
}

func (t *testImpl) GetRunId() string {
	return t.runID
}

func (t *testImpl) RuntimeAssertionsCockroach() string {
	return t.cockroachEA
}

func (t *testImpl) StandardCockroach() string {
	return t.cockroach
}

func (t *testImpl) DeprecatedWorkload() string {
	// Discourage usage of the deprecated workload by gating it behind the
	// 'RequiresDeprecatedWorkload' test spec. Tests should use 'cockroach
	// workload' instead when possible.
	if !t.spec.RequiresDeprecatedWorkload {
		t.Fatal("Using deprecated workload but `RequiresDeprecatedWorkload` is not set in test spec.")
	}
	return t.deprecatedWorkload
}

func (t *testImpl) VersionsBinaryOverride() map[string]string {
	return t.versionsBinaryOverride
}

func (t *testImpl) SkipInit() bool {
	return t.skipInit
}

// Spec returns the TestSpec.
func (t *testImpl) Spec() interface{} {
	return t.spec
}

func (t *testImpl) Helper() {}

func (t *testImpl) Name() string {
	return t.spec.Name
}

func (t *testImpl) Owner() string {
	return string(t.spec.Owner)
}

func (t *testImpl) SnapshotPrefix() string {
	return t.spec.SnapshotPrefix
}

// L returns the test's logger.
func (t *testImpl) L() *logger.Logger {
	return t.l.Load()
}

// ReplaceL replaces the test's logger.
func (t *testImpl) ReplaceL(l *logger.Logger) {
	t.l.Store(l)
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

// AddParam adds a parameter to the test. This parameter will be logged both in
// the github issue if one is created and in the artifacts directory. This is useful if a test
// has metamorphic properties as it makes it easier to spot the differences between runs
// without digging into the logs (i.e. mixed version test deployment mode). It also helps
// debugging when the test failure is not posted to github (i.e. qualification runs).
func (t *testImpl) AddParam(label, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.extraParams == nil {
		t.mu.extraParams = make(map[string]string)
	}
	t.mu.extraParams[label] = value
}

func (t *testImpl) getExtraParams() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.extraParams
}

// IsDebug returns true if the test is in a debug state.
func (t *testImpl) IsDebug() bool {
	return t.debug
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

// collectErrors extracts any arg that is an error
func collectErrors(args []interface{}) []error {
	var errs []error
	for _, a := range args {
		if err, ok := a.(error); ok {
			errs = append(errs, err)
		}
	}
	return errs
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
	t.addFailureAndCancel(1, "", args...)
	panic(errTestFatal)
}

// Fatalf is like Fatal, but takes a format string.
func (t *testImpl) Fatalf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
	panic(errTestFatal)
}

// FailNow implements the TestingT interface.
func (t *testImpl) FailNow() {
	t.addFailureAndCancel(1, "FailNow called")
	panic(errTestFatal)
}

// Error implements the TestingT interface
func (t *testImpl) Error(args ...interface{}) {
	t.addFailureAndCancel(1, "", args...)
}

// Errorf implements the TestingT interface.
func (t *testImpl) Errorf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
}

func (t *testImpl) addFailureAndCancel(depth int, format string, args ...interface{}) {
	t.addFailure(depth+1, format, args...)
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

// addFailure depth indicates how many stack frames to skip when reporting the
// site of the failure in logs. `0` will report the caller of addFailure, `1` the
// caller of the caller of addFailure, etc.
func (t *testImpl) addFailure(depth int, format string, args ...interface{}) {
	if format == "" {
		format = strings.Repeat(" %v", len(args))[1:]
	}
	reportFailure := newFailure(errors.NewWithDepthf(depth+1, format, args...), collectErrors(args))

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.mu.failuresSuppressed {
		t.mu.failures = append(t.mu.failures, reportFailure)
	}

	var b strings.Builder
	formatFailure(&b, reportFailure)
	msg := b.String()

	t.mu.numFailures++
	failureNum := t.mu.numFailures
	failureLog := fmt.Sprintf("failure_%d", failureNum)
	t.L().Printf("test failure #%d: full stack retained in %s.log: %s", failureNum, failureLog, msg)
	// Also dump the verbose error (incl. all stack traces) to a log file, in case
	// we need it. The stacks are sometimes helpful, but we don't want them in the
	// main log as they are highly verbose.
	{
		cl, err := t.L().ChildLogger(failureLog, logger.QuietStderr, logger.QuietStdout)
		if err == nil {
			// We don't actually log through this logger since it adds an unrelated
			// file:line caller (namely ours). The error already has stack traces
			// so it's better to write only it to the file to avoid confusion.
			if cl.File != nil {
				path := cl.File.Name()
				if len(path) > 0 {
					_ = os.WriteFile(path, []byte(fmt.Sprintf("%+v", reportFailure.squashedErr)), 0644)
				}
			}
			cl.Close() // we just wanted the filename
		}
	}

	t.mu.output = append(t.mu.output, msg...)
	t.mu.output = append(t.mu.output, '\n')
}

// suppressFailures will stop future failures from being surfaced to github posting
// or the test logger. It will not stop those failures from being logged in their
// own failure.log files. Used if we are confident on the root cause of a failure and
// want to reduce noise of other failures, i.e. timeouts.
func (t *testImpl) suppressFailures() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.failuresSuppressed = true
}

func (t *testImpl) resetFailures() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.failures = nil
	t.mu.failuresSuppressed = false
}

// We take the "squashed" error that contains information of all the errors for each failure.
func formatFailure(b *strings.Builder, reportFailures ...failure) {
	for i, failure := range reportFailures {
		if i > 0 {
			fmt.Fprintln(b)
		}
		file, line, fn, ok := errors.GetOneLineSource(failure.squashedErr)
		if !ok {
			file, line, fn = "<unknown>", 0, "unknown"
		}
		fmt.Fprintf(b, "(%s:%d).%s: %v", file, line, fn, failure.squashedErr)
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
	return t.mu.numFailures > 0
}

func (t *testImpl) failures() []failure {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failures
}

func (t *testImpl) failureMsg() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var b strings.Builder
	formatFailure(&b, t.mu.failures...)
	return b.String()
}

// failuresMatchingError checks whether the first error in trees of
// any of the errors in the failures passed match the `refError`
// target. If it does, `refError` is set to that target error value
// and returns true. Otherwise, it returns false.
func failuresMatchingError(failures []failure, refError any) bool {
	// unwrap unwraps the error passed to find the innermost error in the
	// chain that satisfies the `refError` provided.
	unwrap := func(err error) bool {
		var matched bool
		for {
			if isRef := errors.As(err, refError); !isRef {
				break
			}

			matched = true
			err = errors.Unwrap(err)
			if err == nil {
				break
			}
		}

		return matched
	}

	for _, f := range failures {
		for _, err := range f.errors {
			if unwrap(err) {
				return true
			}
		}

		if unwrap(f.squashedErr) {
			return true
		}
	}

	return false
}

var transientErrorRegex = regexp.MustCompile(`TRANSIENT_ERROR\((.+)\)`)

// transientErrorOwnershipFallback string matches for `TRANSIENT_ERROR` in the provided
// failures and returns a new ErrorWithOwnership if it does. It iterates through each failure,
// checking both the squashedErr and list of errors for `TRANSIENT_ERROR`.
//
// This is needed as the `require` package does not preserve the error object needed
// for us to properly check if it is a transient error using `failuresMatchingError`.
// Note the match is additionally guarded by the unique substring which denotes that
// the error originated from the require package. If we see somewhere else that does not
// preserve the error object, we want to investigate whether it can be fixed before resorting
// to this fallback. See: #131094
func transientErrorOwnershipFallback(failures []failure) *registry.ErrorWithOwnership {
	const unexpectedErrPrefix = "Received unexpected error:"
	var errWithOwner registry.ErrorWithOwnership
	isTransient := func(err error) bool {
		// Both squashedErr and errors should be non nil in actual roachtest failures,
		// but for testing we sometimes only set one for simplicity.
		if err == nil {
			return false
		}
		// The require package prepends this message to `require.NoError` failures.
		// We may see `TRANSIENT_ERROR` without this prefix, but don't mark it as
		// a flake as we may be able to fix the code that doesn't preserve the error.
		if !strings.Contains(err.Error(), unexpectedErrPrefix) {
			return false
		}

		if match := transientErrorRegex.FindString(err.Error()); match != "" {
			problemCause := strings.TrimPrefix(match, "TRANSIENT_ERROR(")
			problemCause = strings.TrimSuffix(problemCause, ")")
			// The cause will be used to create the github issue creation, so we don't want
			// it to be blank. Instead, return false and let us investigate what is creating
			// a transient error with no cause.
			if problemCause == "" {
				return false
			}

			errWithOwner = registry.ErrorWithOwner(
				registry.OwnerTestEng, err,
				registry.WithTitleOverride(problemCause),
				registry.InfraFlake,
			)
			return true
		}

		return false
	}

	for _, f := range failures {
		for _, err := range f.errors {
			if isTransient(err) {
				return &errWithOwner
			}
		}

		if isTransient(f.squashedErr) {
			return &errWithOwner
		}
	}

	return nil
}

func (t *testImpl) ArtifactsDir() string {
	return t.artifactsDir
}

func (t *testImpl) PerfArtifactsDir() string {
	return perfArtifactsDir
}

func (t *testImpl) GoCoverArtifactsDir() string {
	if t.goCoverEnabled {
		return goCoverArtifactsDir
	}
	return ""
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

// defaultTaskOptions returns the default options for a task started by the test.
func defaultTaskOptions() []task.Option {
	return []task.Option{
		task.PanicHandler(func(_ context.Context, name string, l *logger.Logger, r interface{}) error {
			return fmt.Errorf("test task %s panicked: %v", name, r)
		}),
	}
}

// GoWithCancel runs the given function in a goroutine and returns a
// CancelFunc that can be used to cancel the function.
func (t *testImpl) GoWithCancel(fn task.Func, opts ...task.Option) context.CancelFunc {
	return t.taskManager.GoWithCancel(
		fn, task.OptionList(defaultTaskOptions()...), task.OptionList(opts...),
	)
}

// Go is like GoWithCancel but without a cancel function.
func (t *testImpl) Go(fn task.Func, opts ...task.Option) {
	_ = t.GoWithCancel(fn, task.OptionList(opts...))
}

// NewGroup starts a new task group.
func (t *testImpl) NewGroup(opts ...task.Option) task.Group {
	return t.taskManager.NewGroup(task.OptionList(defaultTaskOptions()...), task.OptionList(opts...))
}

// NewErrorGroup starts a new task error group.
func (t *testImpl) NewErrorGroup(opts ...task.Option) task.ErrorGroup {
	return t.taskManager.NewErrorGroup(task.OptionList(defaultTaskOptions()...), task.OptionList(opts...))
}

// TeamCityEscape escapes a string for use as <value> in a key='<value>' attribute
// in TeamCity build output marker.
// See https://www.jetbrains.com/help/teamcity/2023.05/service-messages.html#Escaped+Values
func TeamCityEscape(s string) string {
	var sb strings.Builder

	for _, runeValue := range s {
		switch runeValue {
		case '\n':
			sb.WriteString("|n")
		case '\r':
			sb.WriteString("|r")
		case '|':
			sb.WriteString("||")
		case '[':
			sb.WriteString("|[")
		case ']':
			sb.WriteString("|]")
		case '\'':
			sb.WriteString("|'")
		default:
			if runeValue > 127 {
				// escape unicode
				sb.WriteString(fmt.Sprintf("|0x%04x", runeValue))
			} else {
				sb.WriteRune(runeValue)
			}
		}
	}
	return sb.String()
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
		// The cluster that the worker is currently operating on. If the worker is
		// currently running a test, the test is using this cluster. Nil if the
		// worker does not currently have a cluster.
		c *clusterImpl
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
