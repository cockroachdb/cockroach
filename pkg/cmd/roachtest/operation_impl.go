// Copyright 2024 The Cockroach Authors.
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

type operationImpl struct {
	spec *registry.OperationSpec

	cockroach string // path to main cockroach binary

	deprecatedWorkload string // path to workload binary
	debug              bool   // whether the test is in debug mode.
	// buildVersion is the version of the Cockroach binary that the test will run
	// against.
	buildVersion *version.Version

	// l is the logger that the test will use for its output.
	l *logger.Logger

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
	}
	cleanupState map[string]string
	// Map from version to path to the cockroach binary to be used when
	// mixed-version test wants a binary for that binary. If a particular version
	// <ver> is found in this map, it is used instead of the binary coming from
	// `roachprod stage release <ver>`. See the --versions-binary-override flags.
	//
	// Version strings look like "20.1.4".
	versionsBinaryOverride map[string]string
}

// BuildVersion exposes the build version of the cluster
// in this test.
func (t *operationImpl) BuildVersion() *version.Version {
	return t.buildVersion
}

func (t *operationImpl) Cockroach() string {
	return t.cockroach
}

func (t *operationImpl) RuntimeAssertionsCockroach() string {
	return t.cockroach
}

func (t *operationImpl) StandardCockroach() string {
	return t.cockroach
}

func (t *operationImpl) DeprecatedWorkload() string {
	return t.deprecatedWorkload
}

func (t *operationImpl) VersionsBinaryOverride() map[string]string {
	return t.versionsBinaryOverride
}

func (t *operationImpl) SkipInit() bool {
	// Operations always skip init.
	return true
}

// Spec returns the TestSpec.
func (t *operationImpl) Spec() interface{} {
	return t.spec
}

func (t *operationImpl) Helper() {}

func (t *operationImpl) Name() string {
	return t.spec.Name
}

func (t *operationImpl) SnapshotPrefix() string {
	return ""
}

// L returns the test's logger.
func (t *operationImpl) L() *logger.Logger {
	return t.l
}

// ReplaceL replaces the test's logger.
func (t *operationImpl) ReplaceL(l *logger.Logger) {
	// TODO(tbg): get rid of this, this is racy & hacky.
	t.l = l
}

func (t *operationImpl) SetCleanupState(key, value string) {
	t.cleanupState[key] = value
}

func (t *operationImpl) GetCleanupState(key string) string {
	return t.cleanupState[key]
}

func (t *operationImpl) status(ctx context.Context, id int64, args ...interface{}) {
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
func (t *operationImpl) Status(args ...interface{}) {
	t.status(context.TODO(), t.runnerID, args...)
}

// IsDebug returns true if the test is in a debug state.
func (t *operationImpl) IsDebug() bool {
	return t.debug
}

// GetStatus returns the status of the tests's main goroutine.
func (t *operationImpl) GetStatus() string {
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
func (t *operationImpl) WorkerStatus(args ...interface{}) {
	t.status(context.TODO(), goid.Get(), args...)
}

func (t *operationImpl) progress(id int64, frac float64) {
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
func (t *operationImpl) Progress(frac float64) {
	t.progress(t.runnerID, frac)
}

// WorkerProgress sets the progress (a fraction in the range [0,1]) associated
// with the a worker status message.
func (t *operationImpl) WorkerProgress(frac float64) {
	t.progress(goid.Get(), frac)
}

var _ skip.SkippableTest = (*operationImpl)(nil)

// Skip skips the test. The first argument if any is the main message.
// The remaining argument, if any, form the details.
// This implements the skip.SkippableTest interface.
func (t *operationImpl) Skip(args ...interface{}) {
	if len(args) > 0 {
		t.spec.Skip = fmt.Sprint(args[0])
		args = args[1:]
	}
	t.spec.SkipDetails = fmt.Sprint(args...)
	panic(errTestFatal)
}

// Skipf skips the test. The formatted message becomes the skip reason.
// This implements the skip.SkippableTest interface.
func (t *operationImpl) Skipf(format string, args ...interface{}) {
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
func (t *operationImpl) Fatal(args ...interface{}) {
	t.addFailureAndCancel(1, "", args...)
	panic(errTestFatal)
}

// Fatalf is like Fatal, but takes a format string.
func (t *operationImpl) Fatalf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
	panic(errTestFatal)
}

// FailNow implements the TestingT interface.
func (t *operationImpl) FailNow() {
	t.addFailureAndCancel(1, "FailNow called")
	panic(errTestFatal)
}

// Error implements the TestingT interface
func (t *operationImpl) Error(args ...interface{}) {
	t.addFailureAndCancel(1, "", args...)
}

// Errorf implements the TestingT interface.
func (t *operationImpl) Errorf(format string, args ...interface{}) {
	t.addFailureAndCancel(1, format, args...)
}

func (t *operationImpl) addFailureAndCancel(depth int, format string, args ...interface{}) {
	t.addFailure(depth+1, format, args...)
	if t.mu.cancel != nil {
		t.mu.cancel()
	}
}

// addFailure depth indicates how many stack frames to skip when reporting the
// site of the failure in logs. `0` will report the caller of addFailure, `1` the
// caller of the caller of addFailure, etc.
func (t *operationImpl) addFailure(depth int, format string, args ...interface{}) {
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
func (t *operationImpl) suppressFailures() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.failuresSuppressed = true
}

func (t *operationImpl) duration() time.Duration {
	return t.end.Sub(t.start)
}

func (t *operationImpl) Failed() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.failedRLocked()
}

func (t *operationImpl) failedRLocked() bool {
	return t.mu.numFailures > 0
}

func (t *operationImpl) failures() []failure {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.mu.failures
}

func (t *operationImpl) failureMsg() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var b strings.Builder
	formatFailure(&b, t.mu.failures...)
	return b.String()
}

func (t *operationImpl) ArtifactsDir() string {
	return t.artifactsDir
}

func (t *operationImpl) PerfArtifactsDir() string {
	return perfArtifactsDir
}

func (t *operationImpl) GoCoverArtifactsDir() string {
	return ""
}

// IsBuildVersion returns true if the build version is greater than or equal to
// minVersion. This allows a test to optionally perform additional checks
// depending on the cockroach version it is running against. Note that the
// versions are Cockroach build tag version numbers, not the internal cluster
// version number.
func (t *operationImpl) IsBuildVersion(minVersion string) bool {
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
