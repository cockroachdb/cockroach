// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/stacktrace"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	raven "github.com/getsentry/raven-go"
)

// The call stack here is usually:
// - ReportPanic
// - RecoverAndReport
// - panic()
// so ReportPanic should pop three frames.
const depthForRecoverAndReportPanic = 3

var (
	// crashReportEnv controls the version reported in crash reports
	crashReportEnv = "development"

	// DiagnosticsReportingEnabled wraps "diagnostics.reporting.enabled".
	//
	// "diagnostics.reporting.enabled" enables reporting of metrics related to a
	// node's storage (number, size and health of ranges) back to CockroachDB.
	// Collecting this data from production clusters helps us understand and improve
	// how our storage systems behave in real-world use cases.
	//
	// Note: while the setting itself is actually defined with a default value of
	// `false`, it is usually automatically set to `true` when a cluster is created
	// (or is migrated from a earlier beta version). This can be prevented with the
	// env var COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING.
	//
	// Doing this, rather than just using a default of `true`, means that a node
	// will not errantly send a report using a default before loading settings.
	DiagnosticsReportingEnabled = settings.RegisterPublicBoolSetting(
		"diagnostics.reporting.enabled",
		"enable reporting diagnostic metrics to cockroach labs",
		false,
	)

	// CrashReports wraps "diagnostics.reporting.send_crash_reports".
	CrashReports = settings.RegisterBoolSetting(
		"diagnostics.reporting.send_crash_reports",
		"send crash and panic reports",
		true,
	)

	// PanicOnAssertions wraps "debug.panic_on_failed_assertions"
	PanicOnAssertions = settings.RegisterBoolSetting(
		"debug.panic_on_failed_assertions",
		"panic when an assertion fails rather than reporting",
		false,
	)

	// startTime records when the process started so that crash reports can
	// include the server's uptime as an extra tag.
	startTime = timeutil.Now()

	// ReportSensitiveDetails enables reporting of unanonymized data.
	//
	// This should not be used by anyone unwilling to share the whole cluster
	// data with Cockroach Labs and various cloud services.
	ReportSensitiveDetails = envutil.EnvOrDefaultBool("COCKROACH_REPORT_SENSITIVE_DETAILS", false)
)

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic(ctx context.Context, sv *settings.Values) {
	if r := recover(); r != nil {
		ReportPanic(ctx, sv, r, depthForRecoverAndReportPanic)
		panic(r)
	}
}

// RecoverAndReportNonfatalPanic is an alternative RecoverAndReportPanic that
// does not re-panic in Release builds.
func RecoverAndReportNonfatalPanic(ctx context.Context, sv *settings.Values) {
	if r := recover(); r != nil {
		ReportPanic(ctx, sv, r, depthForRecoverAndReportPanic)
		if !build.IsRelease() || PanicOnAssertions.Get(sv) {
			panic(r)
		}
	}
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, sv *settings.Values, r interface{}, depth int) {
	Shout(ctx, Severity_ERROR, "a panic has occurred!")

	if mainLog.stderrRedirected() {
		// We do not use Shout() to print the panic details here, because
		// if stderr is not redirected (e.g. when logging to file is
		// disabled) Shout() would copy its argument to stderr
		// unconditionally, and we don't want that: Go's runtime system
		// already unconditionally copies the panic details to stderr.
		// Instead, we copy manually the details to stderr, only when stderr
		// is redirected to a file otherwise.
		fmt.Fprintf(OrigStderr, "%v\n\n%s\n", r, debug.Stack())

		// FIXME(knz): if stderr is redirected and redactableLogs is set,
		// stderr redirection needs to add redaction markers. Until then,
		// we're leaking unsafe data.
	} else {
		// If stderr is not redirected, then Go's runtime will only print
		// out the panic details to the original stderr, and we'll miss a copy
		// in the log file. Produce it here.
		mainLog.printPanicToFile(r)
	}

	sendCrashReport(ctx, sv, PanicAsError(depth+1, r), ReportTypePanic)

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	Flush()
}

// PanicAsError turns r into an error if it is not one already.
func PanicAsError(depth int, r interface{}) error {
	if err, ok := r.(error); ok {
		return errors.WithStackDepth(err, depth+1)
	}
	return errors.NewWithDepthf(depth+1, "panic: %v", r)
}

var crashReportURL = func() string {
	var defaultURL string
	if build.SeemsOfficial() {
		defaultURL = "https://ignored:ignored@errors.cockroachdb.com/sentry"
	}
	return envutil.EnvOrDefaultString("COCKROACH_CRASH_REPORTS", defaultURL)
}()

// crashReportingActive is set to true if raven has been initialized.
var crashReportingActive bool

// SetupCrashReporter sets the crash reporter info.
func SetupCrashReporter(ctx context.Context, cmd string) {
	if crashReportURL == "" {
		return
	}
	if err := raven.SetDSN(crashReportURL); err != nil {
		panic(errors.Wrap(err, "failed to setup crash reporting"))
	}
	crashReportingActive = true

	if cmd == "start" {
		cmd = "server"
	}
	info := build.GetInfo()
	raven.SetRelease(info.Tag)
	raven.SetEnvironment(crashReportEnv)
	raven.SetTagsContext(map[string]string{
		"cmd":          cmd,
		"platform":     info.Platform,
		"distribution": info.Distribution,
		"rev":          info.Revision,
		"goversion":    info.GoVersion,
	})
}

func uptimeTag(now time.Time) string {
	uptime := now.Sub(startTime)
	switch {
	case uptime < 1*time.Second:
		return "<1s"
	case uptime < 10*time.Second:
		return "<10s"
	case uptime < 1*time.Minute:
		return "<1m"
	case uptime < 10*time.Minute:
		return "<10m"
	case uptime < 1*time.Hour:
		return "<1h"
	case uptime < 10*time.Hour:
		return "<10h"
	default:
		daysUp := int(uptime / (24 * time.Hour))
		return fmt.Sprintf("<%dd", daysUp+1)
	}
}

// ReportType is used to differentiate between an actual crash/panic and just
// reporting an error. This data is useful for stability purposes.
type ReportType int

const (
	// ReportTypePanic signifies that this is an actual panic.
	ReportTypePanic ReportType = iota
	// ReportTypeError signifies that this is just a report of an error but it
	// still may include an exception and stack trace.
	ReportTypeError
)

// sendCrashReport posts to sentry.
//
// The crashReportType parameter adds a tag to the event that shows if the
// cluster did indeed crash or not.
func sendCrashReport(
	ctx context.Context, sv *settings.Values, err error, crashReportType ReportType,
) {
	if !ShouldSendReport(sv) {
		return
	}

	errMsg, packetDetails, extraDetails := errors.BuildSentryReport(err)
	SendReport(ctx, errMsg, crashReportType, extraDetails, packetDetails...)
}

// ShouldSendReport returns true iff SendReport() should be called.
func ShouldSendReport(sv *settings.Values) bool {
	if sv == nil || !DiagnosticsReportingEnabled.Get(sv) || !CrashReports.Get(sv) {
		return false // disabled via settings.
	}
	if !crashReportingActive {
		return false // disabled via empty URL env var.
	}
	return true
}

// SendReport uploads a detailed error report to sentry.
// Note that there can be at most one reportable object of each type in the report.
// For more messages, use extraDetails.
// The crashReportType parameter adds a tag to the event that shows if the
// cluster did indeed crash or not.
func SendReport(
	ctx context.Context,
	errMsg string,
	crashReportType ReportType,
	extraDetails map[string]interface{},
	details ...stacktrace.ReportableObject,
) {
	packet := raven.NewPacket(errMsg, details...)

	for extraKey, extraValue := range extraDetails {
		packet.Extra[extraKey] = extraValue
	}

	if !ReportSensitiveDetails {
		// Avoid leaking the machine's hostname by injecting the literal "<redacted>".
		// Otherwise, raven.Client.Capture will see an empty ServerName field and
		// automatically fill in the machine's hostname.
		packet.ServerName = "<redacted>"
	}
	tags := map[string]string{
		"uptime": uptimeTag(timeutil.Now()),
	}

	switch crashReportType {
	case ReportTypePanic:
		tags["report_type"] = "panic"
	case ReportTypeError:
		tags["report_type"] = "error"
	}

	for _, f := range tagFns {
		v := f.value(ctx)
		if v != "" {
			tags[f.key] = maybeTruncate(v)
		}
	}

	eventID, ch := raven.DefaultClient.Capture(packet, tags)
	select {
	case <-ch:
		Shoutf(ctx, Severity_ERROR, "Reported as error %v", eventID)
	case <-time.After(10 * time.Second):
		Shout(ctx, Severity_ERROR, "Time out trying to submit crash report")
	}
}

// ReportOrPanic either reports an error to sentry, if run from a release
// binary, or panics, if triggered in tests. This is intended to be used for
// failing assertions which are recoverable but serious enough to report and to
// cause tests to fail.
//
// Like SendCrashReport, the format string should not contain any sensitive
// data, and unsafe reportables will be redacted before reporting.
func ReportOrPanic(
	ctx context.Context, sv *settings.Values, format string, reportables ...interface{},
) {
	if !build.IsRelease() || (sv != nil && PanicOnAssertions.Get(sv)) {
		panic(fmt.Sprintf(format, reportables...))
	}
	Warningf(ctx, format, reportables...)
	err := errors.Newf("internal error: "+format, reportables...)
	sendCrashReport(ctx, sv, err, ReportTypeError)
}

// Sentry max tag value length.
// From: https://github.com/getsentry/sentry-docs/pull/1304/files
const maxTagLen = 200

func maybeTruncate(tagValue string) string {
	if len(tagValue) > maxTagLen {
		const truncatedSuffix = " [...]"
		return tagValue[:maxTagLen-len(truncatedSuffix)] + truncatedSuffix
	}
	return tagValue
}

type tagFn struct {
	key   string
	value func(context.Context) string
}

var tagFns []tagFn

// RegisterTagFn adds a function for tagging crash reports based on the context.
// This is intended to be called by other packages at init time.
func RegisterTagFn(key string, value func(context.Context) string) {
	tagFns = append(tagFns, tagFn{key, value})
}
