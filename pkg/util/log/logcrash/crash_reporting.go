// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logcrash

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	sentry "github.com/getsentry/sentry-go"
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
	DiagnosticsReportingEnabled = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"diagnostics.reporting.enabled",
		"enable reporting diagnostic metrics to cockroach labs",
		false,
	).WithPublic()

	// CrashReports wraps "diagnostics.reporting.send_crash_reports".
	CrashReports = settings.RegisterBoolSetting(
		settings.TenantWritable,
		"diagnostics.reporting.send_crash_reports",
		"send crash and panic reports",
		true,
	)

	// PanicOnAssertions wraps "debug.panic_on_failed_assertions"
	PanicOnAssertions = settings.RegisterBoolSetting(
		settings.TenantWritable,
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

	// globalSettings stores a global reference to a *setting.Values container;
	// used for code paths where the container is not available.
	globalSettings atomic.Value
)

// SetGlobalSettings sets the *settings.Values container that will be refreshed
// at runtime -- ideally we should have no other *Values containers floating
// around, as they will be stale / lies.
func SetGlobalSettings(v *settings.Values) {
	globalSettings.Store(v)
}

func getGlobalSettings() *settings.Values {
	if ptr := globalSettings.Load(); ptr != nil {
		return ptr.(*settings.Values)
	}
	return nil
}

// ReportPanicWithGlobalSettings is a variant of ReportPanic that uses the
// *settings.Values that was set using SetGlobalSettings(). Does nothing if that
// function was not called.
//
// Should be used only when strictly necessary; use ReportPanic whenever we have
// access to the settings.
func ReportPanicWithGlobalSettings(ctx context.Context, r interface{}, depth int) {
	if sv := getGlobalSettings(); sv != nil {
		ReportPanic(ctx, sv, r, depth+1)
	}
}

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
//
// Note that ReportPanic() does not assume that the panic object
// will be left uncaught to terminate the process. For example,
// at the time of this writing, ReportPanic() is called from
// RecoverAndReportNonfatalPanic() above.
func ReportPanic(ctx context.Context, sv *settings.Values, r interface{}, depth int) {
	// Announce the panic has occurred to all places. The purpose
	// of this call is threefold:
	// - it ensures there's a notice on the terminal, in case
	//   logging would only go to file otherwise;
	// - it ensures there's a notice on the output file, in
	//   case the panic is uncaught and internal stderr
	//   writes by the Go runtime have not been set up to
	//   redirect to a separate log file.
	// - it places a timestamp in front of the panic object,
	//   in case the various configuration options make
	//   the Go runtime solely responsible for printing
	//   out the panic object to the log file.
	//   (The go runtime doesn't time stamp its output.)
	//
	// Note that this code will cause the panic object to be printed
	// twice in some cases (specifically, when the panic is uncaught).
	// A previous version of this code was trying to prevent the
	// double print and was failing to do so effectively, causing
	// instead panics to be lost in the case where they were
	// recovered (eg via RecoverAndReportNonfatalPanic()).
	//
	// To properly prevent double prints, the API could be changed to
	// indicate whether the Go runtime will *eventually* print the panic
	// on its own. Unfortunately, this is a bit hard to do, as the
	// caller of ReportPanic() may not be in a position to know for
	// sure, whether some other caller further in the call stack is
	// catching the panic object in the end or not.
	panicErr := PanicAsError(depth+1, r)
	log.Ops.Shoutf(ctx, severity.ERROR, "a panic has occurred!\n%+v", panicErr)

	// In addition to informing the user, also report the details to telemetry.
	sendCrashReport(ctx, sv, panicErr, ReportTypePanic)

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	log.Flush()
}

// PanicAsError turns r into an error if it is not one already.
func PanicAsError(depth int, r interface{}) error {
	if err, ok := r.(error); ok {
		return errors.WithStackDepth(err, depth+1)
	}
	return errors.NewWithDepthf(depth+1, "panic: %v", r)
}

// Crash reporting URL.
//
// This uses a Sentry proxy run by Cockroach Labs. The proxy
// abstracts the actual Sentry submission project ID and
// submission key.
//
// Non-release builds wishing to use Sentry reports
// are invited to use the following URL instead:
//
//   https://ignored@errors.cockroachdb.com/api/sentrydev/v2/1111
//
// This can be set via e.g. the env var COCKROACH_CRASH_REPORTS.
// Note that the special number "1111" is important as it
// needs to be matched exactly by the CRL proxy.
//
// TODO(knz): We could envision auto-selecting this alternate URL
// when detecting a non-release build.
var crashReportURL = func() string {
	var defaultURL string
	if build.SeemsOfficial() {
		defaultURL = "https://ignored@errors.cockroachdb.com/api/sentry/v2/1111"
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

	if cmd == "start" {
		cmd = "server"
	}
	info := build.GetInfo()

	if err := sentry.Init(sentry.ClientOptions{
		Dsn:         crashReportURL,
		Environment: crashReportEnv,
		Release:     info.Tag,
		Dist:        info.Distribution,
	}); err != nil {
		panic(errors.Wrap(err, "failed to setup crash reporting"))
	}

	crashReportingActive = true

	sentry.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTags(map[string]string{
			"cmd":          cmd,
			"platform":     info.Platform,
			"distribution": info.Distribution,
			"rev":          info.Revision,
			"goversion":    info.GoVersion,
		})
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
	// ReportTypeLogFatal signifies that this is an error report that
	// was generated via a log.Fatal call.
	ReportTypeLogFatal
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

	errEvent, extraDetails := errors.BuildSentryReport(err)
	SendReport(ctx, crashReportType, errEvent, extraDetails)
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
	crashReportType ReportType,
	event *sentry.Event,
	extraDetails map[string]interface{},
) {
	for extraKey, extraValue := range extraDetails {
		event.Extra[extraKey] = extraValue
	}

	if !ReportSensitiveDetails {
		// Avoid leaking the machine's hostname by injecting the literal "<redacted>".
		// Otherwise, raven.Client.Capture will see an empty ServerName field and
		// automatically fill in the machine's hostname.
		event.ServerName = "<redacted>"
	}

	event.Tags["uptime"] = uptimeTag(timeutil.Now())

	switch crashReportType {
	case ReportTypePanic:
		event.Tags["report_type"] = "panic"
	case ReportTypeError:
		event.Tags["report_type"] = "error"
	case ReportTypeLogFatal:
		event.Tags["report_type"] = "log_fatal"
	}

	for _, f := range tagFns {
		v := f.value(ctx)
		if v != "" {
			event.Tags[f.key] = maybeTruncate(v)
		}
	}

	res := sentry.CaptureEvent(event)
	if res != nil {
		log.Shoutf(ctx, severity.ERROR, "Queued as error %v", string(*res))
	}
	if !sentry.Flush(10 * time.Second) {
		log.Shout(ctx, severity.ERROR, "Timeout trying to submit crash report")
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
	err := errors.Newf(format, reportables...)
	if !build.IsRelease() || (sv != nil && PanicOnAssertions.Get(sv)) {
		panic(err)
	}
	log.Warningf(ctx, "%v", err)
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

func maybeSendCrashReport(ctx context.Context, err error) {
	// We load the ReportingSettings from global singleton in this call path.
	if sv := getGlobalSettings(); sv != nil {
		sendCrashReport(ctx, sv, err, ReportTypeLogFatal)
	}
}

func init() {
	log.MaybeSendCrashReport = maybeSendCrashReport
}
