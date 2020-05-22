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
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/stacktrace"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	raven "github.com/getsentry/raven-go"
	"github.com/pkg/errors"
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

// SafeMessager is implemented by objects which have a way of representing
// themselves suitably redacted for anonymized reporting.
type SafeMessager interface {
	SafeMessage() string
}

// A SafeType panic can be reported verbatim, i.e. does not leak information.
// A nil `*SafeType` is not valid for use and may cause panics.
type SafeType struct {
	V      interface{}
	causes []interface{} // unsafe
}

var _ SafeMessager = SafeType{}
var _ interfaceCauser = SafeType{}

type interfaceCauser interface {
	Cause() interface{}
}

// SafeMessage implements SafeMessager. It does not recurse into
// the SafeType's Cause()s.
func (st SafeType) SafeMessage() string {
	return fmt.Sprintf("%v", st.V)
}

// Format implements fmt.Formatter.
func (st SafeType) Format(s fmt.State, verb rune) {
	switch {
	case verb == 'v' && s.Flag('+'):
		fmt.Fprintf(s, "%s", st.Error())
	default:
		// "%d" etc with log.Safe() should minimally work.
		// TODO(knz): This may lose some flags.
		fmt.Fprintf(s, fmt.Sprintf("%%%c", verb), st.V)
	}
}

// Error implements error as a convenience.
func (st SafeType) Error() string {
	var buf bytes.Buffer
	fmt.Fprint(&buf, st.SafeMessage())
	for _, cause := range st.causes {
		fmt.Fprintf(&buf, "; caused by %v", cause)
	}
	return buf.String()
}

// SafeType implements fmt.Stringer as a convenience.
func (st SafeType) String() string {
	return st.SafeMessage()
}

// Cause returns the value passed to Chain() when this SafeType
// was created (or nil).
func (st SafeType) Cause() interface{} {
	if len(st.causes) == 0 {
		return nil
	}
	v := "<redacted>"
	if messager, ok := st.causes[0].(SafeMessager); ok {
		v = messager.SafeMessage()
	}
	return SafeType{
		V:      v,
		causes: st.causes[1:],
	}
}

// Safe constructs a SafeType. It is equivalent to `Chain(v, nil)`.
func Safe(v interface{}) SafeType {
	return SafeType{V: v}
}

// WithCause links a safe message and a child about which nothing is assumed,
// but for which the hope is that some anonymized parts can be obtained from it.
func (st SafeType) WithCause(cause interface{}) SafeType {
	causes := st.causes
	for cause != nil {
		causes = append(causes, cause)
		causer, ok := cause.(interfaceCauser)
		if !ok {
			break
		}
		cause = causer.Cause()
	}
	return SafeType{
		V:      st.V,
		causes: causes,
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
		// already unconditonally copies the panic details to stderr.
		// Instead, we copy manually the details to stderr, only when stderr
		// is redirected to a file otherwise.
		fmt.Fprintf(OrigStderr, "%v\n\n%s\n", r, debug.Stack())
	} else {
		// If stderr is not redirected, then Go's runtime will only print
		// out the panic details to the original stderr, and we'll miss a copy
		// in the log file. Produce it here.
		mainLog.printPanicToFile(r)
	}

	SendCrashReport(ctx, sv, depth+1, "", []interface{}{r}, ReportTypePanic)

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	Flush()
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

type safeError struct {
	message string
}

func (e *safeError) Error() string {
	return e.message
}

// Redact returns a redacted version of the supplied item that is safe to use in
// anonymized reporting.
func Redact(r interface{}) string {
	typAnd := func(i interface{}, text string) string {
		typ := ErrorSource(i)
		if typ == "" {
			typ = fmt.Sprintf("%T", i)
		}
		if text == "" {
			return typ
		}
		if strings.HasPrefix(typ, "errors.") {
			// Don't bother reporting the type for errors.New() and its
			// nondescript siblings. Note that errors coming from pkg/errors
			// usually have `typ` overridden to file:line above, so they won't
			// hit this path.
			return text
		}
		return typ + ": " + text
	}

	handle := func(r interface{}) string {
		switch t := r.(type) {
		case SafeMessager:
			return t.SafeMessage()
		case error:
			// continue below
		default:
			return typAnd(r, "")
		}

		// Now that we're looking at an error, see if it's one we can
		// deconstruct for maximum (safe) clarity. Separating this from the
		// block above ensures that the types below actually implement `error`.
		switch t := r.(error).(type) {
		case runtime.Error:
			return typAnd(t, t.Error())
		case sysutil.Errno:
			return typAnd(t, t.Error())
		case *os.SyscallError:
			s := Redact(t.Err)
			return typAnd(t, fmt.Sprintf("%s: %s", t.Syscall, s))
		case *os.PathError:
			// It hardly matters, but avoid mutating the original.
			cpy := *t
			t = &cpy
			t.Path = "<redacted>"
			return typAnd(t, t.Error())
		case *os.LinkError:
			// It hardly matters, but avoid mutating the original.
			cpy := *t
			t = &cpy

			t.Old, t.New = "<redacted>", "<redacted>"
			return typAnd(t, t.Error())
		case *net.OpError:
			// It hardly matters, but avoid mutating the original.
			cpy := *t
			t = &cpy
			t.Source = &util.UnresolvedAddr{NetworkField: "tcp", AddressField: "redacted"}
			t.Addr = &util.UnresolvedAddr{NetworkField: "tcp", AddressField: "redacted"}
			t.Err = errors.New(Redact(t.Err))
			return typAnd(t, t.Error())
		default:
		}

		// Still an error, but not one we know how to deconstruct.

		switch r.(error) {
		case context.DeadlineExceeded:
		case context.Canceled:
		case os.ErrInvalid:
		case os.ErrPermission:
		case os.ErrExist:
		case os.ErrNotExist:
		case os.ErrClosed:
		default:
			// Not a whitelisted sentinel error.
			return typAnd(r, "")
		}
		// Whitelisted sentinel error.
		return typAnd(r, r.(error).Error())
	}

	reportable := handle(r)

	switch c := r.(type) {
	case interfaceCauser:
		cause := c.Cause()
		if cause != nil {
			reportable += ": caused by " + Redact(c.Cause())
		}
	case (interface {
		Cause() error
	}):
		reportable += ": caused by " + Redact(c.Cause())
	}
	return reportable
}

// ReportablesToSafeError inspects the given format string (taken as not needing
// redaction) and reportables, redacts them appropriately and returns an error
// that is safe to pass to anonymized reporting. The given depth is used to
// insert a callsite when positive.
func ReportablesToSafeError(depth int, format string, reportables []interface{}) error {
	if len(reportables) == 0 {
		reportables = []interface{}{"nothing reported"}
	}

	file := "?"
	var line int
	if depth > 0 {
		file, line, _ = caller.Lookup(depth + 1)
	}

	redacted := make([]string, 0, len(reportables))
	for i := range reportables {
		redacted = append(redacted, Redact(reportables[i]))
	}
	reportables = nil

	var sep string
	// TODO(tschottdorf): it would be nice to massage the format so that all of its verbs are replaced by %v
	// (so that we could now call `fmt.Sprintf(newFormat, reportables...)`).
	// This isn't trivial. For example, "%ss %.2f %#v %U+%04X %%" would become "%ss %s %s %s %%".
	// The logic to do that is known to `fmt.Printf` but we'd have to copy it here.
	if format != "" {
		sep = " | "
	}
	err := &safeError{
		message: fmt.Sprintf("%s:%d: %s%s%s", filepath.Base(file), line, format, sep, strings.Join(redacted, "; ")),
	}
	return err
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

// SendCrashReport posts to sentry. The `reportables` is essentially the `args...` in
// `log.Fatalf(format, args...)` (similarly for `log.Fatal`) or `[]interface{}{arg}` in
// `panic(arg)`.
//
// The format string and those items in `reportables` which are a) an error or b) (values of or
// pointers to) `log.Safe` will be used verbatim to construct the error that is reported to sentry.
//
// TODO(dt,knz,sql-team): we need to audit all sprintf'ing of values into the errors and strings
// passed to panic, to ensure raw user data is kept separate and can thus be elided here. For now,
// the type is about all we can assume is safe to report, which combined with file and line info
// should be at least somewhat helpful in telling us where crashes are coming from. We capture the
// full stacktrace below, so we only need the short file and line here help uniquely identify the
// error. Some exceptions, like a runtime.Error, are assumed to be fine as-is.
//
// The crashReportType parameter adds a tag to the event that shows if the
// cluster did indeed crash or not.
func SendCrashReport(
	ctx context.Context,
	sv *settings.Values,
	depth int,
	format string,
	reportables []interface{},
	crashReportType ReportType,
) {
	if !ShouldSendReport(sv) {
		return
	}
	err := ReportablesToSafeError(depth+1, format, reportables)
	ex := raven.NewException(err, stacktrace.NewStackTrace(depth+1))
	SendReport(ctx, err.Error(), crashReportType, nil, ex)
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
		Shout(ctx, Severity_ERROR, "Reported as error "+eventID)
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
	SendCrashReport(ctx, sv, 1 /* depth */, format, reportables, ReportTypeError)
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

// ErrorSource attempts to return the file:line where `i` was created if `i` has
// that information (i.e. if it is an errors.withStack). Returns "" otherwise.
func ErrorSource(i interface{}) string {
	type stackTracer interface {
		StackTrace() errors.StackTrace
	}
	if e, ok := i.(stackTracer); ok {
		tr := e.StackTrace()
		if len(tr) > 0 {
			return fmt.Sprintf("%v", tr[0]) // prints file:line
		}
	}
	return ""
}
