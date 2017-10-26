// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package log

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	raven "github.com/getsentry/raven-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
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

	// startTime records when the process started so that crash reports can
	// include the server's uptime as an extra tag.
	startTime = timeutil.Now()
)

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic(ctx context.Context, sv *settings.Values) {
	if r := recover(); r != nil {
		// The call stack here is usually:
		// - ReportPanic
		// - RecoverAndReport
		// - panic.go
		// - panic()
		// so ReportPanic should pop four frames.
		ReportPanic(ctx, sv, r, 4)
		panic(r)
	}
}

// A SafeType panic can be reported verbatim, i.e. does not leak information.
//
// TODO(dt): flesh out, see #15892.
type SafeType struct {
	V interface{}
}

// Safe constructs a SafeType.
func Safe(v interface{}) SafeType {
	return SafeType{V: v}
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, sv *settings.Values, r interface{}, depth int) {
	Shout(ctx, Severity_ERROR, "a panic has occurred!")

	SendCrashReport(ctx, sv, depth+1, "", []interface{}{r})

	// Ensure that the logs are flushed before letting a panic
	// terminate the server.
	Flush()

	// We do not use Shout() to print the panic details here, because
	// if stderr is not redirected (e.g. when logging to file is
	// disabled) Shout() would copy its argument to stderr
	// unconditionally, and we don't want that: Go's runtime system
	// already unconditonally copies the panic details to stderr.
	// Instead, we copy manually the details to stderr, only when stderr
	// is redirected to a file otherwise.
	if stderrRedirected {
		fmt.Fprintf(OrigStderr, "%v\n\n%s\n", r, debug.Stack())
	}
}

var crashReportURL = func() string {
	var defaultURL string
	if build.IsRelease() {
		defaultURL = "https://ignored:ignored@errors.cockroachdb.com/sentry"
	}
	return envutil.EnvOrDefaultString("COCKROACH_CRASH_REPORTS", defaultURL)
}()

// SetupCrashReporter sets the crash reporter info.
func SetupCrashReporter(ctx context.Context, cmd string) {
	if err := raven.SetDSN(crashReportURL); err != nil {
		panic(errors.Wrap(err, "failed to setup crash reporting"))
	}

	if cmd == "start" {
		cmd = "server"
	}
	info := build.GetInfo()
	raven.SetRelease(info.Tag)
	raven.SetEnvironment(info.Type)
	raven.SetTagsContext(map[string]string{
		"cmd":          cmd,
		"platform":     info.Platform,
		"distribution": info.Distribution,
		"rev":          info.Revision,
		"goversion":    info.GoVersion,
	})
}

var crdbPaths = []string{
	"github.com/cockroachdb/cockroach",
	"github.com/coreos/etcd/raft",
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

func redact(r interface{}) string {
	handleSafeType := func(v *SafeType) string {
		return fmt.Sprintf("%+v", v.V)
	}

	handle := func(r interface{}) string {
		switch t := r.(type) {
		case *SafeType:
			return handleSafeType(t)
		case SafeType:
			return handleSafeType(&t)
		case error:
			// continue below
		default:
			return ""
		}

		// Now that we're looking at an error, see if it's one we can
		// deconstruct for maximum (safe) clarity. Separating this from the
		// block above ensures that the types below actually implement `error`.
		switch t := r.(error).(type) {
		case runtime.Error:
			return t.Error()
		case syscall.Errno:
			return t.Error()
		case *os.SyscallError:
			return fmt.Sprintf("%s: %s", t.Syscall, redact(t.Err))
		case *os.PathError:
			t.Path = "<redacted>"
			return t.Error()
		case *os.LinkError:
			t.Old, t.New = "<redacted>", "<redacted>"
			return t.Error()
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
			return ""
		}
		// Whitelisted sentinel error.
		return r.(error).Error()
	}

	type causer interface {
		Cause() error
	}

	reportable := handle(r)
	if c, ok := r.(causer); ok {
		if reportable == "" {
			reportable += "<redacted>"
		}
		reportable += ": caused by " + redact(c.Cause())
	}
	return reportable
}

func reportablesToSafeError(depth int, format string, reportables []interface{}) error {
	if len(reportables) == 0 {
		reportables = []interface{}{"nothing reported"}
	}

	file := "?"
	var line int
	if depth > 0 {
		file, line, _ = caller.Lookup(depth)
	}

	if e, ok := reportables[0].(error); ok {
		// Special case so that `panic(err)` for a safe `err` returns `err` (and
		// doesn't wrap it in a `safeError`). In fact, we go further and also try
		// the errors.Cause() similarly. The effect is that something like
		// `errors.Wrap(someSafeErr, "gibberish")` can report `someSafeErr` verbatim.
		for _, err := range []error{e, errors.Cause(e)} {
			if format == "" && len(reportables) == 1 && redact(err) == err.Error() {
				return err
			}
		}
	}

	redacted := make([]string, 0, len(reportables))
	for i := range reportables {
		msg := redact(reportables[i])
		typ := fmt.Sprintf("<%T>", reportables[i])
		if msg == "" {
			redacted = append(redacted, typ)
		} else if typ == "<log.SafeType>" || typ == "<*log.SafeType>" {
			redacted = append(redacted, msg)
		} else {
			redacted = append(redacted, typ+": "+msg)
		}
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
func SendCrashReport(
	ctx context.Context, sv *settings.Values, depth int, format string, reportables []interface{},
) {
	if !DiagnosticsReportingEnabled.Get(sv) || !CrashReports.Get(sv) {
		return // disabled via settings.
	}
	if raven.DefaultClient == nil {
		return // disabled via empty URL env var.
	}

	err := reportablesToSafeError(depth+1, format, reportables)

	// This is close to inlining raven.CaptureErrorAndWait(), except it lets us
	// control the stack depth of the collected trace.
	const contextLines = 3

	ex := raven.NewException(err, raven.NewStacktrace(depth+1, contextLines, crdbPaths))
	packet := raven.NewPacket(err.Error(), ex)
	// Avoid leaking the machine's hostname by injecting the literal "<redacted>".
	// Otherwise, raven.Client.Capture will see an empty ServerName field and
	// automatically fill in the machine's hostname.
	packet.ServerName = "<redacted>"
	tags := map[string]string{
		"uptime": uptimeTag(timeutil.Now()),
	}
	eventID, ch := raven.DefaultClient.Capture(packet, tags)
	select {
	case <-ch:
		Shout(ctx, Severity_ERROR, "Reported as error "+eventID)
	case <-time.After(10 * time.Second):
		Shout(ctx, Severity_ERROR, "Time out trying to submit crash report")
	}
}
