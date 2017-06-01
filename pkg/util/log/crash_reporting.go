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
	"path/filepath"
	"runtime"
	"runtime/debug"

	raven "github.com/getsentry/raven-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

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
var DiagnosticsReportingEnabled = settings.RegisterBoolSetting(
	"diagnostics.reporting.enabled",
	"enable reporting diagnostic metrics to cockroach labs",
	false,
)

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic(ctx context.Context) {
	if r := recover(); r != nil {
		ReportPanic(ctx, r, 1)
		panic(r)
	}
}

// A Safe panic can be reported verbatim, i.e. does not leak information.
//
// TODO(dt): flesh out, see #15892.
type Safe struct {
	V interface{}
}

func format(r interface{}) string {
	switch wrapped := r.(type) {
	case *Safe:
		return fmt.Sprintf("%+v", wrapped.V)
	case Safe:
		return fmt.Sprintf("%+v", wrapped.V)
	}
	return fmt.Sprintf("%T", r)
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, r interface{}, depth int) {
	Shout(ctx, Severity_ERROR, "a panic has occurred!")

	// TODO(dt,knz,sql-team): we need to audit all sprintf'ing of values into the
	// errors and strings passed to panic, to ensure raw user data is kept
	// separate and can thus be elided here. For now, the type is about all we can
	// assume is safe to report, which combined with file and line info should be
	// at least somewhat helpful in telling us where crashes are coming from.
	// We capture the full stacktrace below, so we only need the short file and
	// line here help uniquely identify the error.
	// Some exceptions, like a runtime.Error, are assumed to be fine as-is.

	var reportable interface{}
	switch r.(type) {
	case runtime.Error:
		reportable = r
	default:
		file, line, _ := caller.Lookup(depth + 3)
		reportable = fmt.Sprintf("%s %s:%d", format(r), filepath.Base(file), line)
	}
	sendCrashReport(ctx, reportable, depth+3)

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

var crashReports = settings.RegisterBoolSetting(
	"diagnostics.reporting.send_crash_reports",
	"send crash and panic reports",
	true,
)

// SetupCrashReporter sets the crash reporter info.
func SetupCrashReporter(ctx context.Context, cmd string) {
	url := envutil.EnvOrDefaultString(
		"COCKROACH_CRASH_REPORTS", "https://ignored:ignored@errors.cockroachdb.com/sentry",
	)
	if err := errors.Wrap(raven.SetDSN(url), "failed to setup crash reporting"); err != nil {
		panic(err)
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

var crdbPaths = []string{"github.com/cockroachdb/cockroach"}

func sendCrashReport(ctx context.Context, r interface{}, depth int) {
	if !DiagnosticsReportingEnabled.Get() || !crashReports.Get() {
		return // disabled via settings.
	}
	if raven.DefaultClient == nil {
		return // disabled via empty URL env var.
	}

	var err error
	if e, ok := r.(error); ok {
		err = e
	} else {
		err = fmt.Errorf("%v", r)
	}

	// This is close to inlining raven.CaptureErrorAndWait(), except it lets us
	// control the stack depth of the collected trace.
	const contextLines = 3

	ex := raven.NewException(err, raven.NewStacktrace(depth+1, contextLines, crdbPaths))
	packet := raven.NewPacket(err.Error(), ex)
	eventID, ch := raven.DefaultClient.Capture(packet, nil /* tags */)
	<-ch
	Shout(ctx, Severity_ERROR, "Reported as error "+eventID)
}
