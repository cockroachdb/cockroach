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
	"context"
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	raven "github.com/getsentry/raven-go"
	"github.com/pkg/errors"
)

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
		ReportPanic(ctx, r)
		panic(r)
	}
}

// ReportPanic reports a panic has occurred on the real stderr.
func ReportPanic(ctx context.Context, r interface{}) {
	Shout(ctx, Severity_ERROR, "a panic has occurred!")

	if DiagnosticsReportingEnabled.Get() && crashReports.Get() {
		sendCrashReport(ctx, r)
	}

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
	raven.SetIncludePaths([]string{"github.com/cockroachdb/cockroach"})
	raven.SetTagsContext(map[string]string{
		"cmd":          cmd,
		"platform":     info.Platform,
		"distribution": info.Distribution,
		"rev":          info.Revision,
		"goversion":    info.GoVersion,
	})
}

func sendCrashReport(ctx context.Context, r interface{}) {
	if err, ok := r.(error); ok {
		id := raven.CaptureErrorAndWait(err, nil)
		Shout(ctx, Severity_ERROR, "Reported as error "+id)
	} else {
		id := raven.CaptureMessageAndWait(fmt.Sprint(r), nil)
		Shout(ctx, Severity_ERROR, "Reported as error "+id)
	}
}
