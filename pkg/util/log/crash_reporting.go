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
	"sync/atomic"

	raven "github.com/getsentry/raven-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// ReportingSettings avoids a dependency on pkg/settings/cluster.
type ReportingSettings interface {
	HasDiagnosticsReportingEnabled() bool
	HasCrashReportsEnabled() bool
}

// ReportingSettingsSingleton is a global that holds the ReportingSettings of an
// active instance of cluster.Settings. In a regular running node, there is
// exactly one such instance. In multi-node testing, there may be many, and so
// it is not clear which one will win out, but that is acceptable.
//
// The global is a compromise to free callers of log.Fatal{,f} from having to
// provide their cluster's settings on every call.
//
// NB: We have to store a pointer to the ReportSettings interface or we trigger
// assertions inside of atomic.Value since we pass two types here (one from
// cluster.Settings, one dummy one in tests).
var ReportingSettingsSingleton atomic.Value // stores *ReportingSettings

// RecoverAndReportPanic can be invoked on goroutines that run with
// stderr redirected to logs to ensure the user gets informed on the
// real stderr a panic has occurred.
func RecoverAndReportPanic(ctx context.Context, st ReportingSettings) {
	if r := recover(); r != nil {
		ReportPanic(ctx, st, r, 1)
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
func ReportPanic(ctx context.Context, st ReportingSettings, r interface{}, depth int) {
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
	sendCrashReport(ctx, st, reportable, depth+3)

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

var crdbPaths = []string{"github.com/cockroachdb/cockroach"}

func sendCrashReport(ctx context.Context, st ReportingSettings, r interface{}, depth int) {
	if !st.HasDiagnosticsReportingEnabled() || !st.HasCrashReportsEnabled() {
		return // disabled via settings.
	}
	// FIXME(tschottdorf): this particular method would benefit from globals
	// mirroring the server settings. Callers coming through log.Fatal will
	// pass a nil ReportingSettings above, which is pretty bad. OTOH, perhaps
	// they should just pass ReportingSettings directly through log.Fatal.

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
	// Avoid leaking the machine's hostname by injecting the literal "<redacted>".
	// Otherwise, raven.Client.Capture will see an empty ServerName field and
	// automatically fill in the machine's hostname.
	packet.ServerName = "<redacted>"
	eventID, ch := raven.DefaultClient.Capture(packet, nil /* tags */)
	<-ch
	Shout(ctx, Severity_ERROR, "Reported as error "+eventID)
}
