// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logcrash_test

import (
	"context"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/sentry-go"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line crash_reporting_packet_test.go:1000

// interceptingTransport is an implementation of raven.Transport that delegates
// calls to the Send method to the send function contained within.
type interceptingTransport struct {
	send func(packet *sentry.Event)
}

// SendEvent implements the sentry.Transport interface.
func (it interceptingTransport) SendEvent(packet *sentry.Event) {
	it.send(packet)
}

// Flush implements the sentry.Transport interface.
func (it interceptingTransport) Flush(time.Duration) bool { return true }

// Configure implements the sentry.Transport interface.
func (it interceptingTransport) Configure(sentry.ClientOptions) {}

func TestCrashReportingPacket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var packets []*sentry.Event

	st := cluster.MakeTestingClusterSettings()
	// Enable all crash-reporting settings.
	logcrash.DiagnosticsReportingEnabled.Override(ctx, &st.SV, true)

	defer logcrash.TestingSetCrashReportingURL("https://ignored:ignored@ignored/1234")()

	logcrash.SetupCrashReporter(ctx, "test")

	// Install a Transport that locally records events rather than sending them
	// to Sentry over HTTP.
	defer func(transport sentry.Transport) {
		sentry.CurrentHub().Client().Transport = transport
	}(sentry.CurrentHub().Client().Transport)
	sentry.CurrentHub().Client().Transport = interceptingTransport{
		send: func(packet *sentry.Event) {
			packets = append(packets, packet)
		},
	}

	expectPanic := func(name string) {
		if r := recover(); r == nil {
			t.Fatalf("'%s' failed to panic", name)
		}
	}

	const (
		panicPre  = "boom"
		panicPost = "baam"
	)

	func() {
		defer expectPanic("before server start")
		defer logcrash.RecoverAndReportPanic(ctx, &st.SV)
		panic(log.Safe(panicPre))
	}()

	func() {
		defer expectPanic("after server start")
		defer logcrash.RecoverAndReportPanic(ctx, &st.SV)
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		s.Stopper().Stop(ctx)
		panic(log.Safe(panicPost))
	}()

	const prefix = "crash_reporting_packet_test.go:"

	type extraPair struct {
		key   string
		reVal string
	}
	expectations := []struct {
		serverID *regexp.Regexp
		tagCount int
		message  string
		extra    []extraPair
	}{
		{regexp.MustCompile(`^$`), 7, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1058"
			}
			message += " (TestCrashReportingPacket)"
			return message
		}(), []extraPair{
			{"1: details", "panic: " + panicPre},
		}},
		{regexp.MustCompile(`^[a-z0-9]{8}-1$`), 12, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1066"
			}
			message += " (TestCrashReportingPacket)"
			return message
		}(), []extraPair{
			{"1: details", "panic: " + panicPost},
		}},
	}

	if e, a := len(expectations), len(packets); e != a {
		t.Fatalf("expected %d packets, but got %d", e, a)
	}

	for _, exp := range expectations {
		p := packets[0]
		packets = packets[1:]
		t.Run("", func(t *testing.T) {
			if !logcrash.ReportSensitiveDetails {
				e, a := "<redacted>", p.ServerName
				if e != a {
					t.Errorf("expected ServerName to be '<redacted>', but got '%s'", a)
				}
			}

			tags := p.Tags
			if e, a := exp.tagCount, len(tags); e != a {
				t.Errorf("expected %d tags, but got %d", e, a)
			}

			if serverID := tags["server_id"]; !exp.serverID.MatchString(serverID) {
				t.Errorf("expected server_id '%s' to match %s", serverID, exp.serverID)
			}

			if len(p.Exception) < 1 {
				t.Error("expected some exception in packet, got none")
			} else {
				if p.Exception[0].Type != exp.message {
					t.Errorf("expected %q in exception type, got %q",
						exp.message, p.Exception[0].Type)
				}

				lastFrame := p.Exception[0].Stacktrace.Frames[len(p.Exception[0].Stacktrace.Frames)-1]
				if lastFrame.Function != "TestCrashReportingPacket" {
					t.Errorf("last frame function: expected TestCrashReportingPacket, got %q", lastFrame.Function)
				}
				if !strings.HasSuffix(lastFrame.Filename, "crash_reporting_packet_test.go") {
					t.Errorf("last frame filename: expected crash_reporting_packet_test.go, got %q", lastFrame.Filename)
				}
			}

			for _, ex := range exp.extra {
				data, ok := p.Extra[ex.key]
				if !ok {
					t.Errorf("expected detail %q in extras, was not found", ex.key)
					continue
				}
				sdata, ok := data.(string)
				if !ok {
					t.Errorf("expected detail %q of type string, found %T (%q)", ex.key, data, data)
					continue
				}
				assert.Regexp(t, ex.reVal, sdata)
			}
		})
	}
}

func TestInternalErrorReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var packets []*sentry.Event

	st := cluster.MakeTestingClusterSettings()
	// Enable all crash-reporting settings.
	logcrash.DiagnosticsReportingEnabled.Override(ctx, &st.SV, true)

	defer logcrash.TestingSetCrashReportingURL("https://ignored:ignored@ignored/1234")()

	logcrash.SetupCrashReporter(ctx, "test")

	// Install a Transport that locally records packets rather than sending them
	// to Sentry over HTTP.
	defer func(transport sentry.Transport) {
		sentry.CurrentHub().Client().Transport = transport
	}(sentry.CurrentHub().Client().Transport)
	sentry.CurrentHub().Client().Transport = interceptingTransport{
		send: func(packet *sentry.Event) {
			packets = append(packets, packet)
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec("SELECT crdb_internal.force_assertion_error('woo')"); !testutils.IsError(err, "internal error") {
		t.Fatalf("expected internal error, got %v", err)
	}

	// TODO(knz): With Ben's changes to report errors in pgwire as well
	// as the executor, we get twice the sentry reports. This needs to
	// be fixed, then the test below can check for len(packets) == 1.
	if len(packets) < 1 {
		t.Fatalf("expected at least 1 reporting packet")
	}

	p := packets[0]

	t.Logf("%# v", pretty.Formatter(p))

	// rm is the redaction mark, what remains after redaction when
	// the redaction markers are removed.
	rm := string(redact.RedactableBytes(redact.RedactedMarker()).StripMarkers())

	assert.Regexp(t, `builtins\.go:\d+: crdb_internal.force_assertion_error\(\): `+rm+`\n`+
		`--\n`+
		`\*errutil.leafError: `+rm+` \(1\)\n`+
		`builtins.go:\d+: \*withstack.withStack \(top exception\)\n`+
		`\*assert.withAssertionFailure\n`+
		`\*errutil.withPrefix: crdb_internal.force_assertion_error\(\) \(2\)\n`+
		`eval.go:\d+: \*withstack.withStack \(3\)\n`+
		`\*telemetrykeys.withTelemetry: crdb_internal.force_assertion_error\(\) \(4\)\n`+
		`\*colexecerror.notInternalError\n`+
		`\(check the extra data payloads\)`, p.Message)

	expectedExtra := []struct {
		key   string
		reVal string
	}{
		{"1: details", rm},
		{"2: details", `crdb_internal\.force_assertion_error\(\)`},
		{"4: details", `crdb_internal\.force_assertion_error\(\)`},
	}
	for _, ex := range expectedExtra {
		data, ok := p.Extra[ex.key]
		if !ok {
			t.Errorf("expected detail %q in extras, was not found", ex.key)
			continue
		}
		sdata, ok := data.(string)
		if !ok {
			t.Errorf("expected detail %q of type string, found %T (%q)", ex.key, data, data)
			continue
		}
		assert.Regexp(t, ex.reVal, sdata)
	}

	if len(p.Exception) < 2 {
		t.Fatalf("expected 2 stacktraces, got %d", len(p.Exception))
	}

	// The innermost stack trace (and main exception object) is the last
	// one in the Sentry event.
	assert.Regexp(t, `^builtins.go:\d+ \(.*\)$`, p.Exception[1].Type)
	assert.Regexp(t, `^\*errutil\.leafError: crdb_internal\.force_assertion_error\(\): `+rm, p.Exception[1].Value)
	fr := p.Exception[1].Stacktrace.Frames
	assert.Regexp(t, `.*/builtins.go`, fr[len(fr)-1].Filename)
	assert.Regexp(t, `.*/eval.go`, fr[len(fr)-2].Filename)

	assert.Regexp(t, `^\(3\) eval.go:\d+ \(MaybeWrapError\)$`, p.Exception[0].Type)
	assert.Regexp(t, `^\*withstack\.withStack$`, p.Exception[0].Value)
	fr = p.Exception[0].Stacktrace.Frames
	assert.Regexp(t, `.*/eval.go`, fr[len(fr)-1].Filename)
}
