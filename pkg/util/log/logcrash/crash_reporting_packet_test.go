// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	sentry "github.com/getsentry/sentry-go"
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
	defer log.Scope(t).Close(t)

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
		panic(redact.Safe(panicPre))
	}()

	func() {
		defer expectPanic("after server start")
		defer logcrash.RecoverAndReportPanic(ctx, &st.SV)
		s := serverutils.StartServerOnly(t, base.TestServerArgs{DefaultTestTenant: base.TestNeedsTightIntegrationBetweenAPIsAndTestingKnobs})
		s.Stopper().Stop(ctx)
		panic(redact.Safe(panicPost))
	}()

	const prefix = "crash_reporting_packet_test.go:"

	expectations := []struct {
		serverID *regexp.Regexp
		tagCount int
		title    string
		message  *regexp.Regexp
	}{
		{regexp.MustCompile(`^$`), 9, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1059"
			}
			message += " (TestCrashReportingPacket)"
			return message
		}(),
			regexp.MustCompile(`crash_reporting_packet_test.go:\d+: panic: boom`),
		},
		{regexp.MustCompile(`^[a-z0-9]{8}-1$`), 13, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1067"
			}
			message += " (TestCrashReportingPacket)"
			return message
		}(),
			regexp.MustCompile(`crash_reporting_packet_test.go:\d+: panic: baam`),
		},
	}

	if e, a := len(expectations), len(packets); e != a {
		t.Fatalf("expected %d packets, but got %d", e, a)
	}

	for _, exp := range expectations {
		p := packets[0]
		packets = packets[1:]
		t.Run("", func(t *testing.T) {
			t.Logf("message: %q", p.Message)

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

			assert.Regexp(t, exp.message, p.Message)

			if len(p.Exception) < 1 {
				t.Error("expected some exception in packet, got none")
			} else {
				if p.Exception[0].Type != exp.title {
					t.Errorf("expected %q in exception type, got %q",
						exp.title, p.Exception[0].Type)
				}

				lastFrame := p.Exception[0].Stacktrace.Frames[len(p.Exception[0].Stacktrace.Frames)-1]
				if lastFrame.Function != "TestCrashReportingPacket" {
					t.Errorf("last frame function: expected TestCrashReportingPacket, got %q", lastFrame.Function)
				}
				if !strings.HasSuffix(lastFrame.Filename, "crash_reporting_packet_test.go") {
					t.Errorf("last frame filename: expected crash_reporting_packet_test.go, got %q", lastFrame.Filename)
				}
			}
		})
	}
}

func TestInternalErrorReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	if len(packets) != 1 {
		t.Fatalf("expected exactly 1 reporting packet")
	}
	p := packets[0]

	// rm is the redaction mark, what remains after redaction when
	// the redaction markers are removed.
	rm := string(redact.RedactableBytes(redact.RedactedMarker()).StripMarkers())

	assert.Regexp(t, `builtins\.go:\d+: `+rm+`\n`, p.Message)
	idx := strings.Index(p.Message, "-- report composition:\n")
	assert.GreaterOrEqual(t, idx, 1)
	if idx > 0 {
		assert.Regexp(t,
			`-- report composition:\n`+
				`\*errutil.leafError: `+rm+`\n`+
				`builtins.go:\d+: \*withstack.withStack \(top exception\)\n`+
				`\*assert.withAssertionFailure\n`+
				`\*colexecerror.notInternalError`, p.Message[idx:])
	}

	if len(p.Exception) != 1 {
		t.Fatalf("expected 1 stacktrace, got %d", len(p.Exception))
	}

	extra, ok := p.Extra["error types"]
	assert.True(t, ok)
	if ok {
		assert.Equal(t, "github.com/cockroachdb/errors/errutil/*errutil.leafError (*::)\n"+
			"github.com/cockroachdb/errors/withstack/*withstack.withStack (*::)\n"+
			"github.com/cockroachdb/errors/assert/*assert.withAssertionFailure (*::)\n"+
			"github.com/cockroachdb/cockroach/pkg/sql/colexecerror/*colexecerror.notInternalError (*::)\n",
			extra)
	}

	assert.Regexp(t, `^builtins.go:\d+ \(.*\)$`, p.Exception[0].Type)
	assert.Regexp(t, `^\*errutil\.leafError: `+rm, p.Exception[0].Value)
	fr := p.Exception[0].Stacktrace.Frames
	assert.Regexp(t, `.*/builtins.go`, fr[len(fr)-1].Filename)
	assert.Regexp(t, `.*/expr.go`, fr[len(fr)-2].Filename)
}
