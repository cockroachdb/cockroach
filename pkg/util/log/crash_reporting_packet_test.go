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

package log_test

import (
	"context"
	"regexp"
	"runtime"
	"testing"

	raven "github.com/getsentry/raven-go"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// interceptingTransport is an implementation of raven.Transport that delegates
// calls to the Send method to the send function contained within.
type interceptingTransport struct {
	send func(url, authHeader string, packet *raven.Packet)
}

// Send implements the raven.Transport interface.
func (it interceptingTransport) Send(url, authHeader string, packet *raven.Packet) error {
	it.send(url, authHeader, packet)
	return nil
}

func TestCrashReportingPacket(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer raven.Close()

	ctx := context.Background()
	var packets []*raven.Packet

	st := cluster.MakeTestingClusterSettings()
	// Enable all crash-reporting settings.
	log.DiagnosticsReportingEnabled.Override(&st.SV, true)

	defer log.TestingSetCrashReportingURL("https://ignored:ignored@ignored/ignored")()

	// Install a Transport that locally records packets rather than sending them
	// to Sentry over HTTP.
	defer func(transport raven.Transport) {
		raven.DefaultClient.Transport = transport
	}(raven.DefaultClient.Transport)
	raven.DefaultClient.Transport = interceptingTransport{
		send: func(_, _ string, packet *raven.Packet) {
			packets = append(packets, packet)
		},
	}

	expectPanic := func(name string) {
		if r := recover(); r == nil {
			t.Fatalf("'%s' failed to panic", name)
		}
	}

	log.SetupCrashReporter(ctx, "test")

	const (
		panicPre  = "boom"
		panicPost = "baam"
	)

	func() {
		defer expectPanic("before server start")
		defer log.RecoverAndReportPanic(ctx, &st.SV)
		panic(log.Safe(panicPre))
	}()

	func() {
		defer expectPanic("after server start")
		defer log.RecoverAndReportPanic(ctx, &st.SV)
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		s.Stopper().Stop(ctx)
		panic(log.Safe(panicPost))
	}()

	const prefix = "crash_reporting_packet_test.go:"

	expectations := []struct {
		serverID *regexp.Regexp
		tagCount int
		message  string
	}{
		{regexp.MustCompile(`^$`), 6, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "81"
			} else {
				message += "84"
			}
			message += ": " + panicPre
			return message
		}()},
		{regexp.MustCompile(`^[a-z0-9]{8}-1$`), 9, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "87"
			} else {
				message += "92"
			}
			message += ": " + panicPost
			return message
		}()},
	}

	if e, a := len(expectations), len(packets); e != a {
		t.Fatalf("expected %d packets, but got %d", e, a)
	}

	for _, exp := range expectations {
		p := packets[0]
		packets = packets[1:]
		t.Run("", func(t *testing.T) {
			if !log.ReportSensitiveDetails {
				e, a := "<redacted>", p.ServerName
				if e != a {
					t.Errorf("expected ServerName to be '<redacted>', but got '%s'", a)
				}
			}

			tags := make(map[string]string, len(p.Tags))
			for _, tag := range p.Tags {
				tags[tag.Key] = tag.Value
			}

			if e, a := exp.tagCount, len(tags); e != a {
				t.Errorf("expected %d tags, but got %d", e, a)
			}

			if serverID := tags["server_id"]; !exp.serverID.MatchString(serverID) {
				t.Errorf("expected server_id '%s' to match %s", serverID, exp.serverID)
			}

			if msg := p.Message; msg != exp.message {
				t.Errorf("expected %s, got %s", exp.message, msg)
			}
		})
	}
}
