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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	raven "github.com/getsentry/raven-go"
)

func TestCrashReportingFormatSave(t *testing.T) {
	r1 := "i am hidden"
	r2 := Safe{V: "i am public"}
	r3 := Safe{V: &r2}
	f1, f2, f3 := format(r1), format(r2), format(r3)
	exp1, exp2 := "string", r2.V.(string)
	exp3 := "&{V:i am public}"
	if f1 != exp1 {
		t.Errorf("wanted %s, got %s", exp1, f1)
	}
	if f2 != exp2 {
		t.Errorf("wanted %s, got %s", exp2, f2)
	}
	if f3 != exp3 {
		t.Errorf("wanted %s, got %s", exp3, f3)
	}
}

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
	ctx := context.Background()
	numReported := 0

	// Temporarily enable all crash-reporting settings.
	defer settings.TestingSetBool(&DiagnosticsReportingEnabled, true)()
	defer func(url string) { crashReportURL = url }(crashReportURL)
	crashReportURL = "https://ignored:ignored@ignored/ignored"

	// Install a Transport that performs basic sanity checking on packets rather
	// than sending them to Sentry over HTTP.
	defer func(transport raven.Transport) {
		raven.DefaultClient.Transport = transport
	}(raven.DefaultClient.Transport)
	raven.DefaultClient.Transport = interceptingTransport{
		send: func(url, authHeader string, packet *raven.Packet) {
			numReported++
			if e, a := 64, len(packet.ServerName); e != a {
				t.Errorf("expected hashed server name '%s' to be %d bytes, but was %d bytes", packet.ServerName, e, a)
			}
			if e, a := 5, len(packet.Tags); e != a {
				t.Errorf("expected %d tags, but got %d", e, a)
			}
		},
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected test to panic")
		}
		if e, a := 1, numReported; e != a {
			t.Fatalf("expected to see %d crash reports, but saw %d", e, a)
		}
	}()

	defer RecoverAndReportPanic(ctx)
	SetupCrashReporter(ctx, "test")

	panic("oh te noes!")
}
