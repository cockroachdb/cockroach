// Copyright 2019 The Cockroach Authors.
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

import raven "github.com/getsentry/raven-go"

// InterceptingTransport is an implementation of raven.Transport that delegates
// calls to the Send method to the send function contained within.
type InterceptingTransport struct {
	// SendFunc is the send callback.
	SendFunc func(url, authHeader string, packet *raven.Packet)
}

// Send implements the raven.Transport interface.
func (it InterceptingTransport) Send(url, authHeader string, packet *raven.Packet) error {
	it.SendFunc(url, authHeader, packet)
	return nil
}

// TestingSetCrashReportingURL enables overriding the crash reporting URL
// for a test.
func TestingSetCrashReportingURL(url string) func() {
	oldCrashReportURL := crashReportURL
	crashReportURL = url
	return func() { crashReportURL = oldCrashReportURL }
}
