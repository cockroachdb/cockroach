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

package telemetry

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	markerPath = os.TempDir()
	os.Exit(m.Run())
}

func TestNewTelemetrySource(t *testing.T) {
	a := assert.New(t)

	s, err := NewTelemetrySource("foo")
	if !a.NoError(err) {
		return
	}

	a.Equal("foo", s.Product)
	a.NotEqual("", s.InstallUUID)

	s2, err := NewTelemetrySource("foo")
	if !a.NoError(err) {
		return
	}

	// We shouldn't get the same instance, but expect the same data.
	a.True(s != s2)
	a.Equal(s, s2)

	s3, err := NewTelemetrySource("bar")
	if !a.NoError(err) {
		return
	}

	a.NotEqual(s, s3)
}

func TestTelemetrySource_SetUptime(t *testing.T) {
	a := assert.New(t)

	s, err := NewTelemetrySource("foo")
	if !a.NoError(err) {
		return
	}

	a.Equal(time.Duration(0), s.Uptime)
	a.Equal(s.SetUptime(), s.Uptime)
}
