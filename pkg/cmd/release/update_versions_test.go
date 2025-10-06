// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/Masterminds/semver/v3"
)

func Test_nextReleaseSeries(t *testing.T) {
	tests := []struct {
		version string
		want    string
	}{
		{"21.1.0", "21.2"},
		{"21.1.9", "21.2"},
		{"21.2.0", "22.1"},
		{"21.2.111", "22.1"},
		{"21.6.0", "22.1"},
		{"21.6.12", "22.1"},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			ver, err := semver.NewVersion(tt.version)
			if err != nil {
				t.Errorf("cannot parse %s", tt.version)
			}
			if got := nextReleaseSeries(ver); got != tt.want {
				t.Errorf("nextReleaseSeries(%s) = %v, want %v", ver, got, tt.want)
			}
		})
	}
}
