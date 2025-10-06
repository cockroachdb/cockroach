// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "testing"

func TestBumpVersion(t *testing.T) {
	tests := []struct {
		version     string
		nextVersion string
		wantErr     bool
	}{
		{
			version:     "v23.1.0",
			nextVersion: "v23.1.1",
			wantErr:     false,
		},
		{
			version:     "v23.1.1",
			nextVersion: "v23.1.2",
			wantErr:     false,
		},
		{
			version:     "v23.1.9",
			nextVersion: "v23.1.10",
			wantErr:     false,
		},
		{
			version:     "v23.2.0",
			nextVersion: "v23.2.1",
			wantErr:     false,
		},
		{
			version:     "v23.2.1",
			nextVersion: "v23.2.2",
			wantErr:     false,
		},
		{
			version:     "v23.2.9",
			nextVersion: "v23.2.10",
			wantErr:     false,
		},
		{
			version:     "v23.2.0-alpha.00000000",
			nextVersion: "v23.2.0-alpha.1",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-alpha.0",
			nextVersion: "v23.1.0-alpha.1",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-alpha.1",
			nextVersion: "v23.1.0-alpha.2",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-alpha.9",
			nextVersion: "v23.1.0-alpha.10",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-beta.0",
			nextVersion: "v23.1.0-beta.1",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-beta.1",
			nextVersion: "v23.1.0-beta.2",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-beta.9",
			nextVersion: "v23.1.0-beta.10",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-rc.0",
			nextVersion: "v23.1.0-rc.1",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-rc.1",
			nextVersion: "v23.1.0-rc.2",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-rc.9",
			nextVersion: "v23.1.0-rc.10",
			wantErr:     false,
		},
		{
			version:     "v23.1.0-rc9",
			nextVersion: "",
			wantErr:     true,
		},
		{
			version:     "v23.1.0-beta",
			nextVersion: "",
			wantErr:     true,
		},
		{
			version:     "v23.1.0-alpha-3",
			nextVersion: "",
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got, err := bumpVersion(tt.version)
			if (err != nil) != tt.wantErr {
				t.Errorf("bumpVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.nextVersion {
				t.Errorf("bumpVersion() got = %v, nextVersion %v", got, tt.nextVersion)
			}
		})
	}
}
