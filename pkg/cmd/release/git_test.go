// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestIsStagingBranch(t *testing.T) {
	tests := []struct {
		description        string
		maybeStagingBranch string
		want               bool
	}{
		// happy path tests: should return true
		{
			description:        "should match `release-*-rc` staging branch",
			maybeStagingBranch: "release-23.1.4-rc",
			want:               true,
		},
		{
			description:        "should match `release-*-rc` staging branch",
			maybeStagingBranch: "release-23.0.44-rc",
			want:               true,
		},
		{
			description:        "should match `staging-*` staging branch",
			maybeStagingBranch: "staging-v23.1.4",
			want:               true,
		},
		{
			description:        "should match `staging-*` staging branch",
			maybeStagingBranch: "staging-v23.0.44",
			want:               true,
		},
		// should return false for:
		// - main release branches, e.g. release-23.1
		// - doesn't begin with release or staging
		// - has extra stuff before/after expected pattern
		{
			description:        "should not match main release branch",
			maybeStagingBranch: "release-23.2",
			want:               false,
		},
		{
			description:        "should not match something with extra stuff before pattern",
			maybeStagingBranch: "extra-release-23.2.1-rc",
			want:               false,
		},
		{
			description:        "should not match something with extra stuff after pattern",
			maybeStagingBranch: "release-23.2.1-rc-extra",
			want:               false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.maybeStagingBranch+": "+tt.description, func(t *testing.T) {
			require.Equal(t, tt.want, IsStagingBranch(tt.maybeStagingBranch))
		})
	}
}
