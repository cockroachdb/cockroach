// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/cockroachdb/version"
)

// TestBranchSelectionForVersionBump documents the expected behavior for which
// branches should receive version bump PRs based on the released version and
// next version. This test serves as documentation for the branch selection logic
// in generateRepoList.
func TestBranchSelectionForVersionBump(t *testing.T) {
	tests := []struct {
		name                 string
		releasedVersion      string
		nextVersion          string
		availableBranches    []string          // branches that exist in the repo
		currentVersions      map[string]string // branch -> current version.txt content
		expectedBumpBranches []string          // branches that should get version bump PRs
	}{
		{
			name:              "RC release bumps both RC and release branch",
			releasedVersion:   "v25.1.0-rc.1",
			nextVersion:       "v25.1.0-rc.2",
			availableBranches: []string{"release-25.1.0-rc", "release-25.1"},
			currentVersions: map[string]string{
				"release-25.1.0-rc": "v25.1.0-rc.1",
				"release-25.1":      "v25.1.0-rc.1",
			},
			expectedBumpBranches: []string{"release-25.1.0-rc", "release-25.1"},
		},
		{
			name:              "stable release bumps both RC and release branch",
			releasedVersion:   "v25.1.0",
			nextVersion:       "v25.1.1",
			availableBranches: []string{"release-25.1.0-rc", "release-25.1"},
			currentVersions: map[string]string{
				"release-25.1.0-rc": "v25.1.0",
				"release-25.1":      "v25.1.0",
			},
			expectedBumpBranches: []string{"release-25.1.0-rc", "release-25.1"},
		},
		{
			name:              "patch release bumps both patch RC and release branch",
			releasedVersion:   "v25.1.1",
			nextVersion:       "v25.1.2",
			availableBranches: []string{"release-25.1.1-rc", "release-25.1"},
			currentVersions: map[string]string{
				"release-25.1.1-rc": "v25.1.1",
				"release-25.1":      "v25.1.1",
			},
			expectedBumpBranches: []string{"release-25.1.1-rc", "release-25.1"},
		},
		{
			name:              "skips branches that already have next version",
			releasedVersion:   "v25.1.1",
			nextVersion:       "v25.1.2",
			availableBranches: []string{"release-25.1.1-rc", "release-25.1"},
			currentVersions: map[string]string{
				"release-25.1.1-rc": "v25.1.2", // already has next version
				"release-25.1":      "v25.1.1",
			},
			expectedBumpBranches: []string{"release-25.1"}, // only release-25.1 gets bumped
		},
		{
			name:              "skips staging branches",
			releasedVersion:   "v25.1.0",
			nextVersion:       "v25.1.1",
			availableBranches: []string{"staging-v25.1.0", "release-25.1"},
			currentVersions: map[string]string{
				"staging-v25.1.0": "v25.1.0",
				"release-25.1":    "v25.1.0",
			},
			expectedBumpBranches: []string{"release-25.1"}, // staging branches are skipped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test documents the expected behavior.
			// The actual implementation is in generateRepoList which requires git setup.
			// See update_versions.go lines 463-480 for the filtering logic.
			t.Logf("Released version: %s", tt.releasedVersion)
			t.Logf("Next version: %s", tt.nextVersion)
			t.Logf("Available branches: %v", tt.availableBranches)
			t.Logf("Expected branches to bump: %v", tt.expectedBumpBranches)
		})
	}
}

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
			if tt.wantErr {
				if err == nil {
					t.Errorf("bumpVersion() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			expected := version.MustParse(tt.nextVersion)
			if got != expected {
				t.Errorf("bumpVersion() got = %v, nextVersion %v", got, tt.nextVersion)
			}
		})
	}
}
