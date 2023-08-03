// Copyright 2018 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	test2 "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterNodes(t *testing.T) {
	c := &clusterImpl{spec: spec.MakeClusterSpec(spec.GCE, "", 10)}
	opts := func(opts ...option.Option) []option.Option {
		return opts
	}
	testCases := []struct {
		opts     []option.Option
		expected string
	}{
		{opts(), ""},
		{opts(c.All()), ":1-10"},
		{opts(c.Range(1, 2)), ":1-2"},
		{opts(c.Range(2, 5)), ":2-5"},
		{opts(c.All(), c.Range(2, 5)), ":1-10"},
		{opts(c.Range(2, 5), c.Range(7, 9)), ":2-5,7-9"},
		{opts(c.Range(2, 5), c.Range(6, 8)), ":2-8"},
		{opts(c.Node(2), c.Node(4), c.Node(6)), ":2,4,6"},
		{opts(c.Node(2), c.Node(3), c.Node(4)), ":2-4"},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			nodes := c.MakeNodes(tc.opts...)
			if tc.expected != nodes {
				t.Fatalf("expected %s, but found %s", tc.expected, nodes)
			}
		})
	}
}

type testWrapper struct {
	*testing.T
	l *logger.Logger
}

func (t testWrapper) SnapshotPrefix() string {
	return ""
}

func (t testWrapper) BuildVersion() *version.Version {
	panic("implement me")
}

func (t testWrapper) Cockroach() string {
	return "./dummy-path/to/cockroach"
}

func (t testWrapper) CockroachShort() string {
	return "./dummy-path/to/cockroach-short"
}

func (t testWrapper) DeprecatedWorkload() string {
	return "./dummy-path/to/workload"
}

func (t testWrapper) IsBuildVersion(s string) bool {
	panic("implement me")
}

func (t testWrapper) Spec() interface{} {
	panic("implement me")
}

func (t testWrapper) VersionsBinaryOverride() map[string]string {
	panic("implement me")
}

func (t testWrapper) SkipInit() bool {
	panic("implement me")
}

func (t testWrapper) Progress(f float64) {
	panic("implement me")
}

func (t testWrapper) WorkerStatus(args ...interface{}) {
}

func (t testWrapper) WorkerProgress(f float64) {
	panic("implement me")
}

func (t testWrapper) IsDebug() bool {
	return false
}

var _ test2.Test = testWrapper{}

// ArtifactsDir is part of the test.Test interface.
func (t testWrapper) ArtifactsDir() string {
	return ""
}

// PerfArtifactsDir is part of the test.Test interface.
func (t testWrapper) PerfArtifactsDir() string {
	return ""
}

// logger is part of the testI interface.
func (t testWrapper) L() *logger.Logger {
	return t.l
}

// Status is part of the testI interface.
func (t testWrapper) Status(args ...interface{}) {}

func TestClusterMachineType(t *testing.T) {
	testCases := []struct {
		machineType      string
		expectedCPUCount int
	}{
		// AWS machine types
		{"m6i.large", 2},
		{"m6i.xlarge", 4},
		{"m6i.2xlarge", 8},
		{"m6i.4xlarge", 16},
		{"m6id.8xlarge", 32},
		{"m6id.12xlarge", 48},
		{"m6id.16xlarge", 64},
		{"m6i.24xlarge", 96},
		{"m6id.large", 2},
		{"m6id.xlarge", 4},
		{"m6id.2xlarge", 8},
		{"m6id.4xlarge", 16},
		{"m6id.8xlarge", 32},
		{"m6id.12xlarge", 48},
		{"m6id.16xlarge", 64},
		{"m6id.24xlarge", 96},
		{"c6id.large", 2},
		{"c6id.xlarge", 4},
		{"c6id.2xlarge", 8},
		{"c6id.4xlarge", 16},
		{"c6id.8xlarge", 32},
		{"c6id.12xlarge", 48},
		{"c6id.16xlarge", 64},
		{"c6id.24xlarge", 96},
		// GCE machine types
		{"n2-standard-2", 2},
		{"n2-standard-4", 4},
		{"n2-standard-8", 8},
		{"n2-standard-16", 16},
		{"n2-standard-32", 32},
		{"n2-standard-64", 64},
		{"n2-standard-96", 96},
		{"t2a-standard-2", 2},
		{"t2a-standard-4", 4},
		{"t2a-standard-8", 8},
		{"t2a-standard-16", 16},
		{"t2a-standard-32", 32},
		{"t2a-standard-48", 48},
	}
	for _, tc := range testCases {
		t.Run(tc.machineType, func(t *testing.T) {
			cpuCount := MachineTypeToCPUs(tc.machineType)
			if tc.expectedCPUCount != cpuCount {
				t.Fatalf("expected %d CPUs, but found %d", tc.expectedCPUCount, cpuCount)
			}
		})
	}
}

func TestCmdLogFileName(t *testing.T) {
	ts := time.Date(2000, 1, 1, 15, 4, 12, 0, time.Local)

	const exp = `run_150412.000000000_n1,3-4,9_cockroach-bla-foo-ba`
	nodes := option.NodeListOption{1, 3, 4, 9}
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach", "bla", "--foo", "bar"),
	)
	assert.Equal(t,
		exp,
		cmdLogFileName(ts, nodes, "./cockroach bla --foo bar"),
	)
}

func TestVerifyLibraries(t *testing.T) {
	originalLibraryPaths := libraryFilePaths
	defer func() { libraryFilePaths = originalLibraryPaths }()
	testCases := []struct {
		name             string
		verifyLibs       []string
		libraryFilePaths []string
		expectedError    error
	}{
		{
			name:             "valid nil input",
			verifyLibs:       nil,
			libraryFilePaths: []string{"/some/path/lib.so"},
			expectedError:    nil,
		},
		{
			name:             "no match",
			verifyLibs:       []string{"required_c"},
			libraryFilePaths: []string{"/some/path/lib.so"},
			expectedError: errors.Wrap(errors.Errorf("missing required library %s (arch=\"amd64\")",
				"required_c"), "cluster.VerifyLibraries"),
		},
		{
			name:             "no match on nil libs",
			verifyLibs:       []string{"required_b"},
			libraryFilePaths: nil,
			expectedError: errors.Wrap(errors.Errorf("missing required library %s (arch=\"amd64\")",
				"required_b"), "cluster.VerifyLibraries"),
		},
		{
			name:             "single match",
			verifyLibs:       []string{"geos"},
			libraryFilePaths: []string{"/lib/geos.so"},
			expectedError:    nil,
		},
		{
			name:             "single match, multiple extensions",
			verifyLibs:       []string{"geos"},
			libraryFilePaths: []string{"/lib/geos.linux-amd.so"},
			expectedError:    nil,
		},
		{
			name:             "multiple matches",
			verifyLibs:       []string{"lib", "ltwo", "geos"},
			libraryFilePaths: []string{"ltwo.so", "a/geos.so", "/some/path/to/lib.so"},
			expectedError:    nil,
		},
		{
			name:             "multiple matches, multiple extensions",
			verifyLibs:       []string{"lib", "ltwo", "geos"},
			libraryFilePaths: []string{"ltwo.linux-arm64.so", "a/geos.linux-amd64.fips.so", "/some/path/to/lib.darwin-arm64.so"},
			expectedError:    nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			libraryFilePaths = map[vm.CPUArch][]string{vm.ArchAMD64: tc.libraryFilePaths}
			actualError := VerifyLibraries(tc.verifyLibs, vm.ArchAMD64)
			if tc.expectedError == nil {
				require.NoError(t, actualError)
			} else {
				require.NotNil(t, actualError)
				require.EqualError(t, actualError, tc.expectedError.Error())
			}
		})
	}
}
