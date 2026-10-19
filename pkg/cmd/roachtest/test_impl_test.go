// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTeamCityEscape(t *testing.T) {
	require.Equal(t, "|n", TeamCityEscape("\n"))
	require.Equal(t, "|r", TeamCityEscape("\r"))
	require.Equal(t, "||", TeamCityEscape("|"))
	require.Equal(t, "|[", TeamCityEscape("["))
	require.Equal(t, "|]", TeamCityEscape("]"))

	require.Equal(t, "identity", TeamCityEscape("identity"))
	require.Equal(t, "aaa|nbbb", TeamCityEscape("aaa\nbbb"))
	require.Equal(t, "aaa|nbbb||", TeamCityEscape("aaa\nbbb|"))
	require.Equal(t, "||||", TeamCityEscape("||"))
	require.Equal(t, "Connection to 104.196.113.229 port 22: Broken pipe|r|nlost connection: exit status 1",
		TeamCityEscape("Connection to 104.196.113.229 port 22: Broken pipe\r\nlost connection: exit status 1"))

	require.Equal(t,
		"Messages:   	current binary |'24.1|' not found in |'versionToMinSupportedVersion|'",
		TeamCityEscape("Messages:   	current binary '24.1' not found in 'versionToMinSupportedVersion'"),
	)

	// Unicode
	require.Equal(t, "|0x00bf", TeamCityEscape("\u00bf"))
	require.Equal(t, "|0x00bfaaa", TeamCityEscape("\u00bfaaa"))
	require.Equal(t, "bb|0x00bfaaa", TeamCityEscape("bb\u00bfaaa"))
}

type targetError struct {
	err error
}

func (te targetError) Error() string {
	return "TARGET_ERROR"
}

func Test_failuresMatchingError(t *testing.T) {
	createFailure := func(ref error, squashedErr error) failure {
		return failure{errors: []error{ref}, squashedErr: squashedErr}
	}
	type args struct {
		failures []failure
		refError targetError
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "empty failures",
			args: args{
				failures: []failure{},
				refError: targetError{errors.New("testerror")},
			},
			want: false,
		},
		{
			name: "failures contains expected error",
			args: args{
				failures: []failure{
					createFailure(targetError{errors.New("testerror")}, nil),
				},
				refError: targetError{errors.New("testerror")},
			},
			want: true,
		},
		{
			name: "non first failure contains expected error",
			args: args{
				failures: []failure{
					createFailure(errors.New("unexpected-error"), nil),
					createFailure(targetError{errors.New("expected-error")}, nil),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "first failure contains expected error",
			args: args{
				failures: []failure{
					createFailure(targetError{errors.New("expected-error")}, nil),
					createFailure(errors.New("unexpected-error"), nil),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "first failure's squashedErr contains expected error",
			args: args{
				failures: []failure{
					createFailure(nil, targetError{errors.New("expected-error")}),
					createFailure(errors.New("unexpected-error"), nil),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "non first failure's squashedErr contains expected error",
			args: args{
				failures: []failure{
					createFailure(errors.New("unexpected-error"), errors.New("unexpected-squashed-error")),
					createFailure(nil, targetError{errors.New("expected-error")}),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "both errors and squashedErr contains expected error",
			args: args{
				failures: []failure{
					createFailure(targetError{errors.New("expected-error")}, targetError{errors.New("expected-error")}),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "an error contains the expected error type, as part of a multi-error",
			args: args{
				failures: []failure{
					createFailure(
						// Errors that use the `Join` API are recognizable by the
						// flake detection logic. This test fails if we use
						// `CombineErrors`.
						errors.Join(errors.New("oops"), targetError{errors.New("expected-error")}),
						nil,
					),
				},
				refError: targetError{errors.New("some error")},
			},
			want: true,
		},
		{
			name: "single failure - none of errors or squashedErr contains expected error",
			args: args{
				failures: []failure{
					createFailure(errors.New("unexpected-error"), errors.New("unexpected-error1")),
				},
				refError: targetError{errors.New("some error")},
			},
			want: false,
		},
		{
			name: "multiple failures - none of errors or squashedErr contains expected error",
			args: args{
				failures: []failure{
					createFailure(errors.New("unexpected-error"), errors.New("unexpected-error1")),
					createFailure(errors.New("unexpected-error2"), errors.New("unexpected-error3")),
				},
				refError: targetError{errors.New("some error")},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, failuresMatchingError(tt.args.failures, &tt.args.refError), "failureMatchingError(%v, %v)", tt.args.failures, tt.args.refError)
		})
	}
}

func Test_failureSpecifyOwnerAndAddFailureCombination(t *testing.T) {
	ti := testImpl{}
	ti.ReplaceL(nilLogger())
	ti.addFailure(0, "", vmPreemptionError("my_VM"))
	errWithOwnership := failuresAsErrorWithOwnership(ti.failures())

	require.NotNil(t, errWithOwnership)
	require.Equal(t, registry.OwnerTestEng, errWithOwnership.Owner)
}

// TestCockroachBinaryPick verifies the metamorphic choice between the standard
// and runtime-assertions cockroach binaries made by testImpl.Cockroach.
func TestCockroachBinaryPick(t *testing.T) {
	defer func(p float64) { roachtestflags.CockroachEAProbability = p }(
		roachtestflags.CockroachEAProbability)
	// Always pick the runtime-assertions binary when it is eligible.
	roachtestflags.CockroachEAProbability = 1

	for _, tc := range []struct {
		name      string
		arch      vm.CPUArch
		benchmark bool
		binary    registry.ClusterCockroachBinary
		expected  string
	}{
		{name: "amd64", arch: vm.ArchAMD64, expected: "cockroach-ea"},
		{name: "arm64", arch: vm.ArchARM64, expected: "cockroach-ea"},
		{name: "fips", arch: vm.ArchFIPS, expected: "cockroach"},
		{name: "benchmark", arch: vm.ArchAMD64, benchmark: true, expected: "cockroach"},
		{
			// An explicit request for runtime assertions is honored on FIPS.
			name:     "fips explicit assertions",
			arch:     vm.ArchFIPS,
			binary:   registry.RuntimeAssertionsCockroach,
			expected: "cockroach-ea",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testT := &testImpl{
				spec: &registry.TestSpec{
					Name:            "binary-pick",
					Benchmark:       tc.benchmark,
					CockroachBinary: tc.binary,
				},
				cockroach:   "cockroach",
				cockroachEA: "cockroach-ea",
				arch:        tc.arch,
			}
			testL := nilLogger()
			t.Cleanup(testL.Close)
			testT.ReplaceL(testL)
			require.Equal(t, tc.expected, testT.Cockroach())
		})
	}
}
