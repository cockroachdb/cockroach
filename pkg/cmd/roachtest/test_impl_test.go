// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
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
	ti := testImpl{
		l: nilLogger(),
	}
	ti.addFailure(0, "", vmPreemptionError("my_VM"))
	errWithOwnership := failuresSpecifyOwner(ti.failures())

	require.NotNil(t, errWithOwnership)
	require.Equal(t, registry.OwnerTestEng, errWithOwnership.Owner)
}
