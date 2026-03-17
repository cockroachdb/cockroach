// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgerror

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPLpgSQLContext(t *testing.T) {
	testCases := []struct {
		err             error
		expectedContext string
	}{
		{
			WithPLpgSQLContext(fmt.Errorf("division by zero"),
				"PL/pgSQL function inline_code_block line 5 at assignment"),
			"PL/pgSQL function inline_code_block line 5 at assignment",
		},
		{
			// Nested context: outermost wrapping should be returned by errors.As
			// (which finds the first match in the chain), but the innermost wrapping
			// is what we want. Since errors.As finds the outermost, we rely on
			// the GetPLpgSQLContext == "" check in handleException to only set
			// context once (innermost).
			WithPLpgSQLContext(
				WithPLpgSQLContext(fmt.Errorf("test"), "inner context"),
				"outer context",
			),
			"outer context",
		},
		{
			WithPLpgSQLContext(
				WithCandidateCode(fmt.Errorf("test"), pgcode.DivisionByZero),
				"PL/pgSQL function myfunc line 3 at SQL statement",
			),
			"PL/pgSQL function myfunc line 3 at SQL statement",
		},
		{
			New(pgcode.Uncategorized, "i am an error"),
			"",
		},
		{
			WithCandidateCode(
				WithPLpgSQLContext(errors.Newf("test"), "PL/pgSQL function f line 1 at IF"),
				pgcode.System,
			),
			"PL/pgSQL function f line 1 at IF",
		},
		{
			fmt.Errorf("something else"),
			"",
		},
		{
			WithPLpgSQLContext(nil, "should not panic"),
			"",
		},
	}

	for _, tc := range testCases {
		name := "nil"
		if tc.err != nil {
			name = tc.err.Error()
		}
		t.Run(name, func(t *testing.T) {
			ctx := GetPLpgSQLContext(tc.err)
			require.Equal(t, tc.expectedContext, ctx)
			if tc.err == nil {
				return
			}
			// Test that the context survives an encode/decode cycle.
			enc := errors.EncodeError(context.Background(), tc.err)
			err2 := errors.DecodeError(context.Background(), enc)
			ctx = GetPLpgSQLContext(err2)
			require.Equal(t, tc.expectedContext, ctx)
		})
	}
}

func TestPLpgSQLContextFlatten(t *testing.T) {
	err := WithPLpgSQLContext(
		WithCandidateCode(fmt.Errorf("division by zero"), pgcode.DivisionByZero),
		"PL/pgSQL function inline_code_block line 5 at assignment",
	)
	pgErr := Flatten(err)
	require.Equal(t, "PL/pgSQL function inline_code_block line 5 at assignment", pgErr.Context)
	require.Equal(t, pgcode.DivisionByZero.String(), pgErr.Code)
	require.Equal(t, "division by zero", pgErr.Message)
}

func TestPLpgSQLContextFlattenEmpty(t *testing.T) {
	err := fmt.Errorf("no context here")
	pgErr := Flatten(err)
	require.Equal(t, "", pgErr.Context)
}
