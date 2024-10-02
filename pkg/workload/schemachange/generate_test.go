// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"math/rand"
	"slices"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPickOne(t *testing.T) {
	require.NoError(t, quick.Check(func(seed int64, values []int) bool {
		r := rand.New(rand.NewSource(seed))
		picked, err := PickOne(r, values)

		// If no values are provided, ErrCaseNotPossible is always returned.
		if len(values) == 0 {
			return errors.Is(err, ErrCaseNotPossible)
		}

		// The picked value is always one of the provided options.
		return slices.Contains(values, picked)
	}, nil))
}

func TestPickAtLeast(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	require.Panics(t, func() {
		_, _ = PickAtLeast(r, -1, []int{})
	})

	out, err := PickAtLeast(r, 0, []int{})
	require.NoError(t, err)
	require.Equal(t, []int{}, out)

	_, err = PickAtLeast(r, 1, []int{})
	require.ErrorIs(t, err, ErrCaseNotPossible)

	out, err = PickAtLeast(r, 3, []int{1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, []int{1, 1, 1}, out)
}

func TestPickBetween(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	require.Panics(t, func() {
		_, _ = PickBetween(r, -1, -1, []int{})
	})

	require.Panics(t, func() {
		_, _ = PickBetween(r, 3, 1, []int{})
	})

	out, err := PickBetween(r, 0, 2, []int{})
	require.NoError(t, err)
	require.Equal(t, []int{}, out)

	_, err = PickBetween(r, 1, 2, []int{})
	require.ErrorIs(t, err, ErrCaseNotPossible)

	out, err = PickBetween(r, 2, 2, []int{1, 1, 1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, []int{1, 1}, out)
}

func TestValues(t *testing.T) {
	require.Equal(t, `false, 'some value', "DATA base"."SCHE ma"."TA ble"`, Values{
		tree.MakeDBool(false),
		tree.NewDString("some value"),
		tree.NewTableNameWithSchema("DATA base", "SCHE ma", "TA ble"),
	}.String())
}

func TestGenerate(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	funcs := map[string]any{
		"NotPossible": func() (string, error) {
			return "", ErrCaseNotPossible
		},
		"Possible": func() (string, error) {
			return "ALTER DATABASE MyDatabase RENAME TO NewName", nil
		},
		"Database": func() *tree.Name {
			db := tree.Name("MyDatabase")
			return &db
		},
	}

	t.Run("SyntaxError", func(t *testing.T) {
		_, _, err := Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database}`},
		}, funcs)
		require.EqualError(t, err, `syntax error; could not parse *tree.RenameDatabase: "ALTER DATABASE \"MyDatabase\""`)
	})

	t.Run("WrongType", func(t *testing.T) {
		_, _, err := Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `ALTER TABLE foo RENAME TO bar`},
		}, funcs)
		require.EqualError(t, err, `expected to parse *tree.RenameDatabase; got *tree.RenameTable: "ALTER TABLE foo RENAME TO bar"`)
	})

	// Possible SuccessfulCompletion cases are prioritized when produceError is
	// false.
	prioritizesPossibleSuccessfulCompletion := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		_, code, err := Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{NotPossible}`},
			{pgcode.SuccessfulCompletion, `{Possible}`},
			{pgcode.AdminShutdown, `{Possible}`},
			{pgcode.AmbiguousAlias, `{NotPossible}`},
		}, funcs)
		// The one possible SuccessfulCompletion case is always selected.
		return code == pgcode.SuccessfulCompletion && err == nil
	}

	// When produceError is false, if no successful completion cases are
	// possible, Generate will fallback to any possible error case.
	prioritizesPossibleCases := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		_, code, err := Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{NotPossible}`},
			{pgcode.AdminShutdown, `{NotPossible}`},
			{pgcode.AmbiguousAlias, `{Possible}`},
		}, funcs)
		// The only possible case is always selected.
		return code == pgcode.AmbiguousAlias && err == nil
	}

	// When produceError is true, all SuccessfulCompletion cases are ignored.
	ignoresSuccessfulCompletionWhenProduceError := func(seed int64) bool {
		r := rand.New(rand.NewSource(seed))
		_, _, err := Generate[*tree.RenameDatabase](r, true, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{NotPossible}`},
			{pgcode.SuccessfulCompletion, `{Possible}`},
			{pgcode.AdminShutdown, `{NotPossible}`},
		}, funcs)
		// There are no possible error cases so ErrCaseNotPossible is always
		// returned.
		return errors.Is(err, ErrCaseNotPossible)
	}

	// Assert that our properties are upheld.
	require.NoError(t, quick.Check(prioritizesPossibleCases, nil))
	require.NoError(t, quick.Check(prioritizesPossibleSuccessfulCompletion, nil))
	require.NoError(t, quick.Check(ignoresSuccessfulCompletionWhenProduceError, nil))
}
