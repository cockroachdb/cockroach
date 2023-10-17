// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestPickOne(t *testing.T) {
	r := rand.New(rand.NewSource(0))

	v, err := PickOne(r, []int{0, 1, 2, 3, 4})
	require.NoError(t, err)
	require.Equal(t, 4, v)

	_, err = PickOne(r, ([]int)(nil))
	require.ErrorIs(t, err, ErrCaseNotPossible)
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
		"Database": func() *tree.Name {
			db := tree.Name("MyDatabase")
			return &db
		},
		"ReturnErrCaseNotPossible": func() (tree.Name, error) {
			return "", ErrCaseNotPossible
		},
	}

	t.Run("Successful", func(t *testing.T) {
		// Successful cases will be prioritized when they are possible and
		// produceError is false.
		stmt, code, err := Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{ReturnErrCaseNotPossible}`},
			{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
		}, funcs)
		require.NoError(t, err)
		require.Equal(t, pgcode.SuccessfulCompletion, code)
		require.Equal(t, &tree.RenameDatabase{
			Name:    "MyDatabase",
			NewName: "newname",
		}, stmt)
	})

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

	t.Run("NoCasesPossible", func(t *testing.T) {
		_, _, err := Generate[*tree.RenameDatabase](r, false, nil, funcs)
		require.ErrorIs(t, err, ErrCaseNotPossible)

		_, _, err = Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{ReturnErrCaseNotPossible}`},
		}, funcs)
		require.ErrorIs(t, err, ErrCaseNotPossible)

		// Cases will be tried until we either exhaust all cases or we find a valid
		// case.
		_, _, err = Generate[*tree.RenameDatabase](r, false, []GenerationCase{
			{pgcode.SuccessfulCompletion, `{ReturnErrCaseNotPossible}`},
			{pgcode.SuccessfulCompletion, `{ReturnErrCaseNotPossible}`},
			{pgcode.SuccessfulCompletion, `{ReturnErrCaseNotPossible}`},
			{pgcode.SuccessfulCompletion, `ALTER DATABASE foo RENAME TO bar`},
		}, funcs)
		require.NoError(t, err)
	})

	t.Run("ProduceError", func(t *testing.T) {
		// If the only cases provided are successes and produceError is true, we'll
		// error out with case not possible.
		_, _, err := Generate[*tree.RenameDatabase](r, true, []GenerationCase{
			{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
			{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
			{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
		}, funcs)
		require.ErrorIs(t, err, ErrCaseNotPossible)

		// Assert that only error cases will be selected if produceError is true.
		for i := 0; i < 100; i++ {
			_, _, err := Generate[*tree.RenameDatabase](r, true, []GenerationCase{
				{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
				{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
				{pgcode.SuccessfulCompletion, `ALTER DATABASE {Database} RENAME TO NewName`},
				{pgcode.ReservedName, `ALTER DATABASE {Database} RENAME TO SomeReservedName`},
			}, funcs)
			require.NoError(t, err)
		}
	})
}
