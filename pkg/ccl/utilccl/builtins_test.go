// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func unexportedFunction(s string) string {
	return "behold: " + s
}

func TestRegister(t *testing.T) {
	RegisterCCLBuiltin("test_builtin_from_unexported_function", "", unexportedFunction)
	RegisterCCLBuiltin("test_builtin_with_error", "", func(s string) (string, error) {
		if s == "dog" {
			return "pet the dog", nil
		}
		return s, errors.New("please provide dog")
	})
	resolver := func(fn string) (*tree.FunctionDefinition, error) {
		name := tree.UnresolvedName{NumParts: 1, Parts: [4]string{fn}}
		return name.ResolveFunction(nil)
	}
	wrapper, err := resolver("test_builtin_from_unexported_function")
	require.NoError(t, err)
	require.Equal(t, "(: string) -> string", wrapper.Definition[0].(*tree.Overload).Signature(true))

	args := tree.Datums{tree.NewDString("dog")}
	ctx := eval.MakeTestingEvalContext(nil)
	o, err := wrapper.Definition[0].(*tree.Overload).Fn.(eval.FnOverload)(&ctx, args)
	require.NoError(t, err)
	require.Equal(t, o.ResolvedType(), types.String)
	require.Equal(t, "'behold: dog'", o.String())

	petter, err := resolver("test_builtin_with_error")
	require.NoError(t, err)
	require.Equal(t, "(: string) -> string", petter.Definition[0].(*tree.Overload).Signature(true))

	_, err = petter.Definition[0].(*tree.Overload).Fn.(eval.FnOverload)(&ctx, args)
	require.NoError(t, err)

	args[0] = tree.NewDString("no dog")
	_, err = petter.Definition[0].(*tree.Overload).Fn.(eval.FnOverload)(&ctx, args)
	require.Error(t, err)

}
