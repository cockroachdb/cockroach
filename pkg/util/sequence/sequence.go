// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sequence

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// GetSequenceFromFunc extracts a sequence name from a FuncExpr if the function
// takes a sequence name as an arg. Returns the name of the sequence or nil
// if no sequence was found.
func GetSequenceFromFunc(funcExpr *tree.FuncExpr) (*string, error) {
	searchPath := sessiondata.SearchPath{}

	// Resolve doesn't use the searchPath for resolving FunctionDefinitions
	// so we can pass in an empty SearchPath.
	def, err := funcExpr.Func.Resolve(searchPath)
	if err != nil {
		return nil, err
	}

	fnProps, overloads := builtins.GetBuiltinProperties(def.Name)
	if fnProps != nil && fnProps.HasSequenceArguments {
		found := false
		for _, overload := range overloads {
			// Find the overload that matches funcExpr.
			if funcExpr.ResolvedOverload().Types.Match(overload.Types.Types()) {
				found = true
				argTypes, ok := overload.Types.(tree.ArgTypes)
				if !ok {
					panic(pgerror.Newf(
						pgcode.InvalidFunctionDefinition,
						"%s has invalid argument types", funcExpr.Func.String(),
					))
				}
				for i := 0; i < overload.Types.Length(); i++ {
					// Find the sequence name arg.
					argName := argTypes[i].Name
					if argName == builtins.SequenceNameArg {
						arg := funcExpr.Exprs[i]
						switch a := arg.(type) {
						case *tree.DString:
							seqName := string(*a)
							return &seqName, nil
						}
					}
				}
			}
		}
		if !found {
			panic(pgerror.New(
				pgcode.DatatypeMismatch,
				"could not find matching function overload for given arguments",
			))
		}
	}
	return nil, nil
}

// GetUsedSequenceNames returns the name of the sequence passed to
// a call to sequence function in the given expression or nil if no sequence
// names are found.
// e.g. nextval('foo') => "foo"; <some other expression> => nil
func GetUsedSequenceNames(defaultExpr tree.TypedExpr) ([]string, error) {
	var names []string
	_, err := tree.SimpleVisit(
		defaultExpr,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			switch t := expr.(type) {
			case *tree.FuncExpr:
				name, err := GetSequenceFromFunc(t)
				if err != nil {
					return false, nil, err
				}
				if name != nil {
					names = append(names, *name)
				}
			}
			return true, expr, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return names, nil
}
