// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// sequenceFuncs is a set of sequence functions that take in a sequence name
// (string) as an argument and can be used in a scalar expression.
// TODO(richardjcai): When implicit casting is supported, these builtins
// should take RegClass as the arg type for the sequence name instead of
// string, we will add a dependency on all RegClass types used in a view.
var sequenceFuncs = map[string]struct{}{
	"nextval": {},
	"currval": {},
}

// GetSequenceFromFunc extracts a sequence name from a FuncExpr if the function
// takes a sequence name as an arg. Returns the name of the sequence and a bool
// representing whether a sequence name was found or not.
func GetSequenceFromFunc(funcExpr *tree.FuncExpr) (string, bool, error) {
	searchPath := sessiondata.SearchPath{}

	def, err := funcExpr.Func.Resolve(searchPath)
	if err != nil {
		return "", false, err
	}

	if _, ok := sequenceFuncs[def.Name]; ok {
		arg := funcExpr.Exprs[0]
		switch a := arg.(type) {
		case *tree.DString:
			return string(*a), true, nil
		}
	}
	return "", false, nil
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
				name, found, err := GetSequenceFromFunc(t)
				if err != nil {
					return false, nil, err
				}
				if found {
					names = append(names, name)
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
