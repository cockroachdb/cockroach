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
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// SeqIdentifier wraps together different ways of identifying a sequence.
// The sequence can either be identified via either its name, or its ID.
type SeqIdentifier struct {
	SeqName string
	SeqID   int64
}

// IsByID indicates whether the SeqIdentifier is identifying
// the sequence by its ID or by its name.
func (si *SeqIdentifier) IsByID() bool {
	return len(si.SeqName) == 0
}

// GetSequenceFromFunc extracts a sequence identifier from a FuncExpr if the function
// takes a sequence identifier as an arg (a sequence identifier can either be
// a sequence name or an ID), wrapped in the SeqIdentifier type.
// Returns the identifier of the sequence or nil if no sequence was found.
func GetSequenceFromFunc(funcExpr *tree.FuncExpr) (*SeqIdentifier, error) {
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
			if len(funcExpr.Exprs) == overload.Types.Length() {
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
						if seqIdentifier := getSequenceIdentifier(arg); seqIdentifier != nil {
							return seqIdentifier, nil
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

// getSequenceIdentifier takes a tree.Expr and extracts the
// sequence identifier (either its name or its ID) if it exists.
func getSequenceIdentifier(expr tree.Expr) *SeqIdentifier {
	switch a := expr.(type) {
	case *tree.DString:
		seqName := string(*a)
		return &SeqIdentifier{
			SeqName: seqName,
		}
	case *tree.DOid:
		id := int64(a.DInt)
		return &SeqIdentifier{
			SeqID: id,
		}
	case *tree.StrVal:
		seqName := a.RawString()
		return &SeqIdentifier{
			SeqName: seqName,
		}
	case *tree.NumVal:
		id, err := a.AsInt64()
		if err == nil {
			return &SeqIdentifier{
				SeqID: id,
			}
		}
	case *tree.CastExpr:
		return getSequenceIdentifier(a.Expr)
	case *tree.AnnotateTypeExpr:
		return getSequenceIdentifier(a.Expr)
	}
	return nil
}

// GetUsedSequences returns the identifier of the sequence passed to
// a call to sequence function in the given expression or nil if no sequence
// identifiers are found. The identifier is wrapped in a SeqIdentifier.
// e.g. nextval('foo') => "foo"; nextval(123::regclass) => 123; <some other expression> => nil
func GetUsedSequences(defaultExpr tree.Expr) ([]SeqIdentifier, error) {
	var seqIdentifiers []SeqIdentifier
	_, err := tree.SimpleVisit(
		defaultExpr,
		func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
			switch t := expr.(type) {
			case *tree.FuncExpr:
				identifier, err := GetSequenceFromFunc(t)
				if err != nil {
					return false, nil, err
				}
				if identifier != nil {
					seqIdentifiers = append(seqIdentifiers, *identifier)
				}
			}
			return true, expr, nil
		},
	)
	if err != nil {
		return nil, err
	}
	return seqIdentifiers, nil
}

// ReplaceSequenceNamesWithIDs walks the given expression, and replaces
// any sequence names in the expression by their IDs instead.
// e.g. nextval('foo') => nextval(123::regclass)
func ReplaceSequenceNamesWithIDs(
	defaultExpr tree.Expr, nameToID map[string]int64,
) (tree.Expr, error) {
	replaceFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		switch t := expr.(type) {
		case *tree.FuncExpr:
			identifier, err := GetSequenceFromFunc(t)
			if err != nil {
				return false, nil, err
			}
			if identifier == nil || identifier.IsByID() {
				return true, expr, nil
			}

			id, ok := nameToID[identifier.SeqName]
			if !ok {
				return true, expr, nil
			}
			return false, &tree.FuncExpr{
				Func: t.Func,
				Exprs: tree.Exprs{
					&tree.AnnotateTypeExpr{
						Type:       types.RegClass,
						SyntaxMode: tree.AnnotateShort,
						Expr:       tree.NewNumVal(constant.MakeInt64(id), "", false),
					},
				},
			}, nil
		}
		return true, expr, nil
	}

	newExpr, err := tree.SimpleVisit(defaultExpr, replaceFn)
	return newExpr, err
}
