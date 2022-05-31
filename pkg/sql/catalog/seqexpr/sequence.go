// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package seqexpr provides functionality to find usages of sequences in
// expressions.
//
// The logic here would fit nicely into schemaexpr if it weren't for the
// dependency on builtins, which itself depends on schemaexpr.
package seqexpr

import (
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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

	// Resolve doesn't use the searchPath for resolving FunctionDefinitions
	// so we can pass in an empty SearchPath.
	def, err := funcExpr.Func.Resolve(tree.EmptySearchPath)
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

// UpgradeSequenceReferenceInExpr upgrades all by-name reference in `expr` to by-ID.
// The name to ID resolution logic is aided by injecting a set of sequence names
// (`allUsedSeqNames`). This set is expected to contain names of all sequences used
// in `expr`, so all we need to do is to match each by-name seq reference in `expr`
// to one entry in `allUsedSeqNames`.
func UpgradeSequenceReferenceInExpr(
	expr *string, allUsedSeqNames map[descpb.ID]*tree.TableName,
) (hasUpgraded bool, err error) {
	// Find all sequence references in `expr`.
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		return hasUpgraded, err
	}
	seqRefs, err := GetUsedSequences(parsedExpr)
	if err != nil {
		return hasUpgraded, err
	}

	// Construct the key mapping from seq-by-name-reference to their IDs.
	seqByNameRefToID := make(map[string]int64)
	for _, seqIdentifier := range seqRefs {
		if seqIdentifier.IsByID() {
			continue
		}

		parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
		if err != nil {
			return hasUpgraded, err
		}
		seqByNameRefInTableName := parsedSeqName.ToTableName()

		// Pairing: find out which sequence name in `allUsedSeqNames` matches
		// `seqByNameRefInTableName` so we know the ID of this seq identifier.
		idOfSeqIdentifier, err := findUniqueBestMatchingForTableName(allUsedSeqNames, seqByNameRefInTableName)
		if err != nil {
			return hasUpgraded, err
		}

		seqByNameRefToID[seqIdentifier.SeqName] = int64(idOfSeqIdentifier)
	}

	// With this name-to-ID mapping, we can upgrade `expr`.
	newExpr, err := ReplaceSequenceNamesWithIDs(parsedExpr, seqByNameRefToID)
	if err != nil {
		return hasUpgraded, err
	}

	// Modify `expr` in place, if any upgrade.
	if *expr != tree.Serialize(newExpr) {
		hasUpgraded = true
		*expr = tree.Serialize(newExpr)
	}

	return hasUpgraded, nil
}

// findUniqueBestMatchingForTableName picks the "best-matching" name from `allTableNamesByID` for `tableName`.
// The best-matching name is the one that matches all parts of `tableName`.
//
// Example 1:
// 		allTableNamesByID = {23 : 'db.sc1.t', 25 : 'db.sc2.t'}
//		tableName = 'sc2.t'
//		return = 25 (because `db.sc2.t` best-matches `sc2.t`)
// Example 2:
// 		allTableNamesByID = {23 : 'db.sc1.t', 25 : 'sc2.t'}
//		tableName = 'sc2.t'
//		return = 25 (because `sc2.t` best-matches `sc2.t`)
//
// It returns a non-nill error if `tableName` does not uniquely match a name in `allTableNamesByID`.
//
// Example 3:
// 		allTableNamesByID = {23 : 'sc1.t', 25 : 'sc2.t'}
//		tableName = 't'
//		return = non-nil error (because both 'sc1.t' and 'sc2.t' are equally good matches
//	 			for 't' and we cannot decide,	i.e., >1 valid candidates left.)
// Example 4:
// 		allTableNamesByID = {23 : 'sc1.t', 25 : 'sc2.t'}
//		tableName = 't2'
//		return = non-nil error (because neither 'sc1.t' nor 'sc2.t' matches 't2', that is, 0 valid candidate left)
func findUniqueBestMatchingForTableName(
	allTableNamesByID map[descpb.ID]*tree.TableName, targetTableName tree.TableName,
) (descpb.ID, error) {
	candidates := make(map[descpb.ID]*tree.TableName)

	// Get all candidates whose table name is equal to `t`.
	t := targetTableName.Table()
	if t == "" {
		return descpb.InvalidID, errors.AssertionFailedf("input tableName does not have a Table field.")
	}
	for id, tableName := range allTableNamesByID {
		if tableName.Table() == t {
			candidates[id] = tableName
		}
	}
	if len(candidates) == 0 {
		return descpb.InvalidID, errors.AssertionFailedf("no table name found to match input %v", t)
	}

	// Eliminate candidates whose schema is not equal to `sc`.
	sc := targetTableName.Schema()
	if sc != "" {
		for id, candidateTableName := range candidates {
			if candidateTableName.Schema() != sc {
				delete(candidates, id)
			}
		}
	}
	// Eliminate candidates whose catalog is not equal to `db`.
	db := targetTableName.Catalog()
	if db != "" {
		for id, candidateTableName := range candidates {
			if candidateTableName.Catalog() != db {
				delete(candidates, id)
			}
		}
	}

	// There should be only one candidate left; Return errors accordingly if not.
	if len(candidates) == 0 {
		return descpb.InvalidID, errors.AssertionFailedf("no table name found to match input %v", t)
	}

	if len(candidates) > 1 {
		candidateTableNames := make([]string, 0)
		for _, candidateTableName := range candidates {
			candidateTableNames = append(candidateTableNames, candidateTableName.String())
		}
		return descpb.InvalidID, errors.AssertionFailedf("more than 1 matches found for %v: %v",
			targetTableName.String(), candidateTableNames)
	}

	// Get that only one candidate and return.
	var result descpb.ID
	for id := range candidates {
		result = id
	}
	return result, nil
}
