// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package seqexpr provides functionality to find usages of sequences in
// expressions.
//
// The logic here would fit nicely into schemaexpr if it weren't for the
// dependency on builtins, which itself depends on schemaexpr.
package seqexpr

import (
	"context"
	"go/constant"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
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
//
// `getBuiltinProperties` argument is commonly builtinsregistry.GetBuiltinProperties.
func GetSequenceFromFunc(funcExpr *tree.FuncExpr) (*SeqIdentifier, error) {

	// Resolve doesn't use the searchPath for resolving FunctionDefinitions
	// so we can pass in an empty SearchPath.
	// TODO(mgartner): Plumb a function resolver and ctx here, or determine that
	// the function should have already been resolved.
	// TODO(chengxiong): Since we have funcExpr here, it's possible to narrow down
	// overloads by using input types.
	if ref, ok := funcExpr.Func.FunctionReference.(*tree.FunctionOID); ok && funcdesc.IsOIDUserDefinedFunc(ref.OID) {
		// If it's a user defined function OID, then we know that it's not a
		// sequence reference.
		return nil, nil
	}
	def, err := funcExpr.Func.Resolve(context.Background(), tree.EmptySearchPath, nil /* resolver */)
	if err != nil {
		// We have expression sanitization and type checking to make sure functions
		// exists and type is valid, so here if function is not found, it must be a
		// user defined function. We don't need to get sequences reference from it.
		if errors.Is(err, tree.ErrRoutineUndefined) {
			return nil, nil
		}
		return nil, err
	}

	hasSequenceArguments, err := def.GetHasSequenceArguments()
	if err != nil {
		return nil, err
	}

	if hasSequenceArguments {
		found := false
		for _, overload := range def.Overloads {
			// Find the overload that matches funcExpr.
			if len(funcExpr.Exprs) == overload.Types.Length() {
				paramTypes, ok := overload.Types.(tree.ParamTypes)
				if !ok {
					return nil, pgerror.Newf(
						pgcode.InvalidFunctionDefinition,
						"%s has invalid argument types", funcExpr.Func.String(),
					)
				}
				found = true
				for i := 0; i < len(paramTypes); i++ {
					// Find the sequence name param.
					if paramTypes[i].Name == builtinconstants.SequenceNameArg {
						arg := funcExpr.Exprs[i]
						if seqIdentifier := getSequenceIdentifier(arg); seqIdentifier != nil {
							return seqIdentifier, nil
						}
					}
				}
			}
		}
		if !found {
			return nil, pgerror.New(
				pgcode.DatatypeMismatch,
				"could not find matching function overload for given arguments",
			)
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
		id := int64(a.Oid)
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
//
// `getBuiltinProperties` argument is commonly builtinsregistry.GetBuiltinProperties.
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
//
// `getBuiltinProperties` argument is commonly builtinsregistry.GetBuiltinProperties.
func ReplaceSequenceNamesWithIDs(
	defaultExpr tree.Expr, nameToID map[string]descpb.ID,
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
						Expr:       tree.NewNumVal(constant.MakeInt64(int64(id)), "", false),
					},
				},
			}, nil
		}
		return true, expr, nil
	}

	newExpr, err := tree.SimpleVisit(defaultExpr, replaceFn)
	return newExpr, err
}

// UpgradeSequenceReferenceInExpr upgrades all by-name sequence
// reference in `expr` to by-ID with a provided id-to-name
// mapping `usedSequenceIDsToNames`, from which we should be able
// to uniquely determine the ID of each by-name seq reference.
//
// Such a mapping can often be constructed if we know the sequence IDs
// used in a particular expression, e.g. a column descriptor's
// `usesSequenceIDs` field or a view descriptor's `dependsOn` field if
// the column DEFAULT/ON-UPDATE or the view's query references sequences.
//
// `getBuiltinProperties` argument is commonly builtinsregistry.GetBuiltinProperties.
func UpgradeSequenceReferenceInExpr(
	expr *string, usedSequenceIDsToNames map[descpb.ID]*tree.TableName,
) (hasUpgraded bool, err error) {
	// Find the "reverse" mapping from sequence name to their IDs for those
	// sequences referenced by-name in `expr`.
	usedSequenceNamesToIDs, err := seqNameToIDMappingInExpr(*expr, usedSequenceIDsToNames)
	if err != nil {
		return false, err
	}

	// With this "reverse" mapping, we can simply replace each by-name
	// seq reference in `expr` with the sequence's ID.
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		return false, err
	}

	newExpr, err := ReplaceSequenceNamesWithIDs(parsedExpr, usedSequenceNamesToIDs)
	if err != nil {
		return false, err
	}

	// Modify `expr` in place, if any upgrade.
	if *expr != tree.Serialize(newExpr) {
		hasUpgraded = true
		*expr = tree.Serialize(newExpr)
	}

	return hasUpgraded, nil
}

// seqNameToIDMappingInExpr attempts to find the seq ID for
// every by-name seq reference in `expr` from `seqIDToNameMapping`.
// This process can be thought of as a "reverse mapping" process
// where, given an id-to-seq-name mapping, for each by-name seq reference
// in `expr`, we attempt to find the entry in that mapping such that
// the entry's name "best matches" the by-name seq reference.
// See comments of findUniqueBestMatchingForTableName for "best matching" definition.
//
// It returns a non-nill error if zero or multiple entries
// in `seqIDToNameMapping` have a name that "best matches"
// the by-name seq reference.
//
// See its unit test for some examples.
func seqNameToIDMappingInExpr(
	expr string, seqIDToNameMapping map[descpb.ID]*tree.TableName,
) (map[string]descpb.ID, error) {
	parsedExpr, err := parser.ParseExpr(expr)
	if err != nil {
		return nil, err
	}
	seqRefs, err := GetUsedSequences(parsedExpr)
	if err != nil {
		return nil, err
	}

	// Construct the key mapping from seq-by-name-reference to their IDs.
	result := make(map[string]descpb.ID)
	for _, seqIdentifier := range seqRefs {
		if seqIdentifier.IsByID() {
			continue
		}

		parsedSeqName, err := parser.ParseQualifiedTableName(seqIdentifier.SeqName)
		if err != nil {
			return nil, err
		}

		// Pairing: find out which sequence name in the id-to-name mapping
		// (i.e. `seqIDToNameMapping`) matches `parsedSeqName` so we
		// know the ID of it.
		idOfSeqIdentifier, err := findUniqueBestMatchingForTableName(seqIDToNameMapping, *parsedSeqName)
		if err != nil {
			return nil, err
		}

		// Put it to the reverse mapping.
		result[seqIdentifier.SeqName] = idOfSeqIdentifier
	}
	return result, nil
}

// findUniqueBestMatchingForTableName picks the "best-matching" name from
// `allTableNamesByID` for `targetTableName`. The best-matching name is the
// one that matches all parts of `targetTableName`, if that part exists
// in both names.
// Example 1:
//
//	allTableNamesByID = {23 : 'db.sc1.t', 25 : 'db.sc2.t'}
//	tableName = 'sc2.t'
//	return = 25 (because `db.sc2.t` best-matches `sc2.t`)
//
// Example 2:
//
//	allTableNamesByID = {23 : 'db.sc1.t', 25 : 'sc2.t'}
//	tableName = 'sc2.t'
//	return = 25 (because `sc2.t` best-matches `sc2.t`)
//
// Example 3:
//
//	allTableNamesByID = {23 : 'db.sc1.t', 25 : 'sc2.t'}
//	tableName = 'db.sc2.t'
//	return = 25 (because `sc2.t` best-matches `db.sc2.t`)
//
// Example 4:
//
//		allTableNamesByID = {23 : 'sc1.t', 25 : 'sc2.t'}
//		tableName = 't'
//		return = non-nil error (because both 'sc1.t' and 'sc2.t' are equally good matches
//	 			for 't' and we cannot decide,	i.e., >1 valid candidates left.)
//
// Example 5:
//
//	allTableNamesByID = {23 : 'sc1.t', 25 : 'sc2.t'}
//	tableName = 't2'
//	return = non-nil error (because neither 'sc1.t' nor 'sc2.t' matches 't2', that is, 0 valid candidate left)
func findUniqueBestMatchingForTableName(
	allTableNamesByID map[descpb.ID]*tree.TableName, targetTableName tree.TableName,
) (match descpb.ID, err error) {
	t := targetTableName.Table()
	if t == "" {
		return descpb.InvalidID, errors.AssertionFailedf("input tableName does not have a Table field.")
	}

	for id, candidateTableName := range allTableNamesByID {
		ct, tt := candidateTableName.Table(), targetTableName.Table()
		cs, ts := candidateTableName.Schema(), targetTableName.Schema()
		cdb, tdb := candidateTableName.Catalog(), targetTableName.Catalog()
		if (ct != "" && tt != "" && ct != tt) ||
			(cs != "" && ts != "" && cs != ts) ||
			(cdb != "" && tdb != "" && cdb != tdb) {
			// not a match -- there is a part, either db or schema or table name,
			// that exists in both names but they don't match.
			continue
		}

		// id passes the check; consider it as the result
		// If already found a valid result, report error!
		if match != descpb.InvalidID {
			return descpb.InvalidID, errors.AssertionFailedf("more than 1 matches found for %q",
				targetTableName.String())
		}
		match = id
	}

	if match == descpb.InvalidID {
		return descpb.InvalidID, errors.AssertionFailedf("no table name found to match input %q", t)
	}

	return match, nil
}
