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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
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

// MaybeUpgradeSequenceReferenceToByID upgrades all by-name sequence references in
// `tableDesc` into by-ID references.
// It returns IDs of all sequences that were referenced by-name previously in `tableDesc`
// and have then been upgraded to by-ID by this method.
func MaybeUpgradeSequenceReferenceToByID(
	descLookup *nstree.Map, tableDesc *descpb.TableDescriptor,
) (upgradedSeqIDs []descpb.ID, err error) {
	if tableDesc.IsTable() {
		upgradedSeqIDs, err = maybeUpgradeSequenceReferenceForTable(descLookup, tableDesc)
		if err != nil {
			return nil, err
		}
	} else if tableDesc.IsView() {
		upgradedSeqIDs, err = maybeUpgradeSequenceReferenceForView(descLookup, tableDesc)
		if err != nil {
			return nil, err
		}
	}

	return upgradedSeqIDs, nil
}

// upgradeSequenceReferenceInExpr upgrades all by-name reference in `expr` to by-ID.
// The name to ID resolution logic is aided by injecting a set of fully resolve sequence
// names and their IDs (`fullyResolvedSequenceNames`), so all we need to do is to match
// each by-name seq reference in `expr` to a fully resolved seq name.
func upgradeSequenceReferenceInExpr(
	expr *string, fullyResolvedSequenceNames map[descpb.ID]*tree.TableName,
) (upgradedSeqIDs []descpb.ID, err error) {
	// Parse expression `expr`.
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		return nil, err
	}

	// Retrieve all sequence identifier in the default expression.
	seqIdentifiers, err := GetUsedSequences(parsedExpr)
	if err != nil {
		return nil, err
	}

	// For each by-name sequence reference, find the sequence ID, and add a corresponding entry in `seqIdentifierToID`.
	seqIdentifierToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		if seqIdentifier.IsByID() {
			continue
		}
		// This sequence identifier is by name! Need to find out ID
		// of the referenced sequence, so we can replace the by-name reference
		// to by ID.
		parsedSeqName, err := parser.ParseTableName(seqIdentifier.SeqName)
		if err != nil {
			return nil, err
		}
		seqIdentifierInTableName := parsedSeqName.ToTableName()

		// Pairing: find out which full sequence name in `usedSeqIDToFullName` matches
		// `seqIdentifierInTableName` so we know the ID of this seq identifier.
		idOfSeqIdentifier, err := tableNameMatching(fullyResolvedSequenceNames, seqIdentifierInTableName)
		if err != nil {
			return nil, err
		}

		seqIdentifierToID[seqIdentifier.SeqName] = int64(idOfSeqIdentifier)

		// Record that `idOfSeqIdentifier` will be the ID of an upgrade sequence.
		upgradedSeqIDs = append(upgradedSeqIDs, idOfSeqIdentifier)
	}

	// With the name-to-ID mapping for all sequences in the default expression, we can
	// replace all by-name references to by-ID references.
	newExpr, err := ReplaceSequenceNamesWithIDs(parsedExpr, seqIdentifierToID)
	if err != nil {
		return nil, err
	}

	// Modify `expr` in place.
	*expr = tree.Serialize(newExpr)

	return upgradedSeqIDs, nil
}

// maybeUpgradeSequenceReferenceForTable upgrades all by-name sequence references
// in `tableDesc` to by-ID.
// The difficulty is how do we resolve sequence names to their IDs. Fortunately,
// each column in `tableDesc` has a field `usesSequenceIDs` that stores all the
// sequence IDs this column uses. We can then use the `descLookupFn` to retrieve
// fully resolved names of those used sequences and them into a (ID --> fully resolved name)
// mapping (call it `fullyResolvedSeqNames`).
// Finally, we can do a name matching for each by-name seq reference `seqIdentifer`
// in this column to determine its ID. That is, find the entry in `fullyResolvedSeqNames` whose
// fully resolved name matches `seqIdentifier`.
func maybeUpgradeSequenceReferenceForTable(
	descLookup *nstree.Map, tableDesc *descpb.TableDescriptor,
) (upgradedSeqIDs []descpb.ID, err error) {
	if !tableDesc.IsTable() {
		return nil, nil
	}

	for _, col := range tableDesc.Columns {
		// Find fully resolve sequence names for all sequences used in this column.
		fullyResolvedSequenceNames, err := getFullyResolvedTableNamesForIDs(descLookup, col.UsesSequenceIds)
		if err != nil {
			return nil, err
		}

		// Upgrade sequence reference in DEFAULT expression, if any.
		if col.HasDefault() {
			upgradedSeqIDsInExpr, err := upgradeSequenceReferenceInExpr(col.DefaultExpr, fullyResolvedSequenceNames)
			if err != nil {
				return nil, err
			}
			upgradedSeqIDs = append(upgradedSeqIDs, upgradedSeqIDsInExpr...)
		}

		// Upgrade sequence reference in ON UPDATE expression, if any.
		if col.HasOnUpdate() {
			upgradedSeqIDsInExpr, err := upgradeSequenceReferenceInExpr(col.OnUpdateExpr, fullyResolvedSequenceNames)
			if err != nil {
				return nil, err
			}
			upgradedSeqIDs = append(upgradedSeqIDs, upgradedSeqIDsInExpr...)
		}
	}

	return upgradedSeqIDs, nil
}

// maybeUpgradeSequenceReferenceForView similarily upgrades all by-name sequence references
// in `viewDesc` to by-ID.
func maybeUpgradeSequenceReferenceForView(
	descLookup *nstree.Map, viewDesc *descpb.TableDescriptor,
) (upgradedSeqIDs []descpb.ID, err error) {
	if !viewDesc.IsView() {
		return nil, nil
	}

	// Retrieve all sequence descriptor IDs.
	allSeqIDs := make([]descpb.ID, 0)
	if err = descLookup.IterateByID(func(entry catalog.NameEntry) error {
		desc, ok := entry.(catalog.Descriptor)
		if !ok {
			return errors.AssertionFailedf("`descLookup` contains a entry that is not a `catalog.Descriptor`.")
		}
		if desc.DescriptorType() == catalog.Table && desc.(catalog.TableDescriptor).IsSequence() {
			allSeqIDs = append(allSeqIDs, desc.GetID())
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Find fully resolve sequence names for all sequences stored in the map, which should
	// include all that appear in the view descriptor's query.
	fullyResolvedSequenceNames, err := getFullyResolvedTableNamesForIDs(descLookup, allSeqIDs)
	if err != nil {
		return nil, err
	}

	// A function that looks at an expression and replace any by-name sequence reference with
	// by-ID reference. It, of course, also append replaced sequence IDs to `upgradedSeqIDs`.
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		newExprStr := expr.String()
		upgradedSeqIDsInExpr, err := upgradeSequenceReferenceInExpr(&newExprStr, fullyResolvedSequenceNames)
		if err != nil {
			return false, expr, err
		}
		upgradedSeqIDs = append(upgradedSeqIDs, upgradedSeqIDsInExpr...)
		newExpr, err = parser.ParseExpr(newExprStr)
		if err != nil {
			return false, expr, err
		}

		return false, newExpr, nil
	}

	stmt, err := parser.ParseOne(viewDesc.GetViewQuery())
	if err != nil {
		return nil, err
	}

	// Invoke the "replace" function defined above on each expression in this view's query statement.
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceSeqFunc)
	if err != nil {
		return nil, err
	}

	viewDesc.ViewQuery = newStmt.String()

	return upgradedSeqIDs, nil
}

// Find the fully resolved table names for `ids`.
// Pre-condition: `ids` must identify table descriptors.
func getFullyResolvedTableNamesForIDs(
	descLookup *nstree.Map, ids []descpb.ID,
) (map[descpb.ID]*tree.TableName, error) {
	result := make(map[descpb.ID]*tree.TableName)

	for _, id := range ids {
		if _, exists := result[id]; exists {
			continue
		}

		// Retrieve the table descriptor.
		tableDesc, ok := descLookup.GetByID(id).(catalog.TableDescriptor)
		if !ok || tableDesc == nil {
			return nil, errors.AssertionFailedf("descriptor ID %v does not identify a "+
				"table descriptor.", id)
		}

		// Also retrieve its database and schema descriptor.
		parentID := tableDesc.GetParentID()
		parentSchemaID := tableDesc.GetParentSchemaID()

		dbDesc, ok := descLookup.GetByID(parentID).(catalog.DatabaseDescriptor)
		if !ok || dbDesc == nil {
			return nil, errors.AssertionFailedf("descriptor ID %v does not identify a "+
				"database descriptor.", parentID)
		}

		scDesc, ok := descLookup.GetByID(parentSchemaID).(catalog.SchemaDescriptor)
		scName := ""
		if !ok || scDesc == nil {
			if parentSchemaID == keys.PublicSchemaIDForBackup {
				// For backups created in 21.2 and prior, the "public" schema is descriptorless,
				// and always uses the const `keys.PublicSchemaIDForBackUp` as the "public"
				// schema ID.
				scName = tree.PublicSchema
			} else {
				return nil, errors.AssertionFailedf("descriptor ID %v does not identify a "+
					"schema descriptor.", parentSchemaID)
			}
		} else {
			scName = scDesc.GetName()
		}

		// Construct a fully resolved name for this used sequence.
		result[id] = tree.NewTableNameWithSchema(
			tree.Name(dbDesc.GetName()),
			tree.Name(scName),
			tree.Name(tableDesc.GetName()),
		)
	}

	return result, nil
}

// tableNameMatching picks the entry from `idToFullTableNames` that has a full table name
// that matches `tableName` (which is possibly not fully resolved).
// It returns `InvalidID` if `tableName` does not match any fully resolved name in `fullyResolvedTableNamesByID`.
//
// E.g. idToFullTableNames = {23 : 'db.sc1.t', 25 : 'db.sc2.t'}
//			tableName = 'sc2.t'
//			return = 25 (because `db.sc2.t` matches `sc2.t`)
func tableNameMatching(
	fullyResolvedTableNamesByID map[descpb.ID]*tree.TableName, tableName tree.TableName,
) (descpb.ID, error) {
	candidates := make(map[descpb.ID]bool)

	// Get all candidates whose table name is equal to `t`.
	t := tableName.Table()
	if t == "" {
		return descpb.InvalidID, errors.AssertionFailedf("input tableName does not have a Table field.")
	}
	for id, fullTableName := range fullyResolvedTableNamesByID {
		if fullTableName.Table() == t {
			candidates[id] = true
		}
	}
	if len(candidates) == 0 {
		// no fully resolved name has a table name that equals `t`
		return descpb.InvalidID, errors.AssertionFailedf("no fully resolve name equals input table name %v", t)
	}

	// Eliminate candidates whose schema is not equal to `sc`.
	sc := tableName.Schema()
	if sc != "" {
		for id := range candidates {
			if fullyResolvedTableNamesByID[id].Schema() != sc {
				delete(candidates, id)
			}
		}
	}

	// Eliminate candidates whose catalog is not equal to `db`.
	db := tableName.Catalog()
	if db != "" {
		for id := range candidates {
			if fullyResolvedTableNamesByID[id].Catalog() != db {
				delete(candidates, id)
			}
		}
	}

	// There should be only one candidate left; Return errors accordingly if not.
	if len(candidates) == 0 {
		allFullyResolvedTableNames := make([]string, 0)
		for _, fullyResolvedTableName := range fullyResolvedTableNamesByID {
			allFullyResolvedTableNames = append(allFullyResolvedTableNames, fullyResolvedTableName.String())
		}
		return descpb.InvalidID, errors.AssertionFailedf("No matching name can be found for %v from %v.",
			tableName.String(), allFullyResolvedTableNames)
	}

	if len(candidates) > 1 {
		candidateTableNames := make([]string, 0)
		for id := range candidates {
			candidateTableNames = append(candidateTableNames, fullyResolvedTableNamesByID[id].String())
		}
		return descpb.InvalidID, errors.AssertionFailedf("Cannot find a unique matching name for %v; "+
			"Candidates = %v", tableName.String(), candidateTableNames)
	}

	// Get that only one candidate and return.
	var result descpb.ID
	for id := range candidates {
		result = id
	}
	return result, nil
}
