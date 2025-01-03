// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rewrite

import (
	"go/constant"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// TableDescs mutates tables to match the ID and privilege specified
// in descriptorRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func TableDescs(
	tables []*tabledesc.Mutable, descriptorRewrites jobspb.DescRewriteMap, overrideDB string,
) error {
	for _, table := range tables {
		tableRewrite, ok := descriptorRewrites[table.ID]
		if !ok {
			return errors.Errorf("missing table rewrite for table %d", table.ID)
		}
		// Reset the version and modification time on this new descriptor.
		table.Version = 1
		table.ModificationTime = hlc.Timestamp{}

		if table.IsView() && overrideDB != "" {
			// restore checks that all dependencies are also being restored, but if
			// the restore is overriding the destination database, qualifiers in the
			// view query string may be wrong. Since the destination override is
			// applied to everything being restored, anything the view query
			// references will be in the override DB post-restore, so all database
			// qualifiers in the view query should be replaced with overrideDB.
			if err := rewriteViewQueryDBNames(table, overrideDB); err != nil {
				return err
			}
		}
		if err := rewriteSchemaChangerState(table, descriptorRewrites); err != nil {
			return err
		}

		table.ID = tableRewrite.ID
		table.UnexposedParentSchemaID = tableRewrite.ParentSchemaID
		table.ParentID = tableRewrite.ParentID

		// Rewrite CHECK constraints before function IDs in expressions are
		// rewritten. Check constraint mutations are also dropped if any function
		// referenced are missing.
		if err := dropCheckConstraintMissingDeps(table, descriptorRewrites); err != nil {
			return err
		}

		// Drop column expressions if referenced UDFs not found.
		if err := dropColumnExpressionsMissingDeps(table, descriptorRewrites); err != nil {
			return err
		}

		// Drop triggers if referenced tables, types, or routines not found.
		dropTriggerMissingDeps(table, descriptorRewrites)

		// Remap type IDs and sequence IDs in all serialized expressions within the
		// TableDescriptor.
		// TODO (rohany): This needs tests once partial indexes are ready.
		if err := tabledesc.ForEachExprStringInTableDesc(table,
			func(expr *string, typ catalog.DescExprType) error {
				switch typ {
				case catalog.SQLExpr:
					newExpr, err := rewriteTypesInExpr(*expr, descriptorRewrites)
					if err != nil {
						return err
					}
					*expr = newExpr

					newExpr, err = rewriteSequencesInExpr(*expr, descriptorRewrites)
					if err != nil {
						return err
					}
					*expr = newExpr

					newExpr, err = rewriteFunctionsInExpr(*expr, descriptorRewrites)
					if err != nil {
						return err
					}
					*expr = newExpr
				case catalog.SQLStmt, catalog.PLpgSQLStmt:
					lang := catpb.Function_SQL
					if typ == catalog.PLpgSQLStmt {
						lang = catpb.Function_PLPGSQL
					}
					newExpr, err := rewriteRoutineBody(descriptorRewrites, *expr, overrideDB, lang)
					if err != nil {
						return err
					}
					*expr = newExpr
				default:
					return errors.AssertionFailedf("unexpected expression type")
				}
				return nil
			},
		); err != nil {
			return err
		}

		// Walk view query and remap sequence IDs.
		if table.IsView() {
			viewQuery, err := rewriteSequencesInView(table.ViewQuery, descriptorRewrites)
			if err != nil {
				return err
			}
			viewQuery, err = rewriteTypesInView(viewQuery, descriptorRewrites)
			if err != nil {
				return err
			}
			table.ViewQuery = viewQuery
		}

		// Rewrite outbound FKs in both `OutboundFKs` and `Mutations` slice.
		origFKs := table.OutboundFKs
		table.OutboundFKs = nil
		for i := range origFKs {
			fk := &origFKs[i]
			to := fk.ReferencedTableID
			if indexRewrite, ok := descriptorRewrites[to]; ok {
				fk.ReferencedTableID = indexRewrite.ID
				fk.OriginTableID = tableRewrite.ID
			} else {
				// If indexRewrite doesn't exist, the user has specified
				// restoreOptSkipMissingFKs. Error checking in the case the user hasn't has
				// already been done in allocateDescriptorRewrites.
				continue
			}

			// TODO(dt): if there is an existing (i.e. non-restoring) table with
			// a db and name matching the one the FK pointed to at backup, should
			// we update the FK to point to it?
			table.OutboundFKs = append(table.OutboundFKs, *fk)
		}
		for idx := range table.Mutations {
			if c := table.Mutations[idx].GetConstraint(); c != nil &&
				c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY {
				fk := &c.ForeignKey
				if rewriteOfReferencedTable, ok := descriptorRewrites[fk.ReferencedTableID]; ok {
					fk.ReferencedTableID = rewriteOfReferencedTable.ID
					fk.OriginTableID = tableRewrite.ID
				}
			}
		}

		origInboundFks := table.InboundFKs
		table.InboundFKs = nil
		for i := range origInboundFks {
			ref := &origInboundFks[i]
			if refRewrite, ok := descriptorRewrites[ref.OriginTableID]; ok {
				ref.ReferencedTableID = tableRewrite.ID
				ref.OriginTableID = refRewrite.ID
				table.InboundFKs = append(table.InboundFKs, *ref)
			}
		}

		for i, dest := range table.DependsOn {
			if depRewrite, ok := descriptorRewrites[dest]; ok {
				table.DependsOn[i] = depRewrite.ID
			} else {
				// Views with missing dependencies should have been filtered out
				// or have caused an error in maybeFilterMissingViews().
				return errors.AssertionFailedf(
					"cannot restore %q because referenced table %d was not found",
					table.Name, dest)
			}
		}
		for i, dest := range table.DependsOnTypes {
			if depRewrite, ok := descriptorRewrites[dest]; ok {
				table.DependsOnTypes[i] = depRewrite.ID
			} else {
				// Views with missing dependencies should have been filtered out
				// or have caused an error in maybeFilterMissingViews().
				return errors.AssertionFailedf(
					"cannot restore %q because referenced type %d was not found",
					table.Name, dest)
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if refRewrite, ok := descriptorRewrites[ref.ID]; ok {
				ref.ID = refRewrite.ID
				table.DependedOnBy = append(table.DependedOnBy, ref)
			}
		}

		// Rewrite unique_without_index in both `UniqueWithoutIndexConstraints`
		// and `Mutations` slice.
		origUniqueWithoutIndexConstraints := table.UniqueWithoutIndexConstraints
		table.UniqueWithoutIndexConstraints = nil
		for _, unique := range origUniqueWithoutIndexConstraints {
			if rewrite, ok := descriptorRewrites[unique.TableID]; ok {
				unique.TableID = rewrite.ID
				table.UniqueWithoutIndexConstraints = append(table.UniqueWithoutIndexConstraints, unique)
			} else {
				// A table's UniqueWithoutIndexConstraint.TableID references itself, and
				// we should always find a rewrite for the table being restored.
				return errors.AssertionFailedf("cannot restore %q because referenced table ID in "+
					"UniqueWithoutIndexConstraint %d was not found", table.Name, unique.TableID)
			}
		}
		for idx := range table.Mutations {
			if c := table.Mutations[idx].GetConstraint(); c != nil &&
				c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX {
				uwi := &c.UniqueWithoutIndexConstraint
				if rewrite, ok := descriptorRewrites[uwi.TableID]; ok {
					uwi.TableID = rewrite.ID
				} else {
					return errors.AssertionFailedf("cannot restore %q because referenced table ID in "+
						"UniqueWithoutIndexConstraint %d was not found", table.Name, uwi.TableID)
				}
			}
		}

		if table.IsSequence() && table.SequenceOpts.HasOwner() {
			if ownerRewrite, ok := descriptorRewrites[table.SequenceOpts.SequenceOwner.OwnerTableID]; ok {
				table.SequenceOpts.SequenceOwner.OwnerTableID = ownerRewrite.ID
			} else {
				// The sequence's owner table is not being restored, thus we simply
				// remove the ownership dependency. To get here, the user must have
				// specified 'skip_missing_sequence_owners', otherwise we would have
				// errored out in allocateDescriptorRewrites.
				table.SequenceOpts.SequenceOwner = descpb.TableDescriptor_SequenceOpts_SequenceOwner{}
			}
		}

		// rewriteCol is a closure that performs the ID rewrite logic on a column.
		rewriteCol := func(col *descpb.ColumnDescriptor) error {
			// Rewrite the types.T's IDs present in the column.
			RewriteIDsInTypesT(col.Type, descriptorRewrites)
			var newUsedSeqRefs []descpb.ID
			for _, seqID := range col.UsesSequenceIds {
				if rewrite, ok := descriptorRewrites[seqID]; ok {
					newUsedSeqRefs = append(newUsedSeqRefs, rewrite.ID)
				} else {
					// The referenced sequence isn't being restored.
					// Strip the DEFAULT expression and sequence references.
					// To get here, the user must have specified 'skip_missing_sequences' --
					// otherwise, would have errored out in allocateDescriptorRewrites.
					newUsedSeqRefs = []descpb.ID{}
					col.DefaultExpr = nil
					break
				}
			}
			col.UsesSequenceIds = newUsedSeqRefs

			var newOwnedSeqRefs []descpb.ID
			for _, seqID := range col.OwnsSequenceIds {
				// We only add the sequence ownership dependency if the owned sequence
				// is being restored.
				// If the owned sequence is not being restored, the user must have
				// specified 'skip_missing_sequence_owners' to get here, otherwise
				// we would have errored out in allocateDescriptorRewrites.
				if rewrite, ok := descriptorRewrites[seqID]; ok {
					newOwnedSeqRefs = append(newOwnedSeqRefs, rewrite.ID)
				}
			}
			col.OwnsSequenceIds = newOwnedSeqRefs

			for i, fnID := range col.UsesFunctionIds {
				// We have dropped expressions missing UDF references. so it's safe to
				// just rewrite ids.
				col.UsesFunctionIds[i] = descriptorRewrites[fnID].ID
			}

			return nil
		}

		// Rewrite sequence and type references in column descriptors.
		for idx := range table.Columns {
			if err := rewriteCol(&table.Columns[idx]); err != nil {
				return err
			}
		}
		for idx := range table.Mutations {
			if col := table.Mutations[idx].GetColumn(); col != nil {
				if err := rewriteCol(col); err != nil {
					return err
				}
			}
		}

		for idx := range table.Triggers {
			trigger := &table.Triggers[idx]

			// Rewrite trigger function reference.
			if triggerFnRewrite, ok := descriptorRewrites[trigger.FuncID]; ok {
				trigger.FuncID = triggerFnRewrite.ID
			} else {
				return errors.AssertionFailedf(
					"cannot restore trigger %s on table %q because referenced function %d was not found",
					trigger.Name, table.Name, trigger.FuncID,
				)
			}

			// Rewrite forward-references.
			rewriteIDs := func(ids []descpb.ID, refName string) (newIDs []descpb.ID, err error) {
				newIDs = make([]descpb.ID, len(ids))
				for i, id := range ids {
					if depRewrite, ok := descriptorRewrites[id]; ok {
						newIDs[i] = depRewrite.ID
					} else {
						return nil, errors.AssertionFailedf(
							"cannot restore trigger %s on table %q because referenced %s %d was not found",
							trigger.Name, table.Name, refName, id,
						)
					}
				}
				return newIDs, nil
			}
			newDependsOn, err := rewriteIDs(trigger.DependsOn, "relation")
			if err != nil {
				return err
			}
			newDependsOnTypes, err := rewriteIDs(trigger.DependsOnTypes, "type")
			if err != nil {
				return err
			}
			newDependsOnRoutines, err := rewriteIDs(trigger.DependsOnRoutines, "routine")
			if err != nil {
				return err
			}
			trigger.DependsOn = newDependsOn
			trigger.DependsOnTypes = newDependsOnTypes
			trigger.DependsOnRoutines = newDependsOnRoutines
		}
	}

	return nil
}

func makeDBNameReplaceFunc(newDB string) func(ctx *tree.FmtCtx, tn *tree.TableName) {
	return func(ctx *tree.FmtCtx, tn *tree.TableName) {
		// empty catalog e.g. ``"".information_schema.tables` should stay empty.
		if tn.CatalogName != "" {
			tn.CatalogName = tree.Name(newDB)
		}
		ctx.WithReformatTableNames(nil, func() {
			ctx.FormatNode(tn)
		})
	}
}

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
func rewriteViewQueryDBNames(table *tabledesc.Mutable, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.Syntax,
			"failed to parse underlying query from view %q", table.Name)
	}
	// Re-format to change all DB names to `newDB`.
	f := tree.NewFmtCtx(
		tree.FmtParsable,
		tree.FmtReformatTableNames(makeDBNameReplaceFunc(newDB)),
	)
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
}

func rewriteFunctionBodyDBNames(
	fnBody string, newDB string, lang catpb.Function_Language,
) (string, error) {
	replaceFunc := makeDBNameReplaceFunc(newDB)
	switch lang {
	case catpb.Function_SQL:
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		stmts, err := parser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		for i, stmt := range stmts {
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			f := tree.NewFmtCtx(
				tree.FmtParsable,
				tree.FmtReformatTableNames(replaceFunc),
			)
			f.FormatNode(stmt.AST)
			fmtCtx.WriteString(f.CloseAndGetString())
			fmtCtx.WriteString(";")
		}
		return fmtCtx.CloseAndGetString(), nil

	case catpb.Function_PLPGSQL:
		stmt, err := plpgsqlparser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		fmtCtx := tree.NewFmtCtx(
			tree.FmtParsable,
			tree.FmtReformatTableNames(replaceFunc),
		)
		fmtCtx.FormatNode(stmt.AST)
		return fmtCtx.CloseAndGetString(), nil

	default:
		return "", errors.AssertionFailedf("unexpected function language %s", lang)
	}
}

// rewriteTypesInExpr rewrites all explicit ID type references in the input
// expression string according to rewrites.
func rewriteTypesInExpr(expr string, rewrites jobspb.DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}
	ctx := makeTypeReplaceFmtCtx(rewrites)
	ctx.FormatNode(parsed)
	return ctx.CloseAndGetString(), nil
}

// rewriteTypesInView rewrites all explicit ID type references in the input view
// query string according to rewrites.
func rewriteTypesInView(viewQuery string, rewrites jobspb.DescRewriteMap) (string, error) {
	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}
	ctx := makeTypeReplaceFmtCtx(rewrites)
	ctx.FormatNode(stmt.AST)
	return ctx.CloseAndGetString(), nil
}

func rewriteTypesInRoutine(
	fnBody string, rewrites jobspb.DescRewriteMap, lang catpb.Function_Language,
) (string, error) {
	switch lang {
	case catpb.Function_SQL:
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		stmts, err := parser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		for i, stmt := range stmts {
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			typeReplaceCtx := makeTypeReplaceFmtCtx(rewrites)
			typeReplaceCtx.FormatNode(stmt.AST)
			fmtCtx.WriteString(typeReplaceCtx.CloseAndGetString())
			fmtCtx.WriteString(";")
		}
		return fmtCtx.CloseAndGetString(), nil

	case catpb.Function_PLPGSQL:
		stmt, err := plpgsqlparser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		typeReplaceCtx := makeTypeReplaceFmtCtx(rewrites)
		typeReplaceCtx.FormatNode(stmt.AST)
		return typeReplaceCtx.CloseAndGetString(), nil

	default:
		return "", errors.AssertionFailedf("unexpected function language: %v", lang)
	}
}

// makeTypeReplaceFmtCtx returns a FmtCtx which rewrites explicit ID references
// according to the rewrites map.
func makeTypeReplaceFmtCtx(rewrites jobspb.DescRewriteMap) *tree.FmtCtx {
	return tree.NewFmtCtx(
		tree.FmtSerializable,
		tree.FmtIndexedTypeFormat(func(ctx *tree.FmtCtx, ref *tree.OIDTypeReference) {
			newRef := ref
			id := typedesc.UserDefinedTypeOIDToID(ref.OID)
			if rw, ok := rewrites[id]; ok {
				newRef = &tree.OIDTypeReference{OID: catid.TypeIDToOID(rw.ID)}
			}
			ctx.WriteString(newRef.SQLString())
		}),
	)
}

// rewriteSequencesInExpr rewrites all sequence IDs in the input expression
// string according to rewrites.
func rewriteSequencesInExpr(expr string, rewrites jobspb.DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	newExpr, err := tree.SimpleVisit(parsed, makeSequenceReplaceFunc(rewrites))
	if err != nil {
		return "", err
	}
	return newExpr.String(), nil
}

func rewriteFunctionsInExpr(expr string, rewrites jobspb.DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	replaceFunc := func(ex tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		funcExpr, ok := ex.(*tree.FuncExpr)
		if !ok {
			return true, ex, nil
		}
		oidRef, ok := funcExpr.Func.FunctionReference.(*tree.FunctionOID)
		if !ok {
			return true, ex, nil
		}
		if !funcdesc.IsOIDUserDefinedFunc(oidRef.OID) {
			return true, ex, nil
		}
		fnID := funcdesc.UserDefinedFunctionOIDToID(oidRef.OID)
		rewriteID := catid.FuncIDToOID(rewrites[fnID].ID)
		newFuncExpr := *funcExpr
		newFuncExpr.Func = tree.ResolvableFunctionReference{
			FunctionReference: &tree.FunctionOID{OID: rewriteID},
		}
		return true, &newFuncExpr, nil
	}

	newExpr, err := tree.SimpleVisit(parsed, replaceFunc)
	if err != nil {
		return "", err
	}
	return newExpr.String(), nil
}

func makeSequenceReplaceFunc(
	rewrites jobspb.DescRewriteMap,
) func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
	return func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		id, ok := schemaexpr.GetSeqIDFromExpr(expr)
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr, ok := expr.(*tree.AnnotateTypeExpr)
		if !ok {
			return true, expr, nil
		}
		rewrite, ok := rewrites[descpb.ID(id)]
		if !ok {
			return true, expr, nil
		}
		annotateTypeExpr.Expr = tree.NewNumVal(
			constant.MakeInt64(int64(rewrite.ID)),
			strconv.Itoa(int(rewrite.ID)),
			false, /* negative */
		)
		return false, annotateTypeExpr, nil
	}
}

// rewriteSequencesInView walks the given viewQuery and
// rewrites all sequence IDs in it according to rewrites.
func rewriteSequencesInView(viewQuery string, rewrites jobspb.DescRewriteMap) (string, error) {
	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, makeSequenceReplaceFunc(rewrites))
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

func rewriteSequencesInFunction(
	fnBody string, rewrites jobspb.DescRewriteMap, lang catpb.Function_Language,
) (string, error) {
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	replaceSeqFunc := makeSequenceReplaceFunc(rewrites)
	switch lang {
	case catpb.Function_SQL:
		stmts, err := parser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceSeqFunc)
			if err != nil {
				return "", err
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			fmtCtx.WriteString(";")
		}

	case catpb.Function_PLPGSQL:
		stmt, err := plpgsqlparser.Parse(fnBody)
		if err != nil {
			return "", err
		}
		v := plpgsqltree.SQLStmtVisitor{Fn: replaceSeqFunc}
		newStmt := plpgsqltree.Walk(&v, stmt.AST)
		fmtCtx.FormatNode(newStmt)

	default:
		return "", errors.AssertionFailedf("unexpected function language %s", lang)
	}
	return fmtCtx.CloseAndGetString(), nil
}

// RewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func RewriteIDsInTypesT(typ *types.T, descriptorRewrites jobspb.DescRewriteMap) {
	if !typ.UserDefined() {
		return
	}
	tid := typedesc.GetUserDefinedTypeDescID(typ)
	// Collect potential new OID values.
	var newOID, newArrayOID oid.Oid
	if rw, ok := descriptorRewrites[tid]; ok {
		newOID = catid.TypeIDToOID(rw.ID)
	}
	if typ.Family() != types.ArrayFamily {
		tid = typedesc.GetUserDefinedArrayTypeDescID(typ)
		if rw, ok := descriptorRewrites[tid]; ok {
			newArrayOID = catid.TypeIDToOID(rw.ID)
		}
	}
	types.RemapUserDefinedTypeOIDs(typ, newOID, newArrayOID)
	// If the type is an array, then we need to rewrite the element type as well.
	if typ.Family() == types.ArrayFamily {
		RewriteIDsInTypesT(typ.ArrayContents(), descriptorRewrites)
	}
}

// rewriteRoutineBody rewrites a set of SQL or PL/pgSQL statements.
func rewriteRoutineBody(
	descriptorRewrites jobspb.DescRewriteMap,
	fnBody, overrideDB string,
	fnLang catpb.Function_Language,
) (string, error) {
	if overrideDB != "" {
		dbNameReplaced, err := rewriteFunctionBodyDBNames(fnBody, overrideDB, fnLang)
		if err != nil {
			return "", err
		}
		fnBody = dbNameReplaced
	}
	fnBody, err := rewriteSequencesInFunction(fnBody, descriptorRewrites, fnLang)
	if err != nil {
		return "", err
	}
	fnBody, err = rewriteTypesInRoutine(fnBody, descriptorRewrites, fnLang)
	if err != nil {
		return "", err
	}
	return fnBody, nil
}

// MaybeClearSchemaChangerStateInDescs goes over all mutable descriptors and
// cleans any state information from descriptors which have no targets associated
// with the corresponding jobs. The state is used to lock a descriptor to ensure
// no concurrent schema change jobs can occur, which needs to be cleared if no
// jobs exist working on *any* targets, since otherwise the descriptor would
// be left locked.
func MaybeClearSchemaChangerStateInDescs(descriptors []catalog.MutableDescriptor) error {
	nonEmptyJobs := make(map[jobspb.JobID]struct{})
	// Track all the schema changer states that have a non-empty job associated
	// with them.
	for _, desc := range descriptors {
		if state := desc.GetDeclarativeSchemaChangerState(); state != nil &&
			len(state.Targets) > 0 {
			nonEmptyJobs[state.JobID] = struct{}{}
		}
	}
	// Clean up any schema changer states that have empty jobs that don't have any
	// targets associated.
	for _, desc := range descriptors {
		if state := desc.GetDeclarativeSchemaChangerState(); state != nil &&
			len(state.Targets) == 0 {
			if _, found := nonEmptyJobs[state.JobID]; !found {
				desc.SetDeclarativeSchemaChangerState(nil)
			}
		}
	}
	return nil
}

// TypeDescs rewrites all ID's in the input slice of TypeDescriptors
// using the input ID rewrite mapping.
func TypeDescs(types []*typedesc.Mutable, descriptorRewrites jobspb.DescRewriteMap) error {
	for _, typ := range types {
		rewrite, ok := descriptorRewrites[typ.ID]
		if !ok {
			return errors.Errorf("missing rewrite for type %d", typ.ID)
		}
		// Reset the version and modification time on this new descriptor.
		typ.Version = 1
		typ.ModificationTime = hlc.Timestamp{}

		if err := rewriteSchemaChangerState(typ, descriptorRewrites); err != nil {
			return err
		}

		typ.ID = rewrite.ID
		typ.ParentSchemaID = rewrite.ParentSchemaID
		typ.ParentID = rewrite.ParentID
		for i := range typ.ReferencingDescriptorIDs {
			id := typ.ReferencingDescriptorIDs[i]
			if rw, ok := descriptorRewrites[id]; ok {
				typ.ReferencingDescriptorIDs[i] = rw.ID
			}
		}
		switch t := typ.Kind; t {
		case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_COMPOSITE, descpb.TypeDescriptor_MULTIREGION_ENUM:
			if rw, ok := descriptorRewrites[typ.ArrayTypeID]; ok {
				typ.ArrayTypeID = rw.ID
			}
		case descpb.TypeDescriptor_ALIAS:
			// We need to rewrite any ID's present in the aliased types.T.
			RewriteIDsInTypesT(typ.Alias, descriptorRewrites)
		default:
			return errors.AssertionFailedf("unknown type kind %s", t.String())
		}
	}
	return nil
}

// SchemaDescs rewrites all ID's in the input slice of SchemaDescriptors
// using the input ID rewrite mapping.
func SchemaDescs(schemas []*schemadesc.Mutable, descriptorRewrites jobspb.DescRewriteMap) error {
	for _, sc := range schemas {
		rewrite, ok := descriptorRewrites[sc.ID]
		if !ok {
			return errors.Errorf("missing rewrite for schema %d", sc.ID)
		}
		// Reset the version and modification time on this new descriptor.
		sc.Version = 1
		sc.ModificationTime = hlc.Timestamp{}

		sc.ID = rewrite.ID
		sc.ParentID = rewrite.ParentID

		// Rewrite function ID and types ID in function signatures.
		newFns := make(map[string]descpb.SchemaDescriptor_Function)
		for fnName, fn := range sc.GetFunctions() {
			newSigs := make([]descpb.SchemaDescriptor_FunctionSignature, 0, len(fn.Signatures))
			for i := range fn.Signatures {
				sig := &fn.Signatures[i]
				// If the function is not found in the backup, we just skip. This only
				// happens when restoring from a backup with `BACKUP TABLE` where the
				// function descriptors are not backup.
				fnDesc, ok := descriptorRewrites[sig.ID]
				if !ok {
					continue
				}
				sig.ID = fnDesc.ID
				for _, typ := range sig.ArgTypes {
					RewriteIDsInTypesT(typ, descriptorRewrites)
				}
				RewriteIDsInTypesT(sig.ReturnType, descriptorRewrites)
				for _, typ := range sig.OutParamTypes {
					RewriteIDsInTypesT(typ, descriptorRewrites)
				}
				newSigs = append(newSigs, *sig)
			}
			if len(newSigs) > 0 {
				newFns[fnName] = descpb.SchemaDescriptor_Function{
					Name:       fnName,
					Signatures: newSigs,
				}
			}
		}
		sc.Functions = newFns

		if err := rewriteSchemaChangerState(sc, descriptorRewrites); err != nil {
			return err
		}
	}
	return nil
}

// rewriteSchemaChangerState handles rewriting any references to IDs stored in
// the descriptor's declarative schema changer state.
func rewriteSchemaChangerState(
	d catalog.MutableDescriptor, descriptorRewrites jobspb.DescRewriteMap,
) (err error) {
	state := d.GetDeclarativeSchemaChangerState()
	if state == nil {
		return nil
	}
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "rewriting declarative schema changer state")
		}
	}()

	var droppedConstraints catalog.ConstraintIDSet
	for i := 0; i < len(state.Targets); i++ {
		t := &state.Targets[i]
		// Since the parent database ID is never written in the descriptorRewrites
		// map we need to special case certain elements that need their ParentID
		// re-written
		if data := t.GetTableData(); data != nil {
			rewrite, ok := descriptorRewrites[data.TableID]
			if !ok {
				return errors.Errorf("missing rewrite for id %d in %s", data.TableID, screl.ElementString(t.Element()))
			}
			data.TableID = rewrite.ID
			data.DatabaseID = rewrite.ParentID
			continue
		} else if data := t.GetNamespace(); data != nil {
			rewrite, ok := descriptorRewrites[data.DescriptorID]
			if !ok {
				return errors.Errorf("missing rewrite for id %d in %s", data.DescriptorID, screl.ElementString(t.Element()))
			}
			data.DescriptorID = rewrite.ID
			data.DatabaseID = rewrite.ParentID
			data.SchemaID = rewrite.ParentSchemaID
			continue
		}

		// removeElementAtCurrentIdx deletes the element at the current index.
		removeElementAtCurrentIdx := func() {
			state.Targets = append(state.Targets[:i], state.Targets[i+1:]...)
			state.CurrentStatuses = append(state.CurrentStatuses[:i], state.CurrentStatuses[i+1:]...)
			state.TargetRanks = append(state.TargetRanks[:i], state.TargetRanks[i+1:]...)
			i--
		}

		missingID := descpb.InvalidID
		if err := screl.WalkDescIDs(t.Element(), func(id *descpb.ID) error {
			if *id == descpb.InvalidID {
				// Some descriptor ID fields in elements may be deliberately unset.
				// Skip these as they are not subject to rewrite.
				return nil
			}
			rewrite, ok := descriptorRewrites[*id]
			if !ok {
				missingID = *id
				return errors.Errorf("missing rewrite for id %d in %s", *id, screl.ElementString(t.Element()))
			}
			*id = rewrite.ID
			return nil
		}); err != nil {
			switch el := t.Element().(type) {
			case *scpb.SchemaParent:
				// We'll permit this in the special case of a schema parent element.
				_, scExists := descriptorRewrites[el.SchemaID]
				if !scExists && state.CurrentStatuses[i] == scpb.Status_ABSENT {
					removeElementAtCurrentIdx()
					continue
				}
			case *scpb.CheckConstraint:
				// IF there is any dependency missing for check constraint, we just drop
				// the target.
				removeElementAtCurrentIdx()
				droppedConstraints.Add(el.ConstraintID)
				continue
			case *scpb.ColumnDefaultExpression:
				// IF there is any dependency missing for column default expression, we
				// just drop the target.
				removeElementAtCurrentIdx()
				continue
			case *scpb.SequenceOwner:
				// If a sequence owner is missing the sequence, then the sequence
				// was already dropped and this element can be safely removed.
				if el.SequenceID == missingID {
					removeElementAtCurrentIdx()
					continue
				}
			}
			return errors.Wrap(err, "rewriting descriptor ids")
		}

		if err := screl.WalkExpressions(t.Element(), func(expr *catpb.Expression) error {
			if *expr == "" {
				return nil
			}
			newExpr, err := rewriteTypesInExpr(string(*expr), descriptorRewrites)
			if err != nil {
				return errors.Wrapf(err, "rewriting expression type references: %q", *expr)
			}
			newExpr, err = rewriteSequencesInExpr(newExpr, descriptorRewrites)
			if err != nil {
				return errors.Wrapf(err, "rewriting expression sequence references: %q", newExpr)
			}
			newExpr, err = rewriteFunctionsInExpr(newExpr, descriptorRewrites)
			if err != nil {
				return errors.Wrapf(err, "rewriting expression function references: %q", newExpr)
			}
			*expr = catpb.Expression(newExpr)
			return nil
		}); err != nil {
			return err
		}
		if err := screl.WalkTypes(t.Element(), func(t *types.T) error {
			RewriteIDsInTypesT(t, descriptorRewrites)
			return nil
		}); err != nil {
			return errors.Wrap(err, "rewriting user-defined type references")
		}
		// TODO(ajwerner): Remember to rewrite views when the time comes. Currently
		// views are not handled by the declarative schema changer.
	}

	// Drop all children targets of dropped CHECK constraint.
	for i := 0; i < len(state.Targets); i++ {
		t := &state.Targets[i]
		if err := screl.WalkConstraintIDs(t.Element(), func(id *catid.ConstraintID) error {
			if !droppedConstraints.Contains(*id) {
				return nil
			}
			state.Targets = append(state.Targets[:i], state.Targets[i+1:]...)
			state.CurrentStatuses = append(state.CurrentStatuses[:i], state.CurrentStatuses[i+1:]...)
			state.TargetRanks = append(state.TargetRanks[:i], state.TargetRanks[i+1:]...)
			i--
			return nil
		}); err != nil {
			return err
		}
	}
	d.SetDeclarativeSchemaChangerState(state)
	return nil
}

func dropCheckConstraintMissingDeps(
	table *tabledesc.Mutable, descriptorRewrites jobspb.DescRewriteMap,
) error {
	var newChecks []*descpb.TableDescriptor_CheckConstraint
	for i := range table.Checks {
		fnIDs, err := table.GetAllReferencedFunctionIDsInConstraint(table.Checks[i].ConstraintID)
		if err != nil {
			return err
		}
		allFnFound := true
		for _, fnID := range fnIDs.Ordered() {
			if _, ok := descriptorRewrites[fnID]; !ok {
				allFnFound = false
				break
			}
		}
		if allFnFound {
			newChecks = append(newChecks, table.Checks[i])
		}
	}
	table.Checks = newChecks
	var newMutations []descpb.DescriptorMutation
	for i := range table.Mutations {
		keepMutation := true
		if c := table.Mutations[i].GetConstraint(); c != nil && c.ConstraintType == descpb.ConstraintToUpdate_CHECK {
			fnIDs, err := table.GetAllReferencedFunctionIDsInConstraint(c.Check.ConstraintID)
			if err != nil {
				return err
			}
			for _, fnID := range fnIDs.Ordered() {
				if _, ok := descriptorRewrites[fnID]; !ok {
					keepMutation = false
					break
				}
			}
		}
		if keepMutation {
			newMutations = append(newMutations, table.Mutations[i])
		}
	}
	table.Mutations = newMutations
	return nil
}

func dropColumnExpressionsMissingDeps(
	table *tabledesc.Mutable, descriptorRewrites jobspb.DescRewriteMap,
) error {
	maybeDropExpressions := func(col *descpb.ColumnDescriptor) error {
		allFnFound := true
		fnIDs, err := table.GetAllReferencedFunctionIDsInColumnExprs(col.ID)
		if err != nil {
			return err
		}
		for _, fnID := range fnIDs.Ordered() {
			if _, ok := descriptorRewrites[fnID]; !ok {
				allFnFound = false
				break
			}
		}
		if !allFnFound {
			// TODO(chengxiong): right now, we only allow UDFs in DEFAULT expression,
			// so it's ok to just clear default expression and referenced function
			// ids. Need to refactor to support ON UPDATE and computed column
			// expression once supported.
			col.DefaultExpr = nil
			col.UsesFunctionIds = nil
		}
		return nil
	}

	for i := range table.Columns {
		col := &table.Columns[i]
		if err := maybeDropExpressions(col); err != nil {
			return err
		}
	}
	for i := range table.Mutations {
		if col := table.Mutations[i].GetColumn(); col != nil {
			if err := maybeDropExpressions(col); err != nil {
				return err
			}
		}
	}
	return nil
}

func dropTriggerMissingDeps(table *tabledesc.Mutable, descriptorRewrites jobspb.DescRewriteMap) {
	foundAllDeps := func(ids []descpb.ID) bool {
		for _, id := range ids {
			if _, ok := descriptorRewrites[id]; !ok {
				return false
			}
		}
		return true
	}
	newTriggers := make([]descpb.TriggerDescriptor, 0, len(table.Triggers))
	for i := range table.Triggers {
		trigger := &table.Triggers[i]
		if !foundAllDeps(trigger.DependsOn) || !foundAllDeps(trigger.DependsOnTypes) ||
			!foundAllDeps(trigger.DependsOnRoutines) {
			continue
		}
		newTriggers = append(newTriggers, *trigger)
	}
	table.Triggers = newTriggers
}

// DatabaseDescs rewrites all ID's in the input slice of DatabaseDescriptors
// using the input ID rewrite mapping. The function elides remapping offline schemas,
// since they will not get restored into the cluster.
func DatabaseDescs(
	databases []*dbdesc.Mutable,
	descriptorRewrites jobspb.DescRewriteMap,
	offlineSchemas map[descpb.ID]struct{},
) error {
	for _, db := range databases {
		rewrite, ok := descriptorRewrites[db.ID]
		if !ok {
			return errors.Errorf("missing rewrite for database %d", db.ID)
		}
		db.ID = rewrite.ID

		if rewrite.NewDBName != "" {
			db.Name = rewrite.NewDBName
		}

		db.Version = 1
		db.ModificationTime = hlc.Timestamp{}

		if err := rewriteSchemaChangerState(db, descriptorRewrites); err != nil {
			return err
		}

		// Rewrite the name-to-ID mapping for the database's child schemas.
		newSchemas := make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
		err := db.ForEachSchema(func(id descpb.ID, name string) error {
			rewrite, ok := descriptorRewrites[id]
			if !ok {
				return errors.Errorf("missing rewrite for schema %d", id)
			}
			if _, ok := offlineSchemas[id]; ok {
				// offline schema should not get added to the database descriptor.
				return nil
			}
			newSchemas[name] = descpb.DatabaseDescriptor_SchemaInfo{ID: rewrite.ID}
			return nil
		})
		if err != nil {
			return err
		}
		db.Schemas = newSchemas
	}
	return nil
}

// FunctionDescs rewrites all ID's in the input slice of function descriptors
// using the input ID rewrite mapping.
func FunctionDescs(
	functions []*funcdesc.Mutable, descriptorRewrites jobspb.DescRewriteMap, overrideDB string,
) error {
	for _, fnDesc := range functions {
		fnRewrite, ok := descriptorRewrites[fnDesc.ID]
		if !ok {
			return errors.Errorf("missing function rewrite for function %d", fnDesc.ID)
		}
		// Reset the version and modification time on this new descriptor.
		fnDesc.Version = 1
		fnDesc.ModificationTime = hlc.Timestamp{}

		fnDesc.ID = fnRewrite.ID
		fnDesc.ParentSchemaID = fnRewrite.ParentSchemaID
		fnDesc.ParentID = fnRewrite.ParentID

		// Rewrite function body.
		var err error
		fnDesc.FunctionBody, err = rewriteRoutineBody(
			descriptorRewrites, fnDesc.FunctionBody, overrideDB, fnDesc.Lang,
		)
		if err != nil {
			return err
		}

		// Rewrite type IDs.
		for _, param := range fnDesc.Params {
			RewriteIDsInTypesT(param.Type, descriptorRewrites)
		}
		RewriteIDsInTypesT(fnDesc.ReturnType.Type, descriptorRewrites)

		// Rewrite Dependency IDs.
		for i, depID := range fnDesc.DependsOn {
			if depRewrite, ok := descriptorRewrites[depID]; ok {
				fnDesc.DependsOn[i] = depRewrite.ID
			} else {
				return errors.AssertionFailedf(
					"cannot restore function %q because referenced table %d was not found",
					fnDesc.Name, depID)
			}
		}

		for i, typID := range fnDesc.DependsOnTypes {
			if typRewrite, ok := descriptorRewrites[typID]; ok {
				fnDesc.DependsOnTypes[i] = typRewrite.ID
			} else {
				return errors.AssertionFailedf(
					"cannot restore function %q because referenced type %d was not found",
					fnDesc.Name, typID)
			}
		}

		for i, funcID := range fnDesc.DependsOnFunctions {
			if funcRewrite, ok := descriptorRewrites[funcID]; ok {
				fnDesc.DependsOnFunctions[i] = funcRewrite.ID
			} else {
				return errors.AssertionFailedf(
					"cannot restore function %q because referenced function %d was not found",
					fnDesc.Name, funcID)
			}
		}

		// Rewrite back reference IDs.
		for i, dep := range fnDesc.DependedOnBy {
			if depRewrite, ok := descriptorRewrites[dep.ID]; ok {
				fnDesc.DependedOnBy[i].ID = depRewrite.ID
			} else {
				return errors.AssertionFailedf(
					"cannot restore function %q because back referenced relation %d was not found",
					fnDesc.Name, dep.ID)
			}
		}
	}
	return nil
}
