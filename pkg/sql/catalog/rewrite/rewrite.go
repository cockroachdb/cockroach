// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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

		// Remap type IDs and sequence IDs in all serialized expressions within the TableDescriptor.
		// TODO (rohany): This needs tests once partial indexes are ready.
		if err := tabledesc.ForEachExprStringInTableDesc(table, func(expr *string) error {
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
			return nil
		}); err != nil {
			return err
		}

		// Walk view query and remap sequence IDs.
		if table.IsView() {
			viewQuery, err := rewriteSequencesInView(table.ViewQuery, descriptorRewrites)
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
			if err := rewriteIDsInTypesT(col.Type, descriptorRewrites); err != nil {
				return err
			}
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

func rewriteFunctionBodyDBNames(fnBody string, newDB string) (string, error) {
	stmts, err := parser.Parse(fnBody)
	if err != nil {
		return "", err
	}
	replaceFunc := makeDBNameReplaceFunc(newDB)

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
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
}

// rewriteTypesInExpr rewrites all explicit ID type references in the input
// expression string according to rewrites.
func rewriteTypesInExpr(expr string, rewrites jobspb.DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	ctx := tree.NewFmtCtx(
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
	ctx.FormatNode(parsed)
	return ctx.CloseAndGetString(), nil
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

func rewriteSequencesInFunction(fnBody string, rewrites jobspb.DescRewriteMap) (string, error) {
	stmts, err := parser.Parse(fnBody)
	if err != nil {
		return "", err
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i, stmt := range stmts {
		newStmt, err := tree.SimpleStmtVisit(stmt.AST, makeSequenceReplaceFunc(rewrites))
		if err != nil {
			return "", err
		}
		if i > 0 {
			fmtCtx.WriteString("\n")
		}
		fmtCtx.FormatNode(newStmt)
		fmtCtx.WriteString(";")
	}
	return fmtCtx.CloseAndGetString(), nil
}

// rewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func rewriteIDsInTypesT(typ *types.T, descriptorRewrites jobspb.DescRewriteMap) error {
	if !typ.UserDefined() {
		return nil
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
		if err := rewriteIDsInTypesT(typ.ArrayContents(), descriptorRewrites); err != nil {
			return err
		}
	}

	return nil
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
		case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
			if rw, ok := descriptorRewrites[typ.ArrayTypeID]; ok {
				typ.ArrayTypeID = rw.ID
			}
		case descpb.TypeDescriptor_ALIAS:
			// We need to rewrite any ID's present in the aliased types.T.
			if err := rewriteIDsInTypesT(typ.Alias, descriptorRewrites); err != nil {
				return err
			}
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
		for _, fn := range sc.GetFunctions() {
			for i := range fn.Signatures {
				sig := &fn.Signatures[i]
				sig.ID = descriptorRewrites[sig.ID].ID
				for _, typ := range sig.ArgTypes {
					if err := rewriteIDsInTypesT(typ, descriptorRewrites); err != nil {
						return err
					}
				}
				if err := rewriteIDsInTypesT(sig.ReturnType, descriptorRewrites); err != nil {
					return err
				}
			}
		}

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
	for i := 0; i < len(state.Targets); i++ {
		t := &state.Targets[i]
		if err := screl.WalkDescIDs(t.Element(), func(id *descpb.ID) error {
			if *id == descpb.InvalidID {
				// Some descriptor ID fields in elements may be deliberately unset.
				// Skip these as they are not subject to rewrite.
				return nil
			}
			rewrite, ok := descriptorRewrites[*id]
			if !ok {
				return errors.Errorf("missing rewrite for id %d in %s", *id, screl.ElementString(t.Element()))
			}
			*id = rewrite.ID
			return nil
		}); err != nil {
			// We'll permit this in the special case of a schema parent element.
			switch el := t.Element().(type) {
			case *scpb.SchemaParent:
				_, scExists := descriptorRewrites[el.SchemaID]
				if !scExists && state.CurrentStatuses[i] == scpb.Status_ABSENT {
					state.Targets = append(state.Targets[:i], state.Targets[i+1:]...)
					state.CurrentStatuses = append(state.CurrentStatuses[:i], state.CurrentStatuses[i+1:]...)
					state.TargetRanks = append(state.TargetRanks[:i], state.TargetRanks[i+1:]...)
					i--
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
			*expr = catpb.Expression(newExpr)
			return nil
		}); err != nil {
			return err
		}
		if err := screl.WalkTypes(t.Element(), func(t *types.T) error {
			return rewriteIDsInTypesT(t, descriptorRewrites)
		}); err != nil {
			return errors.Wrap(err, "rewriting user-defined type references")
		}
		// TODO(ajwerner): Remember to rewrite views when the time comes. Currently
		// views are not handled by the declarative schema changer.
	}
	d.SetDeclarativeSchemaChangerState(state)
	return nil
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
		fnBody := fnDesc.FunctionBody
		if overrideDB != "" {
			dbNameReplaced, err := rewriteFunctionBodyDBNames(fnDesc.FunctionBody, overrideDB)
			if err != nil {
				return err
			}
			fnBody = dbNameReplaced
		}
		fnBody, err := rewriteSequencesInFunction(fnBody, descriptorRewrites)
		if err != nil {
			return err
		}
		fnDesc.FunctionBody = fnBody

		// Rewrite type IDs.
		for _, param := range fnDesc.Params {
			if err := rewriteIDsInTypesT(param.Type, descriptorRewrites); err != nil {
				return err
			}
		}
		if err := rewriteIDsInTypesT(fnDesc.ReturnType.Type, descriptorRewrites); err != nil {
			return err
		}

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
	}
	return nil
}
