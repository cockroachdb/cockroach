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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
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

		// TODO(lucy): deal with outbound foreign key mutations here as well.
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

// rewriteViewQueryDBNames rewrites the passed table's ViewQuery replacing all
// non-empty db qualifiers with `newDB`.
//
// TODO: this AST traversal misses tables named in strings (#24556).
func rewriteViewQueryDBNames(table *tabledesc.Mutable, newDB string) error {
	stmt, err := parser.ParseOne(table.ViewQuery)
	if err != nil {
		return pgerror.Wrapf(err, pgcode.Syntax,
			"failed to parse underlying query from view %q", table.Name)
	}
	// Re-format to change all DB names to `newDB`.
	f := tree.NewFmtCtx(
		tree.FmtParsable,
		tree.FmtReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
			// empty catalog e.g. ``"".information_schema.tables` should stay empty.
			if tn.CatalogName != "" {
				tn.CatalogName = tree.Name(newDB)
			}
			ctx.WithReformatTableNames(nil, func() {
				ctx.FormatNode(tn)
			})
		}),
	)
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
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
			var id descpb.ID
			id, err = typedesc.UserDefinedTypeOIDToID(ref.OID)
			if err != nil {
				return
			}
			if rw, ok := rewrites[id]; ok {
				newRef = &tree.OIDTypeReference{OID: typedesc.TypeIDToOID(rw.ID)}
			}
			ctx.WriteString(newRef.SQLString())
		}),
	)
	if err != nil {
		return "", err
	}
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
	rewriteFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
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

	newExpr, err := tree.SimpleVisit(parsed, rewriteFunc)
	if err != nil {
		return "", err
	}
	return newExpr.String(), nil
}

// rewriteSequencesInView walks the given viewQuery and
// rewrites all sequence IDs in it according to rewrites.
func rewriteSequencesInView(viewQuery string, rewrites jobspb.DescRewriteMap) (string, error) {
	rewriteFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
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

	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, rewriteFunc)
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

// rewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func rewriteIDsInTypesT(typ *types.T, descriptorRewrites jobspb.DescRewriteMap) error {
	if !typ.UserDefined() {
		return nil
	}
	tid, err := typedesc.GetUserDefinedTypeDescID(typ)
	if err != nil {
		return err
	}
	// Collect potential new OID values.
	var newOID, newArrayOID oid.Oid
	if rw, ok := descriptorRewrites[tid]; ok {
		newOID = typedesc.TypeIDToOID(rw.ID)
	}
	if typ.Family() != types.ArrayFamily {
		tid, err = typedesc.GetUserDefinedArrayTypeDescID(typ)
		if err != nil {
			return err
		}
		if rw, ok := descriptorRewrites[tid]; ok {
			newArrayOID = typedesc.TypeIDToOID(rw.ID)
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
			rewrite, ok := descriptorRewrites[*id]
			if !ok {
				return errors.Errorf("missing rewrite for id %d in %T", *id, t)
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
	if len(state.Targets) == 0 {
		d.SetDeclarativeSchemaChangerState(nil)
	}
	return nil
}

// DatabaseDescs rewrites all ID's in the input slice of
// DatabaseDescriptors using the input ID rewrite mapping.
func DatabaseDescs(databases []*dbdesc.Mutable, descriptorRewrites jobspb.DescRewriteMap) error {
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
		err := db.ForEachNonDroppedSchema(func(id descpb.ID, name string) error {
			rewrite, ok := descriptorRewrites[id]
			if !ok {
				return errors.Errorf("missing rewrite for schema %d", db.ID)
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
