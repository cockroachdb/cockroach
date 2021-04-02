// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package bulkccl

import (
	"context"
	"go/constant"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DescRewriteMap maps old descriptor IDs to new descriptor and parent IDs.
type DescRewriteMap map[descpb.ID]*jobspb.RestoreDetails_DescriptorRewrite

// RewriteTableDescs mutates tables to match the ID and privilege specified
// in descriptorRewrites, as well as adjusting cross-table references to use the
// new IDs. overrideDB can be specified to set database names in views.
func RewriteTableDescs(
	tables []*tabledesc.Mutable, descriptorRewrites DescRewriteMap, overrideDB string,
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

		table.ID = tableRewrite.ID
		table.UnexposedParentSchemaID = maybeRewriteSchemaID(table.GetParentSchemaID(),
			descriptorRewrites, table.IsTemporary())
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

		if err := catalog.ForEachNonDropIndex(table, func(indexI catalog.Index) error {
			index := indexI.IndexDesc()
			// Verify that for any interleaved index being restored, the interleave
			// parent is also being restored. Otherwise, the interleave entries in the
			// restored IndexDescriptors won't have anything to point to.
			// TODO(dan): It seems like this restriction could be lifted by restoring
			// stub TableDescriptors for the missing interleave parents.
			for j, a := range index.Interleave.Ancestors {
				ancestorRewrite, ok := descriptorRewrites[a.TableID]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave parent %d", table.Name, a.TableID,
					)
				}
				index.Interleave.Ancestors[j].TableID = ancestorRewrite.ID
			}
			for j, c := range index.InterleavedBy {
				childRewrite, ok := descriptorRewrites[c.Table]
				if !ok {
					return errors.Errorf(
						"cannot restore table %q without interleave child table %d", table.Name, c.Table,
					)
				}
				index.InterleavedBy[j].Table = childRewrite.ID
			}
			return nil
		}); err != nil {
			return err
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
			rewriteIDsInTypesT(col.Type, descriptorRewrites)
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

		// since this is a "new" table in eyes of new cluster, any leftover change
		// lease is obviously bogus (plus the nodeID is relative to backup cluster).
		table.Lease = nil
	}
	return nil
}

// RewriteDatabaseDescs rewrites all ID's in the input slice of
// DatabaseDescriptors using the input ID rewrite mapping.
func RewriteDatabaseDescs(databases []*dbdesc.Mutable, descriptorRewrites DescRewriteMap) error {
	for _, db := range databases {
		rewrite, ok := descriptorRewrites[db.ID]
		if !ok {
			return errors.Errorf("missing rewrite for database %d", db.ID)
		}
		db.ID = rewrite.ID

		db.Version = 1
		db.ModificationTime = hlc.Timestamp{}

		// Rewrite the name-to-ID mapping for the database's child schemas.
		newSchemas := make(map[string]descpb.DatabaseDescriptor_SchemaInfo)
		for schemaName, schemaInfo := range db.Schemas {
			if schemaInfo.Dropped {
				continue
			}
			rewrite, ok := descriptorRewrites[schemaInfo.ID]
			if !ok {
				return errors.Errorf("missing rewrite for schema %d", db.ID)
			}
			newSchemas[schemaName] = descpb.DatabaseDescriptor_SchemaInfo{ID: rewrite.ID}
		}
		db.Schemas = newSchemas
	}
	return nil
}

// rewriteIDsInTypesT rewrites all ID's in the input types.T using the input
// ID rewrite mapping.
func rewriteIDsInTypesT(typ *types.T, descriptorRewrites DescRewriteMap) {
	if !typ.UserDefined() {
		return
	}
	// Collect potential new OID values.
	var newOID, newArrayOID oid.Oid
	if rw, ok := descriptorRewrites[typedesc.GetTypeDescID(typ)]; ok {
		newOID = typedesc.TypeIDToOID(rw.ID)
	}
	if typ.Family() != types.ArrayFamily {
		if rw, ok := descriptorRewrites[typedesc.GetArrayTypeDescID(typ)]; ok {
			newArrayOID = typedesc.TypeIDToOID(rw.ID)
		}
	}
	types.RemapUserDefinedTypeOIDs(typ, newOID, newArrayOID)
	// If the type is an array, then we need to rewrite the element type as well.
	if typ.Family() == types.ArrayFamily {
		rewriteIDsInTypesT(typ.ArrayContents(), descriptorRewrites)
	}
}

// RewriteTypeDescs rewrites all ID's in the input slice of TypeDescriptors
// using the input ID rewrite mapping.
func RewriteTypeDescs(types []*typedesc.Mutable, descriptorRewrites DescRewriteMap) error {
	for _, typ := range types {
		rewrite, ok := descriptorRewrites[typ.ID]
		if !ok {
			return errors.Errorf("missing rewrite for type %d", typ.ID)
		}
		// Reset the version and modification time on this new descriptor.
		typ.Version = 1
		typ.ModificationTime = hlc.Timestamp{}

		typ.ID = rewrite.ID
		typ.ParentSchemaID = maybeRewriteSchemaID(typ.ParentSchemaID, descriptorRewrites,
			false /* isTemporaryDesc */)
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
			rewriteIDsInTypesT(typ.Alias, descriptorRewrites)
		default:
			return errors.AssertionFailedf("unknown type kind %s", t.String())
		}
	}
	return nil
}

// RewriteSchemaDescs rewrites all ID's in the input slice of SchemaDescriptors
// using the input ID rewrite mapping.
func RewriteSchemaDescs(schemas []*schemadesc.Mutable, descriptorRewrites DescRewriteMap) error {
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
	}
	return nil
}

func maybeRewriteSchemaID(
	curSchemaID descpb.ID, descriptorRewrites DescRewriteMap, isTemporaryDesc bool,
) descpb.ID {
	// If the current schema is the public schema, then don't attempt to
	// do any rewriting.
	if curSchemaID == keys.PublicSchemaID && !isTemporaryDesc {
		return curSchemaID
	}
	rw, ok := descriptorRewrites[curSchemaID]
	if !ok {
		return curSchemaID
	}
	return rw.ID
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
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.SetReformatTableNames(func(ctx *tree.FmtCtx, tn *tree.TableName) {
		// empty catalog e.g. ``"".information_schema.tables` should stay empty.
		if tn.CatalogName != "" {
			tn.CatalogName = tree.Name(newDB)
		}
		ctx.WithReformatTableNames(nil, func() {
			ctx.FormatNode(tn)
		})
	})
	f.FormatNode(stmt.AST)
	table.ViewQuery = f.CloseAndGetString()
	return nil
}

// rewriteTypesInExpr rewrites all explicit ID type references in the input
// expression string according to rewrites.
func rewriteTypesInExpr(expr string, rewrites DescRewriteMap) (string, error) {
	parsed, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}
	ctx := tree.NewFmtCtx(tree.FmtSerializable)
	ctx.SetIndexedTypeFormat(func(ctx *tree.FmtCtx, ref *tree.OIDTypeReference) {
		newRef := ref
		if rw, ok := rewrites[typedesc.UserDefinedTypeOIDToID(ref.OID)]; ok {
			newRef = &tree.OIDTypeReference{OID: typedesc.TypeIDToOID(rw.ID)}
		}
		ctx.WriteString(newRef.SQLString())
	})
	ctx.FormatNode(parsed)
	return ctx.CloseAndGetString(), nil
}

// rewriteSequencesInExpr rewrites all sequence IDs in the input expression
// string according to rewrites.
func rewriteSequencesInExpr(expr string, rewrites DescRewriteMap) (string, error) {
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
func rewriteSequencesInView(viewQuery string, rewrites DescRewriteMap) (string, error) {
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

// WriteDescriptors writes all the new descriptors: First the ID ->
// TableDescriptor for the new table, then flip (or initialize) the name -> ID
// entry so any new queries will use the new one. The tables are assigned the
// permissions of their parent database and the user must have CREATE permission
// on that database at the time this function is called.
func WriteDescriptors(
	ctx context.Context,
	codec keys.SQLCodec,
	txn *kv.Txn,
	user security.SQLUsername,
	descsCol *descs.Collection,
	databases []catalog.DatabaseDescriptor,
	schemas []catalog.SchemaDescriptor,
	tables []catalog.TableDescriptor,
	types []catalog.TypeDescriptor,
	descCoverage tree.DescriptorCoverage,
	settings *cluster.Settings,
	extra []roachpb.KeyValue,
) error {
	ctx, span := tracing.ChildSpan(ctx, "WriteDescriptors")
	defer span.Finish()
	err := func() error {
		b := txn.NewBatch()
		wroteDBs := make(map[descpb.ID]catalog.DatabaseDescriptor)
		for i := range databases {
			desc := databases[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, desc, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := desc.(*dbdesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for database %d, %T, expected Mutable",
						desc.GetID(), desc)
				}
			}
			wroteDBs[desc.GetID()] = desc
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, desc.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			dKey := catalogkv.MakeDatabaseNameKey(ctx, settings, desc.GetName())
			b.CPut(dKey.Key(codec), desc.GetID(), nil)
		}

		// Write namespace and descriptor entries for each schema.
		for i := range schemas {
			sc := schemas[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, sc, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := sc.(*schemadesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for schema %d, %T, expected Mutable",
						sc.GetID(), sc)
				}
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, sc.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			skey := catalogkeys.NewSchemaKey(sc.GetParentID(), sc.GetName())
			b.CPut(skey.Key(codec), sc.GetID(), nil)
		}

		for i := range tables {
			table := tables[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, table, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := table.(*tabledesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for table %d, %T, expected Mutable",
						table.GetID(), table)
				}
			}
			// If the table descriptor is being written to a multi-region database and
			// the table does not have a locality config setup, set one up here. The
			// table's locality config will be set to the default locality - REGIONAL
			// BY TABLE IN PRIMARY REGION.
			_, dbDesc, err := descsCol.GetImmutableDatabaseByID(
				ctx, txn, table.GetParentID(), tree.DatabaseLookupFlags{
					Required:       true,
					AvoidCached:    true,
					IncludeOffline: true,
				})
			if err != nil {
				return err
			}
			if dbDesc.IsMultiRegion() {
				if table.GetLocalityConfig() == nil {
					table.(*tabledesc.Mutable).SetTableLocalityRegionalByTable(tree.PrimaryRegionNotSpecifiedName)
				}
			} else {
				// If the database is not multi-region enabled, ensure that we don't
				// write any multi-region table descriptors into it.
				if table.GetLocalityConfig() != nil {
					return pgerror.Newf(pgcode.FeatureNotSupported,
						"cannot write descriptor for multi-region table %s into non-multi-region database %s",
						table.GetName(),
						dbDesc.GetName(),
					)
				}
			}

			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, tables[i].(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			// Depending on which cluster version we are restoring to, we decide which
			// namespace table to write the descriptor into. This may cause wrong
			// behavior if the cluster version is bumped DURING a restore.
			tkey := catalogkv.MakeObjectNameKey(
				ctx,
				settings,
				table.GetParentID(),
				table.GetParentSchemaID(),
				table.GetName(),
			)
			b.CPut(tkey.Key(codec), table.GetID(), nil)
		}

		// Write all type descriptors -- create namespace entries and write to
		// the system.descriptor table.
		for i := range types {
			typ := types[i]
			updatedPrivileges, err := getRestoringPrivileges(ctx, codec, txn, typ, user, wroteDBs, descCoverage)
			if err != nil {
				return err
			}
			if updatedPrivileges != nil {
				if mut, ok := typ.(*typedesc.Mutable); ok {
					mut.Privileges = updatedPrivileges
				} else {
					log.Fatalf(ctx, "wrong type for type %d, %T, expected Mutable",
						typ.GetID(), typ)
				}
			}
			if err := descsCol.WriteDescToBatch(
				ctx, false /* kvTrace */, typ.(catalog.MutableDescriptor), b,
			); err != nil {
				return err
			}
			tkey := catalogkv.MakeObjectNameKey(ctx, settings, typ.GetParentID(), typ.GetParentSchemaID(), typ.GetName())
			b.CPut(tkey.Key(codec), typ.GetID(), nil)
		}

		for _, kv := range extra {
			b.InitPut(kv.Key, &kv.Value, false)
		}
		if err := txn.Run(ctx, b); err != nil {
			if errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
				return pgerror.Newf(pgcode.DuplicateObject, "table already exists")
			}
			return err
		}
		return nil
	}()
	return errors.Wrapf(err, "restoring table desc and namespace entries")
}

func getRestoringPrivileges(
	ctx context.Context,
	codec keys.SQLCodec,
	txn *kv.Txn,
	desc catalog.Descriptor,
	user security.SQLUsername,
	wroteDBs map[descpb.ID]catalog.DatabaseDescriptor,
	descCoverage tree.DescriptorCoverage,
) (updatedPrivileges *descpb.PrivilegeDescriptor, err error) {
	switch desc := desc.(type) {
	case catalog.TableDescriptor, catalog.SchemaDescriptor:
		if wrote, ok := wroteDBs[desc.GetParentID()]; ok {
			// If we're creating a new database in this restore, the privileges of the
			// table and schema should be that of the parent DB.
			//
			// Leave the privileges of the temp system tables as the default too.
			// XXX(pbardea): I deleted `|| wrote.GetName() == restoreTempSystemDB`. Is this ok?j
			if descCoverage == tree.RequestedDescriptors {
				updatedPrivileges = wrote.GetPrivileges()
			}
		} else if descCoverage == tree.RequestedDescriptors {
			parentDB, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, codec, desc.GetParentID())
			if err != nil {
				return nil, errors.Wrapf(err, "failed to lookup parent DB %d", errors.Safe(desc.GetParentID()))
			}

			// Default is to copy privs from restoring parent db, like CREATE {TABLE,
			// SCHEMA}. But also like CREATE {TABLE,SCHEMA}, we set the owner to the
			// user creating the table (the one running the restore).
			// TODO(dt): Make this more configurable.
			updatedPrivileges = sql.CreateInheritedPrivilegesFromDBDesc(parentDB, user)
		}
	case catalog.TypeDescriptor, catalog.DatabaseDescriptor:
		if descCoverage == tree.RequestedDescriptors {
			// If the restore is not a cluster restore we cannot know that the users on
			// the restoring cluster match the ones that were on the cluster that was
			// backed up. So we wipe the privileges on the type/database.
			updatedPrivileges = descpb.NewDefaultPrivilegeDescriptor(user)
		}
	}
	return updatedPrivileges, nil
}
